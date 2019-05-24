import requests
import os
import logging
from argparse import ArgumentParser
import re
from bs4 import BeautifulSoup
from collections import namedtuple
from peewee import SqliteDatabase, IntegerField, Model
import telepot
from telepot.exception import BotWasBlockedError, BotWasKickedError, TelegramError
from time import sleep
from functools import lru_cache
from datetime import datetime, timedelta
import pytz
import schedule
import dateutil.parser
import retrying
from emoji import emojize

log = logging.getLogger('mensabot')

ingredients_re = re.compile(r'[(](\d+[a-z]*,?\s*)+[)]')
price_re = re.compile(r'(\d+),(\d+) €')

URL = 'https://www.stwdo.de/mensa-co/tu-dortmund/hauptmensa/'
TZ = pytz.timezone('Europe/Berlin')

parser = ArgumentParser()
parser.add_argument('bot_token')
parser.add_argument('--database', default='mensabot_clients.sqlite')


MenuItem = namedtuple(
    'MenuItem',
    ['category', 'description', 'supplies', 'emoticons', 'p_student', 'p_staff', 'p_guest']
)


supplies_emoticons = {
    'Mit Schweinefleisch': emojize(':pig_face:'),
    'Mit Rindfleisch': emojize(':cow_face:'),
    'Mit Geflügel': emojize(':chicken:'),
    'Mit Fisch bzw. Meeresfrüchten': emojize(':dolphin:'),
    'Ohne Fleisch': emojize(':carrot:'),
    'Vegane Speise': emojize(':deciduous_tree:')
}


db = SqliteDatabase(None)


class Client(Model):
    chat_id = IntegerField(unique=True)

    class Meta:
        database = db


class MenuNotFound(Exception):
    pass


def find_item(soup, cls):
    return soup.find('div', {'class': re.compile('item {}.*'.format(cls))})


def download_menu_page(day):
    log.info('Downloading menu for day {}'.format(day))
    ret = requests.get(URL, params={'tx_pamensa_mensa[date]': str(day)})
    ret.raise_for_status()
    log.info('Done')
    return BeautifulSoup(ret.text, 'lxml')


def extract_menu_items(soup):

    menu_div = soup.find('div', {'class': 'meals-wrapper'})

    if menu_div is None:
        raise MenuNotFound

    menu_items = menu_div.find_all('div', {'class': 'meal-item'})

    return list(map(parse_menu_item, menu_items))


def parse_price(price_div):
    m = price_re.search(price_div.text)
    euros, cents = map(int, m.groups())
    return euros + cents / 100


def parse_menu_item(menu_item):

    category = find_item(menu_item, 'category').find('img')['title']
    description = find_item(menu_item, 'description').text.lstrip()
    description = ingredients_re.sub('', description)
    description = re.sub(r'(\w),(\w)', r'\1, \2', description)

    supplies = list(map(
        lambda img: img['title'],
        find_item(menu_item, 'supplies').find_all('img')
    ))

    emoticons = ''.join(list(map(
        lambda title: supplies_emoticons[title],
        filter(
            lambda title: title in supplies_emoticons,
            supplies
    ))))

    p_student = parse_price(find_item(menu_item, 'price student'))
    p_staff = parse_price(find_item(menu_item, 'price staff'))
    p_guest = parse_price(find_item(menu_item, 'price guest'))

    return MenuItem(category, description, supplies, emoticons, p_student, p_staff, p_guest)


@lru_cache(maxsize=10)
@retrying.retry(
    stop_max_delay=30000,
    wait_fixed=2000,
)
def get_menu(day):
    soup = download_menu_page(day)
    items = extract_menu_items(soup)
    return items


def format_menu(menu, full=False, date=None):

    if full is False:
        menu = filter(lambda i: i.category not in ('Grillstation', 'Beilagen'), menu)

    title = '*Hauptmensa*'
    if date is not None:
        title += ' ({:%d.%m.%Y})'.format(date)

    return title + '\n\n' + '\n\n'.join(
        '*{item.category}* - {item.emoticons}\n{item.description}'.format(item=item)
        for item in menu
    )


def build_menu_reply(text):
    full = text.startswith('/fullmenu')

    try:
        datestring = text.split()[1]
        dt = dateutil.parser.parse(datestring)
    except (ValueError, IndexError):
        dt = datetime.now(TZ)
        if dt.hour >= 15:
            dt += timedelta(days=1)

    day = dt.date()

    if day.weekday() >= 5:
        return 'Am Wochenende bleibt die Mensaküche kalt'

    try:
        menu = get_menu(day)
    except Exception:
        log.exception('Error getting menu')
        return 'Fehler beim herunterladen von Tag {}'.format(day)

    try:
        return format_menu(menu, full=full, date=dt)
    except Exception:
        log.exception('Error formatting menu')
        return 'Fehler beim formatieren von Tag {}'.format(day)


class MensaBot(telepot.Bot):

    def handle(self, msg):
        content_type, chat_type, chat_id = telepot.glance(msg)

        if chat_type == 'private':
            start = 'Du bekommst '
        else:
            start = 'Ihr bekommt '

        if content_type != 'text':
            return

        text = msg['text']

        if text.startswith('/start'):
            client, new = Client.get_or_create(chat_id=chat_id)

            if new:
                reply = start + 'ab jetzt jeden Tag um 11 das Menü'
            else:
                reply = start + 'das Menü schon!'

        elif text.startswith('/stop'):
            try:
                client = Client.get(chat_id=chat_id)
                client.delete_instance()
                reply = start + 'das Menü ab jetzt nicht mehr'
            except Client.DoesNotExist:
                reply = start + 'das Menü doch gar nicht'

        elif text.startswith('/menu') or text.startswith('/fullmenu'):
            reply = build_menu_reply(text)
        else:
            reply = 'Das habe ich nicht verstanden'

        log.info('Sending message to {}'.format(chat_id))
        self.sendMessage(chat_id, reply, parse_mode='markdown')

    def send_menu_to_clients(self):
        day = datetime.now(TZ).date()
        
        if day.weekday() >= 5:
            return

        try:
            menu = get_menu(day)
            # if parsed menu is empty, we can return and not send empty messages
            if len(menu) == 0:
                log.error('Empty menu {}'.format(menu))
                return
            text = format_menu(menu, date=day)
        except Exception:
            log.exception('Error getting menu')
            text = 'Kein Menü gefunden für {}'.format(day)

        for client in Client.select():
            log.info('Sending menu to {}'.format(client.chat_id))
            try:
                self.sendMessage(client.chat_id, text, parse_mode='markdown')
            except (BotWasBlockedError, BotWasKickedError):
                log.warning('Removing client {}'.format(client.chat_id))
                client.delete_instance()
            except TelegramError as e:
                if e.error_code == 403:
                    log.warning('Removing client {}'.format(client.chat_id))
                    client.delete_instance()


def main():
    args = parser.parse_args()

    log.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        fmt='%(asctime)s|%(levelname)s|%(name)s|%(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    log.addHandler(stream_handler)

    file_handler = logging.FileHandler('mensabot.log')
    file_handler.setFormatter(formatter)
    log.addHandler(file_handler)

    db.init(args.database)
    Client.create_table(safe=True)

    log.info("Using database {}".format(os.path.abspath(args.database)))
    log.info("Database contains {} active clients".format(Client.select().count()))

    bot = MensaBot(args.bot_token)
    bot.message_loop()
    log.info('Bot runnning')

    schedule.every().day.at('11:00').do(bot.send_menu_to_clients)

    while True:
        try:
            schedule.run_pending()
        except:
            log.exception('Exception during schedule execution')
        sleep(1)


if __name__ == '__main__':

    try:
        main()
    except (KeyboardInterrupt, SystemExit):
        log.info('Aborted')
