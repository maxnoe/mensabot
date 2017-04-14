import requests
import logging
from argparse import ArgumentParser
import re
from bs4 import BeautifulSoup
from collections import namedtuple
from peewee import SqliteDatabase, IntegerField, Model
import telepot
from time import sleep
from functools import lru_cache
from datetime import datetime, timedelta
import pytz
import schedule
import dateutil.parser

log = logging.getLogger('mensabot')
log.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    fmt='%(asctime)s - %(levelname)s - %(name)s | %(message)s',
    datefmt='%H:%M:%S',
)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
log.addHandler(stream_handler)

ingredients_re = re.compile(' [(](\d+[a-z]*,?)+[)]')
price_re = re.compile('(\d+),(\d+) €')

URL = 'http://www.stwdo.de/mensa-co/tu-dortmund/hauptmensa/'
TZ = pytz.timezone('Europe/Berlin')

parserinfo = dateutil.parser.parserinfo(dayfirst=True)


parser = ArgumentParser()
parser.add_argument('bot_token')


MenuItem = namedtuple(
    'MenuItem',
    ['category', 'description', 'supplies', 'p_student', 'p_staff', 'p_guest']
)


db = SqliteDatabase('mensabot_clients.sqlite')


class Client(Model):
    chat_id = IntegerField(unique=True)

    class Meta:
        database = db


class MenuNotFound(Exception):
    pass


def find_item(soup, cls):
    return soup.find('div', {'class': 'item {}'.format(cls)})


def download_menu_page(day):
    log.info('Downloading menu')

    ret = requests.post(URL, data={'tx_pamensa_mensa[date]': str(day)})
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
    description = find_item(menu_item, 'description').text
    description = ingredients_re.sub('', description)
    description = re.sub('(\w),(\w)', r'\1, \2', description)

    supplies = list(map(
        lambda img: img['title'],
        find_item(menu_item, 'supplies').find_all('img')
    ))

    p_student = parse_price(find_item(menu_item, 'price student'))
    p_staff = parse_price(find_item(menu_item, 'price staff'))
    p_guest = parse_price(find_item(menu_item, 'price guest'))

    return MenuItem(category, description, supplies, p_student, p_staff, p_guest)


@lru_cache(maxsize=10)
def get_menu(day):
    soup = download_menu_page(day)
    items = extract_menu_items(soup)
    return items


def format_menu(menu, full=False):

    if full is False:
        menu = filter(lambda i: i.category not in ('Grillstation', 'Beilagen'), menu)

    return '\n'.join(
        '*{item.category}:* {item.description}'.format(item=item)
        for item in menu
    )


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

        elif text.startswith('/menu'):

            try:
                dt = dateutil.parser.parse(text.lstrip('/menu '), parserinfo)
            except ValueError:
                dt = datetime.now(TZ)
                if dt.hour >= 15:
                    dt += timedelta(days=1)

            day = dt.date()

            if day.weekday() >= 5:
                reply = 'Am Wochenende bleibt die Mensaküche kalt'
            else:
                try:
                    menu = get_menu(day)
                    reply = format_menu(menu)
                except Exception as e:
                    log.exception('Error getting menu')
                    reply = 'Kein Menü gefunden für {}'.format(day)
        else:
            reply = 'Das habe ich nicht verstanden'

        log.info('Sending message to {}'.format(chat_id))
        self.sendMessage(chat_id, reply, parse_mode='markdown')

    def send_menu_to_clients(self):
        day = datetime.now(TZ).date()

        if day.weekday() >= 5:
            return

        try:
            menu = get_menu(str(day))
            text = format_menu(menu)
        except Exception as e:
            log.exception('Error getting menu')
            text = 'Kein Menü gefunden für {}'.format(day)

        for client in Client.select():
            log.info('Sending menu to {}'.format(client.chat_id))
            self.sendMessage(client.chat_id, text)


def main():
    args = parser.parse_args()

    db.create_table(Client, safe=True)

    bot = MensaBot(args.bot_token)
    bot.message_loop()
    log.info('Bot runnning')

    schedule.every().day.at("11:00").do(bot.send_menu_to_clients)

    while True:
        schedule.run_pending()
        sleep(1)


if __name__ == '__main__':

    try:
        main()
    except (KeyboardInterrupt, SystemExit):
        log.info('Aborted')
