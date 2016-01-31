import requests
import pandas as pd
from threading import Thread, Event
import re
import datetime as dt
from bs4 import BeautifulSoup
from copy import copy
import logging
from collections import namedtuple
from time import sleep
import sys

Message = namedtuple(
    'Result', ['chat_id', 'update_id', 'text', 'timestamp']
)

log = logging.getLogger('mensabot')
log.setLevel(logging.INFO)
formatter = logging.Formatter(
    fmt='%(asctime)s - %(levelname)s - %(name)s | %(message)s',
    datefmt='%H:%M:%S',
)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
log.addHandler(stream_handler)

ingredients_re = re.compile('[(].*[)] *')
WEEKDAYS = [
    'montag',
    'dienstag',
    'mittwoch',
    'donnerstag',
    'freitag',
]

DELTA_T = dt.timedelta(minutes=5)
URL = 'http://www.stwdo.de/gastronomie/speiseplaene/' \
      'hauptmensa/wochenansicht-hauptmensa'


def replace_all(regex, string, repl):
    old = ''
    while old != string:
        old = string
        string = regex.sub(repl, string)

    return string


def get_date(soup, weekday):
    a = soup.find('a', {'href': '#' + weekday})
    date = dt.datetime.strptime(a.text.split()[1], '%d.%m.%Y')
    return dt.date(date.year, date.month, date.day)


def extract_daily_menu(soup, weekday):
    table_div = soup.find('div', {'id': weekday})

    table = table_div.find('table')
    table = parse_counter(table)
    menu, = pd.read_html(
        str(table),
    )
    menu.columns = ['gericht', 'beschreibung', 'counter']
    menu['gericht'] = menu.gericht.apply(
        lambda s: replace_all(ingredients_re, s, '')
    )
    menu.dropna(inplace=True)
    return menu


def parse_counter(table):
    table = copy(table)
    for a in table.select('a[data-tooltip]'):
        counter = BeautifulSoup(a.attrs['data-tooltip'], 'html.parser')
        a.replace_with(counter)

    return table


def fetch_weekly_menu():

    ret = requests.get(URL)
    soup = BeautifulSoup(
        ret.content.decode('utf-8'),
        'html.parser',
    )

    menu = {
        get_date(soup, weekday): extract_daily_menu(soup, weekday)
        for weekday in WEEKDAYS
    }

    return menu


class MensaBot(Thread):
    url = 'https://api.telegram.org/bot{token}'

    def __init__(self, bot_token):
        self.url = self.url.format(token=bot_token)
        self.stop_event = Event()
        self.menu = {}
        super().__init__()

    def run(self):
        while not self.stop_event.is_set():
            try:
                messages = self.getUpdates()
                now = dt.datetime.now()
                date = dt.date.today()
                if now.hour > 15:
                    date += dt.timedelta(days=1)

                for message in messages:
                    if dt.datetime.now() - message.timestamp < DELTA_T:
                        if message.text.startswith('/menu'):
                            menu = self.format_menu(date)
                            self.send_message(
                                message.chat_id,
                                menu,
                            )
                            log.info('Send menu for {:%Y-%m-%d} to {}'.format(
                                date, message.chat_id
                            ))
                    self.confirm_message(message)
                self.stop_event.wait(1)
            except requests.exceptions.RequestException:
                self.stop_event.wait(30)

    def format_menu(self, date):
        # saturday and sunday
        if date.weekday() >= 5:
            return 'Am Wochenende bleibt die Mensaküche kalt'

        if date not in self.menu:
            try:
                self.menu = fetch_weekly_menu()
            except:
                return 'Kein Menü gefunden'

        if date not in self.menu:
            return 'Nix gefunden für den {:%d.%m.%Y}'.format(date)

        text = ''
        menu = self.menu[date].query('counter != "Grillstation"')
        for row in menu.itertuples():
            text += '*{}*: {} \n'.format(row.counter, row.gericht)

        return text

    def terminate(self):
        self.stop_event.set()

    def getUpdates(self):
        ret = requests.get(self.url + '/getUpdates', timeout=5).json()

        if ret['ok']:
            messages = []
            for update in ret['result']:
                message_data = update['message']
                chatdata = message_data['chat']

                message = Message(
                    update_id=update['update_id'],
                    chat_id=chatdata['id'],
                    text=message_data.get('text', ''),
                    timestamp=dt.datetime.fromtimestamp(message_data['date'])
                )
                messages.append(message)
            return messages

    def confirm_message(self, message):
        requests.get(
            self.url + '/getUpdates',
            params={'offset': message.update_id + 1},
            timeout=5,
        )

    def send_message(self, chat_id, message):
        try:
            r = requests.post(
                self.url + '/sendMessage',
                data={
                    'chat_id': chat_id,
                    'text': message,
                    'parse_mode': 'Markdown'
                },
                timeout=5,
            )
        except requests.exceptions.Timeout:
            log.exception('Telegram "send_message" timed out')
        return r


if __name__ == '__main__':
    bot = MensaBot(sys.argv[1])
    bot.start()
    log.info('bot running')
    try:
        while True:
            sleep(10)
    except (KeyboardInterrupt, SystemExit):
        bot.terminate()
