import base64
import datetime
import locale
import logging
import urllib
import pandas as pd
from email.mime.text import MIMEText
from itertools import product
from bs4 import BeautifulSoup
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from tqdm import tqdm

from mail_to_events import get_credentials
from param import *

logging.basicConfig(filename='app.log', filemode='a', level=logging.DEBUG)
logging.getLogger('googleapiclient.discovery').setLevel(logging.WARNING)
logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.WARNING)
pd.set_option('mode.chained_assignment', None)
locale.setlocale(locale.LC_ALL, 'fr_FR.UTF-8')

year_now = datetime.datetime.now().year
sites_dico = {'3': 'Bastille', '2': "Pereire", "1": "Vendome"}
classes_history = "all_classes.csv"


def log_now():
    return datetime.datetime.now().strftime('%Y %b %d %H:%M:%S')


def get_weeks_of_classes(max_weeks):
    df_classes = pd.DataFrame()

    for (week, site_id) in tqdm(product(range(max_weeks), sites_dico.keys())):
        r2_url = 'https://r2training.zingfit.com/reserve/index.cfm?action=Reserve.chooseClass&site=' \
                 + site_id + '&wk=' + str(int(week))

        content = urllib.request.urlopen(r2_url).read()
        soup = BeautifulSoup(content, 'html.parser')

        days = [day.string.title() for day in soup.find_all('span', {'class': 'thead-dow'})]
        dates = [date.string for date in soup.find_all('span', {'class': 'thead-date'})]

        for i in range(7):
            td_this_day = 'day' + str(int(i))
            classes = soup.find("td", {"class": td_this_day}).findAll("div", {"class", "scheduleBlock"})
            for c in classes:
                instructor = c.find("span", {"class": "scheduleInstruc"}).string
                class_type = c.find("span", {"class": "scheduleClass"}).string.replace('\n', '').replace('\t', '')
                # class_length = c.find("span", {"class": "classlength"}).string
                c.find('span', class_='classlength').decompose()
                time = c.find("span", {"class": "scheduleTime"}).string.replace(' ', '').replace('min', ' min')

                if class_type != 'Open Gym':
                    new_row = {'Dow': days[i],
                               'Date': dates[i],
                               'Time': time,
                               'Site': sites_dico[site_id],
                               'Class': class_type,
                               'Instructor': instructor,
                               'Datetime': str(year_now) + '.' + dates[i] + ' ' + time}
                    df_classes = df_classes.append(new_row, ignore_index=True)

    df_classes['Datetime'] = pd.to_datetime(df_classes['Datetime'], format='%Y.%d.%m %H:%M')
    return df_classes


def filter_classes_with_values(df, choices):
    for key, values in choices.items():
        if key in df.columns:
            df = df[df[key].isin(values)]
    return df


def get_classes_id(df):
    string_columns = df.dtypes.index[df.dtypes == 'object'].tolist()
    return df[sorted(string_columns)].apply(lambda row: row.str.cat(), axis=1)


def get_classes_local():
    return pd.read_csv(classes_history, parse_dates=[7], index_col=0, dtype=str)


def get_new_classes():
    known_classes = get_classes_local()
    available_classes = get_weeks_of_classes(3)

    known_ids = get_classes_id(known_classes)
    available_ids = get_classes_id(available_classes)
    new_ids = set(available_ids) - set(known_ids)
    new_classes = available_classes.iloc[[index for index, class_id in enumerate(available_ids) if class_id in new_ids]]
    if new_classes.shape[0]:
        pd.concat([known_classes, new_classes]).to_csv(classes_history)
    return new_classes


def class_df_to_html(df):
    html_classes = df[df['Dow'] != '']
    html_classes['Datetime'] = html_classes['Datetime'].dt.strftime('%a %d %h %H:%M')
    html_classes = html_classes.drop(columns=['Dow', 'Date', 'Time'], axis=1, errors='ignore').to_html(index=False)
    return "<!DOCTYPE html><html><head><style>" + open("planning_css.css", "r").read() + \
           "</style></head><body>" + html_classes + "</body></html>"


def send_email(df):
    logging.info('SENDING EMAIL')
    message = MIMEText(class_df_to_html(df), 'html')
    message['to'] = planning_sender
    message['from'] = planning_recipient
    message['subject'] = 'R2 Planning Update - Unexpected Hind Classes'
    body = {'raw': base64.urlsafe_b64encode(message.as_bytes()).decode()}

    gmail_service = build('gmail', 'v1', credentials=get_credentials())
    try:
        gmail_service.users().messages().send(userId='me', body=body).execute()
    except HttpError:
        logging.error('HTTP ERROR SENDING MESSAGE')


def check_planning():
    logging.info(log_now() + ' : STARTING PLANNING_CHECK')
    unknown_classes = get_new_classes()

    hind_classes = filter_classes_with_values(unknown_classes, dict({'Instructor': ['Hind M']}))
    is_not_jeudi = hind_classes['Dow'] != 'Jeu'
    is_not_night = (hind_classes['Time'].str.split(':').map(lambda x: x[0])).astype(int) < 17
    unexpected_hind_classes = hind_classes[is_not_night & is_not_jeudi]
    logging.info('FOUND ' + str(unexpected_hind_classes.shape[0]) + ' UNEXPECTED CLASSES')
    if unexpected_hind_classes.shape[0]:
        send_email(unexpected_hind_classes)
    logging.info(log_now() + ' : PLANNING_CHECK FINISHED ' + '\n' + '*' * 99)


if __name__ == '__main__':
    check_planning()
