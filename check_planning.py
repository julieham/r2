import datetime
import locale
import logging
import urllib
import pandas as pd
from itertools import product
from bs4 import BeautifulSoup
from tqdm import tqdm

from google_manager import GmailService
from nosync.param import *

logging.basicConfig(filename='nosync/app.log', filemode='a', level=logging.DEBUG)
pd.set_option('mode.chained_assignment', None)
locale.setlocale(locale.LC_ALL, 'fr_FR.UTF-8')

year_now = datetime.datetime.now().year
sites_dico = {'3': 'Bastille', '2': "Pereire", "1": "Vendome"}
classes_history = "nosync/all_classes.csv"


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


def get_known_classes():
    return pd.read_csv(classes_history, parse_dates=[7], index_col=0, dtype=str)


def get_new_classes():
    known_classes = get_known_classes()
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


def send_planning_update_email(classes, gmail_service=None):
    df_as_html = class_df_to_html(classes)
    if not gmail_service:
        gmail_service = GmailService()
    gmail_service.send_email(planning_sender, planning_recipient, planning_subject, df_as_html)


def check_planning():
    logging.info(log_now() + ' : STARTING PLANNING_CHECK')
    unknown_classes = get_new_classes()

    target_classes = filter_classes_with_values(unknown_classes, dict(target_filter))
    target_avoid_filter_1 = target_classes[target_avoid_and_1[0]].isin(target_avoid_and_1[1]) == False
    target_avoid_filter_2 = target_classes[target_avoid_and_2[0]].isin(target_avoid_and_2[1]) == False
    unexpected_target_classes = target_classes[target_avoid_filter_1 & target_avoid_filter_2]

    logging.info('FOUND ' + str(unexpected_target_classes.shape[0]) + ' UNEXPECTED CLASSES')
    if unexpected_target_classes.shape[0]:
        send_planning_update_email(unexpected_target_classes)
    logging.info(log_now() + ' : PLANNING_CHECK FINISHED ' + '\n' + '*' * 99)


if __name__ == '__main__':
    check_planning()
