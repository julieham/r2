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
                try:
                    name = c.find("span", {"class": "scheduleClass"}).string.replace('\n', '').replace('\t', '')
                    # class_length = c.find("span", {"class": "classlength"}).string
                    c.find('span', class_='classlength').decompose()
                except AttributeError:
                    name = c.find("span", {"class": "scheduleCancelled"}).string.replace('\n', '').replace('\t', '')

                time = c.find("span", {"class": "scheduleTime"}).string.replace(' ', '').split('(')[0]

                if name != 'Open Gym':
                    new_row = {'Dow': days[i],
                               'Date': dates[i],
                               'Time': time,
                               'Site': sites_dico[site_id],
                               'Class': name,
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
    # string_columns = df.dtypes.index[(df.dtypes == 'object') & (df.columns != "Instructor")].tolist()
    string_columns = ['Dow', 'Date', 'Time', 'Site', 'Class']
    return df[sorted(string_columns)].apply(lambda row: row.str.cat(), axis=1)


def get_known_classes():
    return pd.read_csv(classes_history, parse_dates=[7], index_col=0, dtype=str)


def get_all_classes_no_dupes():
    csv = get_known_classes()
    csv['id'] = get_classes_id(csv)
    csv['origin'] = 'csv'

    web = get_weeks_of_classes(3)
    web['id'] = get_classes_id(web)
    web['origin'] = 'web'
    cols = [u for u in web.columns if u != 'origin']
    return pd.concat([csv, web]).drop_duplicates(subset=cols, keep='first').sort_values(by='Datetime', ascending=True)


def warn_for_schedule_change(classes):
    duplicates = [the_id for the_id, count in classes['id'].value_counts().items() if count > 1]

    df_dup = classes[classes['id'].isin(duplicates)].sort_values(by=['Date', 'daytime', 'Site', 'origin', 'Datetime'],
                                                                 ascending=True)

    for group_info, the_classes in df_dup.groupby(by=['Dow', 'Date', 'daytime', 'Site', 'Class']):
        str_group_info = (' '.join(group_info[:3]) + ' @' + group_info[3] + ' - ' + group_info[4])[:50].ljust(50)
        the_classes = the_classes[['Instructor', 'origin']].drop_duplicates()
        if the_classes.shape[0] == 2 and set(the_classes['origin']) == {'web', 'csv'}:
            the_classes = the_classes.set_index('origin')
            str_prof_info = the_classes.loc['csv'][0][:20].rjust(20) + ' > ' + the_classes.loc['web'][0].upper()
            logging.info(str_group_info + str_prof_info)
        else:
            logging.warning('DUPLICATE CLASSES ' + str_group_info)
            for _, u in the_classes.iterrows():
                logging.warning(u['Instructor'] + ' (' + u['origin'] + ')')


def get_new_classes(warning_schedule_change=True, memorize=True):
    keep_columns = ['Dow', 'Date', 'Time', 'Site', 'Class', 'Instructor', 'Datetime']
    classes = get_all_classes_no_dupes()
    classes['Date_dt'] = classes['Datetime'].dt.date
    classes['daytime'] = classes['Time'].str[:2].astype(int).apply(
        lambda x: 'matin' if x < 12 else ('midi ' if x < 17 else 'soir '))
    classes = classes.sort_values(by=['Date_dt', 'Site', 'Datetime']).reset_index(drop=True)
    if warning_schedule_change:
        warn_for_schedule_change(classes)
    if memorize:
        classes[keep_columns].to_csv(classes_history)
    return classes[classes['origin'] == 'web'][keep_columns]


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
