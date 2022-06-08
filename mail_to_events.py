import datetime
import json
import locale
from base64 import b64decode
from bs4 import BeautifulSoup
from google_manager import GmailService, CalendarService
from tqdm import tqdm
import logging

locale.setlocale(locale.LC_ALL, 'fr_FR.UTF-8')
logging.basicConfig(filename='nosync/app.log', filemode='a', level=logging.DEBUG)

time_min_events = (datetime.datetime.utcnow() - datetime.timedelta(days=30))

auto_desc = "Automatically generated event by r2cal.py"
subjects = {'R2 Training - Annulation du cours',
            'R2 Training - Réservation validée',
            'R2 Training - Réservation Confirmée',
            "R2 Training - Inscription en liste d'attente"}


def class_to_event(class_name, class_instructor, class_location, class_datetime_dt):
    return {
        'summary': class_name + ' (' + class_instructor + ')',
        'location': 'R2 ' + class_location,
        'start': {
            'dateTime': class_datetime_dt.strftime("%Y-%m-%dT%H:%M:%S"),
            'timeZone': 'Europe/Paris',
        },
        'end': {
            'dateTime': (class_datetime_dt + datetime.timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S"),
            'timeZone': 'Europe/Paris',
        },
        'description': auto_desc
    }


def event_is_class(cal_event, class_event):
    if cal_event['summary'] == class_event['summary']:
        if cal_event['location'] == class_event['location']:
            if cal_event['start']['dateTime'][:19] == class_event['start']['dateTime'][:19]:
                return True
    return False


def txt_to_class_variables(text):
    if 'parts' in text['payload'].keys():
        data = text['payload']['parts'][0]['parts'][0]['body']['data']
    else:
        data = text['payload']['body']['data']
    html_body = b64decode(data.replace("-", "+").replace("_", "/"))
    soup = BeautifulSoup(html_body, "lxml")
    class_info = soup.findAll('blockquote')[0].get_text()[:-1]
    class_name, class_info = class_info.split(' avec ') if 'avec' in class_info else class_info.split(' with ')
    class_instructor, class_info = class_info.split(' à ') if ' à ' in class_info else class_info.split(' on ')
    class_datetime_str, class_location = class_info.split(
        " dans l'espace ") if "dans l'espace" in class_info else class_info.split(" at ")
    try:
        class_datetime_dt = datetime.datetime.strptime(class_datetime_str, '%d %B %Y %H:%M')
    except ValueError:
        class_datetime_dt = datetime.datetime.strptime(class_datetime_str, '%d %b. %Y %H:%M')
    return class_location, class_datetime_dt, class_datetime_str, class_instructor, class_name


def add_or_remove_in_calendar(cal_service, email_subject, class_variables, upcoming_classes):
    location, datetime_dt, datetime_str, instructor, name = class_variables
    if datetime_dt > time_min_events:
        class_as_event = class_to_event(name, instructor, location, datetime_dt)
        waitlist_class_as_event = class_to_event('WAITLIST - ' + name, instructor, location, datetime_dt)
        matching_events = [e for e in upcoming_classes if event_is_class(e, class_as_event)]
        matching_waitlist_events = [e for e in upcoming_classes if event_is_class(e, waitlist_class_as_event)]
        if 'Annulation' in email_subject:
            for e in matching_events:
                cal_service.delete_event(e, authorized_desc=auto_desc)
        elif 'attente' in email_subject:
            if len(matching_waitlist_events) == 0:
                logging.debug('adding WAITLIST class to calendar')
                e = cal_service.add_event(waitlist_class_as_event)
                upcoming_classes.append(e)
            else:
                logging.debug('WAITLIST class already in calendar')
        else:  # booking
            if 'Confirmée' in email_subject:
                for e in matching_waitlist_events:
                    logging.debug('deleting WAITLIST class')
                    cal_service.delete_event(e, authorized_desc=auto_desc)
            if len(matching_events) == 0:
                logging.debug('adding class to calendar')
                e = cal_service.add_event(class_as_event)
                upcoming_classes.append(e)
            else:
                logging.debug('class already in calendar')
    else:
        logging.debug('Class too old, not editing calendar')


def analyse_messages():
    logging.info(datetime.datetime.now().strftime('%Y %b %d %H:%M:%S') + ' : STARTING MAIL_TO_EVENTS')
    gmail_service = GmailService()
    cal_service = CalendarService()
    ids_known_msgs = set(json.load(open('nosync/read_msgs.json', 'r')))

    msgs = gmail_service.fetch_labelled_messages(labelIds=['UNREAD', 'INBOX'])
    msgs_ids = [msg['id'] for msg in msgs if msg['id'] not in ids_known_msgs]
    ids_known_msgs = ids_known_msgs | set(msgs_ids)

    upcoming_classes = cal_service.fetch_upcoming_events(time_min_events, authorized_desc=auto_desc)

    logging.info(str(len(msgs)) + ' MESSAGES FOUND, ' + str(len(msgs_ids)) + ' UNKNOWN')
    logging.info(str(len(upcoming_classes)) + ' CLASSES FOUND')

    for msg_id in tqdm(msgs_ids):
        txt = gmail_service.read_email_with_id(msg_id)
        sender = [u for u in txt['payload']['headers'] if u['name'].lower() == 'from'][0]['value']

        if sender == '"contact@r2training.fr" <noreply@zingfitstudio.com>':
            subject = [u for u in txt['payload']['headers'] if u['name'].lower() == 'subject'][0]['value']
            logging.debug('Analyzing message : ' + subject)

            if subject in subjects:
                class_variables = txt_to_class_variables(txt)
                location, datetime_dt, datetime_str, instructor, name = class_variables

                logging.debug(' - '.join([location, datetime_str, instructor, name, subject.split(' - ')[-1]]))
                add_or_remove_in_calendar(cal_service, subject, class_variables, upcoming_classes)

                logging.debug('updating labels')
                gmail_service.edit_message_labels(msg_id=txt['id'], add=["Label_44"], remove=['UNREAD', 'INBOX'])

    with open('nosync/read_msgs.json', 'w') as f:
        f.write(json.dumps(list(ids_known_msgs)))
    logging.info(datetime.datetime.now().strftime('%Y %b %d %H:%M:%S') + ' : MAIL_TO_EVENTS FINISHED' + '\n' + '*' * 99)


if __name__ == '__main__':
    analyse_messages()
