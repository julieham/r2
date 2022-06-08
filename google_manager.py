import base64
import logging
import os
from email.mime.text import MIMEText

from google.auth.exceptions import RefreshError
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
from googleapiclient.errors import HttpError

logging.getLogger('googleapiclient.discovery').setLevel(logging.WARNING)
logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.WARNING)


def get_credentials():
    scopes = ['https://www.googleapis.com/auth/gmail.modify', 'https://www.googleapis.com/auth/calendar']
    cwd = os.getcwd() + '/'
    cred = None
    if os.path.exists(cwd + 'nosync/token.json'):
        cred = Credentials.from_authorized_user_file(cwd + 'nosync/token.json', scopes)
    if not cred or not cred.valid:
        if cred and cred.expired and cred.refresh_token:
            try:
                logging.info('REFRESHING TOKEN')
                cred.refresh(Request())
            except RefreshError:
                logging.info('FAILED, AUTHENTICATING WITH HUMAN')
                flow = InstalledAppFlow.from_client_secrets_file(cwd + 'nosync/credentials.json', scopes)
                cred = flow.run_local_server(port=0)
        else:
            logging.info('AUTHENTICATING WITH HUMAN')
            flow = InstalledAppFlow.from_client_secrets_file(cwd + 'nosync/credentials.json', scopes)
            cred = flow.run_local_server(port=0)
        with open(cwd + 'nosync/token.json', 'w') as token:
            logging.info('SAVING TOKEN')
            token.write(cred.to_json())
    return cred


class GmailService:

    def __init__(self, credentials=None):
        if not credentials:
            credentials = get_credentials()
        self.service = build('gmail', 'v1', credentials=credentials)

    def read_email_with_id(self, msg_id):
        return self.service.users().messages().get(userId='me', id=msg_id).execute()

    def fetch_labelled_messages(self, labelIds, maxResults=200):
        return self.service.users().messages().list(userId='me', labelIds=labelIds, maxResults=maxResults) \
            .execute().get('messages')

    def edit_message_labels(self, msg_id, add, remove):
        self.service.users().messages().modify(userId='me', id=msg_id,
                                               body={"addLabelIds": add, "removeLabelIds": remove}).execute()

    def send_email(self, sender, recipient, subject, html_body):
        logging.info('SENDING EMAIL : ' + subject)
        message = MIMEText(html_body, 'html')
        message['to'] = sender
        message['from'] = recipient
        message['subject'] = subject
        body = {'raw': base64.urlsafe_b64encode(message.as_bytes()).decode()}

        gmail_service = build('gmail', 'v1', credentials=get_credentials())
        try:
            gmail_service.users().messages().send(userId='me', body=body).execute()
        except HttpError:
            logging.error('HTTP ERROR SENDING MESSAGE')

class CalendarService:

    def __init__(self, credentials=None):
        if not credentials:
            credentials = get_credentials()
        self.service = build('calendar', 'v3', credentials=credentials)

    def fetch_upcoming_events(self, start_date, authorized_desc=None):
        upcoming_events = self.service.events().list(calendarId='primary', timeMin=(start_date.isoformat() + 'Z'),
                                                maxResults=100, singleEvents=True,
                                                orderBy='startTime').execute().get('items', [])
        if authorized_desc:
            upcoming_events_w_desc = [e for e in upcoming_events if 'description' in e.keys()]
            return [e for e in upcoming_events_w_desc if e['description'] == authorized_desc]
        return upcoming_events

    def add_event(self, event, calendarId='primary'):
        return self.service.events().insert(calendarId=calendarId, body=event).execute()

    def delete_event(self, event, calendarId='primary', authorized_desc=None):
        if authorized_desc:
            if 'description' in event.keys():
                desc = event['description']
                if desc != authorized_desc:
                    logging.error('event cannot be deleted cause it was not automatically generated')
                    return
        self.service.events().delete(calendarId=calendarId, eventId=event['id']).execute()
        logging.warning('deleting event : ' + event['summary'])
