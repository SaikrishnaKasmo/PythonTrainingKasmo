from __future__ import annotations
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import base64
import io
import numpy as np

SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]

def get_header(name,headers):
    return next((h['value'] for h in headers if h['name'].lower() == name.lower()), None)

def transform(session,response,service):

    s3 = session.client('s3')
    mails= []

    for msg in response.get('messages',[]):
        msg_id = msg['id']
        full_msg = service.users().messages().get(userId = 'me', id = msg_id, format = 'full').execute()
        payload = full_msg.get('payload', {})
        headers = payload.get('headers', [])
        sender = get_header('From',headers)
        receiver = get_header('To',headers)
        cc = get_header('Cc',headers)
        subject = get_header('Subject',headers)
        date = get_header('Date',headers)

        attachments = []

        for subpart in payload.get('parts',[]):
            filename = subpart.get('filename')
            if filename :
                att_id = subpart['body'].get('attachmentId')
                if att_id:
                    att = service.users().messages().attachments().get(userId='me',messageId = msg_id, id = att_id).execute()
                    file_data = base64.urlsafe_b64decode(att['data'])
                    file_obj = io.BytesIO(file_data)
                    s3.upload_fileobj(file_obj, 'gmail-kasmo', filename)
                    url = f"https://gmail-kasmo.s3.us-east-1.amazonaws.com/{filename}"
                    attachments.append(url)

        data = {"msg_id":msg_id,"sender":sender,"receiver":receiver,"cc":cc,"subject":subject,"date":date}

        if len(attachments) > 0:
            data['attachments_1'] = attachments[0]
        
        if len(attachments) > 1:
            data['attachments_2'] = attachments[1]

        mails.append(data)

    return mails 

