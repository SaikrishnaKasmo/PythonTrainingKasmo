from __future__ import annotations
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import base64
import io
import numpy as np

SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]

def extract():
    flow = InstalledAppFlow.from_client_secrets_file("./data/client.json",SCOPES)
    creds = flow.run_local_server(port = 0)
    service = build('gmail','v1',credentials = creds)
    response = service.users().messages().list(
        userId = 'me',
        q='is:unread',
        maxResults = 10
    ).execute()

    return service,response
