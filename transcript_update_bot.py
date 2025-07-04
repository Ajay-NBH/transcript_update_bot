import os

import datetime
import os.path
import time
import base64
import traceback
from email.mime.text import MIMEText
import io # For GDrive downloads
import re 
import json
import fitz
import requests

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from zoneinfo import ZoneInfo

import google.generativeai as genai

SCOPES = [
    'https://www.googleapis.com/auth/calendar',
    'https://www.googleapis.com/auth/gmail.send',
    'https://www.googleapis.com/auth/drive.readonly',
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/documents'
]

token = "token_brandvmeet.json"
CREDENTIALS_FILE = "brand_vmeet_creds.json"
creds = None
if os.path.exists(token):
    creds = Credentials.from_authorized_user_file(token, SCOPES)
if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    else:
        flow = InstalledAppFlow.from_client_secrets_file(
            CREDENTIALS_FILE, SCOPES
        )
        creds = flow.run_local_server(port=0)
        with open(token, "w") as token:
            token.write(creds.to_json())

# Build drive service
drive_service = build("drive", "v3", credentials=creds)

# Build sheet service
sheets_service = build("sheets", "v4", credentials=creds)

# Build docs service
docs_service = build('docs', 'v1', credentials=creds)

# Write a function to fetch transcript payload using fireflies API
API_URL = "https://api.fireflies.ai/graphql"
FIREFLY_API_KEY = os.getenv("FIREFLY_API_KEY")
if not FIREFLY_API_KEY:
    raise ValueError("FIREFLY_API_KEY environment variable is not set.")

# GraphQL query: use limit & skip; transcripts returns a list
query = """
query Transcripts($limit: Int, $skip: Int) {
  transcripts(limit: $limit, skip: $skip) {
    id
    calendar_id      # Google Calendar event ID
    transcript_url   # Dashboard URL
    title
    sentences {
      index
      speaker_name
      speaker_id
      text
      raw_text
      start_time
      end_time
    }
    # add any other fields you need here…
  }
}
"""

headers = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json",
}

def fetch_all_transcripts(limit=50):
    all_transcripts = []
    skip = 0

    while True:
        payload = {
            "query": query,
            "variables": {"limit": limit, "skip": skip}
        }
        r = requests.post(API_URL, json=payload, headers=headers)
        r.raise_for_status()
        data = r.json()

        # handle GraphQL errors
        if "errors" in data:
            raise RuntimeError(f"GraphQL Error: {data['errors']}")

        batch = data["data"]["transcripts"]
        if not batch:
            break

        all_transcripts.extend(batch)

        skip += limit
        print(f"{len(all_transcripts)} processed")
        # End the loop once more than 100 transcripts are fetched
        
        if len(all_transcripts) > 100:
            break



    return all_transcripts

def complete_transcript(sentences):
    complete_text = ""
    if not sentences:
        return complete_text
    else:
        for sentence in sentences:
            start_time = sentence["start_time"]
            end_time = sentence["end_time"]
            speaker = sentence["speaker_name"]
            text = sentence["text"]
            complete_text += f"Time (in seconds): {start_time} to {end_time}\n"
            complete_text += f"{speaker}: {text}\n\n"
    return complete_text

# Write a function to create a doc file in a particular folder and return doc id
def create_google_doc_in_folder(drive_service, folder_id, doc_name, text, transcript_id):
    doc_id = None
    try:
        # Create a Google Doc in the specified folder
        file_metadata = {
            'name': doc_name,
            'mimeType': 'application/vnd.google-apps.document',
            'parents': [folder_id]
        }
        created = drive_service.files().create(
            body=file_metadata,
            fields='id, name, parents'
        ).execute()
        
        print(f"Created Google Doc: {created['name']} (ID: {created['id']})")
        
        doc_id = created['id']
        # Write the content to the Google Doc
        requests = [
            {
                'insertText': {
                    'location': { 'index': 1 },
                    'text': text
                }
            }
        ]
    
        docs_service.documents().batchUpdate(
            documentId=doc_id,
            body={'requests': requests}
        ).execute()
        
        print(f"Written content in file: {created['name']} (ID: {created['id']})")
        
        # Update the file with the transcript ID as an app property
        # This is useful for later retrieval or tagging
        drive_service.files().update(
            fileId=doc_id,
            body={
                'appProperties': {
                    'transcript_id': transcript_id
                }
            }
        ).execute()
        
        print(f"Tagged the file: {created['name']} with transcript id: {transcript_id}")
    
    except Error as e:
        print(f"An error occured while creating google doc {e}")
    return doc_id

# Write a function to update the transcript sheet and to update master sheet with doc link
def write_data_into_sheets(sheets_service, sheet_id, range, data):
    
    values = data

    body = {
        'values': values
    }
    try: 
        result = sheets_service.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range=range,
            valueInputOption='RAW',
            body=body
        ).execute()
        print(f"Updated values: {data} in sheet: {sheet_id}")
    except:
        print(f"An error occured while writing {data} values in sheet: {sheet_id}")

def read_data_from_sheets(sheets_service, sheet_id, range):

    try:
        result = (
                sheets_service.spreadsheets()
                .values()
                .get(spreadsheetId=sheet_id, range=range)
                .execute()
            )
        sheet_data = result.get("values", [])
        print(f"{len(sheet_data)} rows retrieved")
        return sheet_data
    except HttpError as error:
        print(f"An error occurred: {error}")    


def get_doc_with_t_id(drive_service, folder_id, transcript_id):
    q = (
    f"'{folder_id}' in parents and "
    f"appProperties has {{ key='transcript_id' and value='{transcript_id}' }} and "
        "mimeType='application/vnd.google-apps.document' and trashed=false"
    )
    resp = drive_service.files().list(
        q=q,
        fields="files(id, name, webViewLink)"
    ).execute()

    files = resp.get('files', [])
    if files:
        # we found at least one matching Doc
        doc = files[0]
        link = doc.get('webViewLink', f"https://docs.google.com/document/d/{doc['id']}")
        print("Found in folder:", doc['name'])
        return link
        
    else:
        print(f"No existing doc tagged with transcript_id={transcript_id}")
        return None
    

def main():
    transcripts = fetch_all_transcripts()
    print(f"Fetched {len(transcripts)} transcripts from Fireflies API")
    
    transcript_sheet_id = "1tEwCsqu-lThnaf_Z8i_X4-pUNzEYuy62Q-fkzsvGRzI"
    transcript_folder_id = "1EqbAFfiaKWJh051mX_fzIvig917Ofvy7"
    master_sheet_id = "1xtB1KUAXJ6IKMQab0Sb0NJfQppCKLkUERZ4PMZlNfOw"
    transcript_ids = read_data_from_sheets(sheets_service, transcript_sheet_id, "Sheet1!C2:C")

    if not transcripts:
        print("Something went wrong while fetching transcripts")
        return

    for i, t in enumerate(transcripts):
        t_id = t["id"]
        t_event_id = t["calendar_id"]
        t_sentences = t["sentences"]
        t_title = t["title"]
        if t_sentences is None:
            t_complete_text = " "
            meeting_duration = "0.0"
        else:
            t_complete_text = complete_transcript(t_sentences)
            meeting_duration = (t_sentences[-1]["end_time"] - t_sentences[0]["start_time"])/60
            meeting_duration = f"{meeting_duration: .2f}"
            
        if [t_id] in transcript_ids: # If the t_id exists in transcript sheet then do not process it
            continue
        
        doc_url = get_doc_with_t_id(drive_service, transcript_folder_id, t_id)
        
        if doc_url is not None: # If there's a doc already present in the folder, just get the link and update in the sheet
            
            ff_url = f"https://app.fireflies.ai/view/{t_id}"

            # Write data into transcript record sheet
            data_ts_sheet = [[t_event_id, t_title, t_id, doc_url, ff_url, meeting_duration]]

            body = {
                'values': data_ts_sheet
            }
            try:
                result = sheets_service.spreadsheets().values().append(
                    spreadsheetId=transcript_sheet_id,
                    range="Sheet1",
                    valueInputOption='RAW',
                    insertDataOption='INSERT_ROWS',
                    body=body
                ).execute()
                print(f"Appended row: {t_title} to transcript sheet")
            except:
                print("An error occurred while writing into sheets")

            
        else: # If doc with the given transcript is not present in the folder then create one and stamp it with transcript id
            
            # Create a transcript doc in the folder and get it's ID
            
            doc_id = create_google_doc_in_folder(drive_service, transcript_folder_id, t_title, t_complete_text, t_id)
            
            if doc_id is None:
                print("Moving on to next transcript")
                continue

            # Create doc URL
            doc_url = f"https://docs.google.com/document/d/{doc_id}"
            ff_url = f"https://app.fireflies.ai/view/{t_id}"

            # Write data into transcript record sheet
            data_ts_sheet = [[t_event_id, t_title, t_id, doc_url, ff_url, meeting_duration]]

            body = {
                'values': data_ts_sheet
            }
            try:
                result = sheets_service.spreadsheets().values().append(
                    spreadsheetId=transcript_sheet_id,
                    range="Sheet1",
                    valueInputOption='RAW',
                    insertDataOption='INSERT_ROWS',
                    body=body
                ).execute()
                print(f"Appended row: {t_title} to transcript sheet")
            except:
                print("An error occurred while writing into sheets")
                
        
        if (i+1)%50 == 0:
            print("Sleep initiated")
            time.sleep(50)

    print("All transcripts processed successfully")
    # Updating master sheet with the doc links by comparing with the transcript sheet
    transcript_sheet_data = read_data_from_sheets(sheets_service, transcript_sheet_id, "Sheet1!A2:F")
    ts_dict = []
    for t in transcript_sheet_data:
        dict = {}
        dict["calendar_id"] = t[0]
        dict["event_name"] = t[1]
        dict["transcript_id"] = t[2]
        dict["transcript_url"] = t[3]
        dict["firefly_url"] = t[4]
        if len(t) == 6:
            dict["meeting_duration"] = t[5]
        else:
            dict["meeting_duration"] = ''
        ts_dict.append(dict)
    
    transcript_urls_from_master = read_data_from_sheets(sheets_service, master_sheet_id, "Meeting_data!I2:I")
    calendar_ids_from_master = read_data_from_sheets(sheets_service, master_sheet_id, "Meeting_data!A2:A")
    
    for t in ts_dict:
        url = t["transcript_url"]
        cal_id = t["calendar_id"]
        if [url] in transcript_urls_from_master:
            continue
        if [cal_id] in calendar_ids_from_master:
            index = calendar_ids_from_master.index([cal_id]) + 2
            data = [[url, t["meeting_duration"]]]
            range = f"Meeting_data!I{index}:J{index}"
            write_data_into_sheets(sheets_service, master_sheet_id, range, data)
            print(f"Updated transcript in master sheet for {t['event_name']}")
        


if __name__ == "__main__":
    main()           
        

        

        
