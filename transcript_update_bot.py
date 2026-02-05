import os

import datetime
import os.path
import time
import base64
import traceback
import io # For GDrive downloads
import re 
import json
import requests
import time

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

from google import genai
from google.genai import types
from pydantic import BaseModel, ValidationError
import enum
from data_config import column_index
import pandas as pd

SCOPES = [
    'https://www.googleapis.com/auth/calendar',
    'https://www.googleapis.com/auth/gmail.send',
    'https://www.googleapis.com/auth/drive',  # This includes drive.readonly
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/documents'
]

token = "brand_vmeet_token.json"  # Use fixed filename
CREDENTIALS_FILE = "brand_vmeet_credentials.json"  # Use fixed filename

creds = None
if os.path.exists(token):
    print(f"Loading credentials from {token}")
    creds = Credentials.from_authorized_user_file(token, SCOPES)
else:
    print(f"ERROR: Token file {token} not found!")

if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
        print("Refreshing expired credentials...")
        try:
            creds.refresh(Request())
            print("✅ Credentials refreshed successfully")
        except Exception as e:
            print(f"❌ Failed to refresh credentials: {e}")
            print(f"Token scopes: {creds.scopes if creds else 'N/A'}")
            print(f"Required scopes: {SCOPES}")
            raise
    else:
        print("ERROR: No valid credentials and cannot run interactive flow in GitHub Actions")
        raise ValueError("Authentication failed - token may be invalid or missing refresh_token")

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
    "Authorization": f"Bearer {FIREFLY_API_KEY}",
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
        
        if len(all_transcripts) > 500:
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
    
    except Exception as e:
        print(f"An error occured while creating google doc {e}")
    return doc_id

# Write a function to update the transcript sheet and to update master sheet with doc link
def write_data_into_sheets(sheets_service, sheet_id, range, data):
    """Write data with retry logic"""
    return write_with_retry(sheets_service, sheet_id, range, data)

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

def read_doc_text(docs_service, document_id):
    """Fetches a Google Doc and returns its full text as one string."""
    doc = docs_service.documents().get(documentId=document_id).execute()
    content = doc.get('body', {}).get('content', [])

    full_text = []
    for structural_element in content:
        # Paragraphs, headings, lists all sit under 'paragraph'
        paragraph = structural_element.get('paragraph')
        if not paragraph:
            continue

        for elem in paragraph.get('elements', []):
            text_run = elem.get('textRun')
            if not text_run:
                continue
            # Append the raw text (including newlines)
            full_text.append(text_run.get('content', ''))

    return ''.join(full_text)
    # ============= NEW RETRY LOGIC FUNCTIONS =============

import time
from googleapiclient.errors import HttpError

def write_with_retry(sheets_service, sheet_id, range, data, max_retries=5):
    """Write data with exponential backoff retry"""
    for attempt in range(max_retries):
        try:
            body = {'values': data}
            result = sheets_service.spreadsheets().values().update(
                spreadsheetId=sheet_id,
                range=range,
                valueInputOption='USER_ENTERED',
                body=body
            ).execute()
            print(f"Updated values: {data} in sheet: {sheet_id}")
            return True
        except HttpError as error:
            if error.resp.status == 429:  # Rate limit error
                wait_time = (2 ** attempt) + 1  # Exponential backoff: 2s, 5s, 9s, 17s...
                print(f"Rate limit hit. Waiting {wait_time} seconds before retry {attempt + 1}/{max_retries}...")
                time.sleep(wait_time)
            else:
                print(f"An error occurred: {error}")
                return False
    
    print(f"Failed after {max_retries} retries")
    return False


def batch_write_two_ranges_with_retry(sheets_service, spreadsheet_id, range1, values1, range2, values2, max_retries=5):
    """Batch write with retry logic"""
    for attempt in range(max_retries):
        try:
            if not values1 or not values2:
                print("No data to write in one or both ranges.")
                return None
                
            body = {
                "valueInputOption": "USER_ENTERED",
                "data": [
                    {"range": range1, "values": values1},
                    {"range": range2, "values": values2},
                ],
            }

            resp = (
                sheets_service.spreadsheets()
                .values()
                .batchUpdate(spreadsheetId=spreadsheet_id, body=body)
                .execute()
            )

            total_cells = sum(r.get("updatedCells", 0) for r in resp["responses"])
            print(f"Done. {total_cells} cells updated across both ranges.")
            return resp

        except HttpError as err:
            if err.resp.status == 429:  # Rate limit error
                wait_time = (2 ** attempt) + 1
                print(f"Rate limit hit. Waiting {wait_time} seconds before retry {attempt + 1}/{max_retries}...")
                time.sleep(wait_time)
            else:
                print(f"Sheets API error: {err}")
                return None
    
    print(f"Failed after {max_retries} retries")
    return None


def batch_write_multiple_ranges(sheets_service, spreadsheet_id, updates_list, max_retries=5):
    """
    Write multiple ranges in a single API call with retry logic
    updates_list: [{"range": "Sheet!A1:B1", "values": [[data]]}, ...]
    """
    for attempt in range(max_retries):
        try:
            if not updates_list:
                return None
                
            body = {
                "valueInputOption": "USER_ENTERED",
                "data": updates_list
            }

            resp = (
                sheets_service.spreadsheets()
                .values()
                .batchUpdate(spreadsheetId=spreadsheet_id, body=body)
                .execute()
            )

            total_cells = sum(r.get("updatedCells", 0) for r in resp["responses"])
            print(f"Done. {total_cells} cells updated across {len(updates_list)} ranges.")
            return resp

        except HttpError as err:
            if err.resp.status == 429:  # Rate limit error
                wait_time = (2 ** attempt) + 1
                print(f"Rate limit hit. Waiting {wait_time} seconds before retry {attempt + 1}/{max_retries}...")
                time.sleep(wait_time)
            else:
                print(f"Sheets API error: {err}")
                return None
    
    print(f"Failed after {max_retries} retries")
    return None


class RateLimiter:
    """Rate limiter to prevent hitting API quotas"""
    def __init__(self, max_calls=45, period=60):
        self.max_calls = max_calls  # Keep under 60/min limit
        self.period = period
        self.calls = []
    
    def wait_if_needed(self):
        now = time.time()
        # Remove calls older than the period
        self.calls = [call_time for call_time in self.calls if now - call_time < self.period]
        
        if len(self.calls) >= self.max_calls:
            sleep_time = self.period - (now - self.calls[0]) + 2
            print(f"Rate limit approaching ({len(self.calls)} calls in last minute). Sleeping for {sleep_time:.1f} seconds...")
            time.sleep(sleep_time)
            self.calls = []
        
        self.calls.append(time.time())

# ============= END NEW FUNCTIONS =============

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
try:
    client = genai.Client(api_key = GEMINI_API_KEY)
    # Using a specific model version. 1.5 Flash is faster and cheaper for many tasks.
    # For higher quality, consider 'gemini-1.5-pro-latest'.
    print(f"Gemini model configured successfully.")
except Exception as e:
    print(f"Error configuring Gemini API: {e}")

class Brand_Size(enum.Enum):
    NATIONAL = "National"
    REGIONAL = "Regional"
    CITY_LEVEL = "City Level"
    UNKNOWN = "Unknown"

class PitchDirection(enum.Enum):
    DIGITAL = "Digital Only"
    PHYSICAL = "Physical Only"
    HYBRID = "Hybrid"
    UNKNOWN = "Unknown"

class YesNo(enum.Enum):
    YES = "Yes"
    NO = "No"
    NA = "NA"

class RapportLevel(enum.Enum):
    HIGH = "High"
    MEDIUM = "Medium"
    LOW = "Low

class ActionItem(BaseModel):
    owner: str
    task: str
    priority: str

class CompetitorInsight(BaseModel):
    competitor_name: str
    client_perception_or_insight: str



class Analysis(BaseModel):
    """Pydantic model for the JSON structure returned by Gemini."""
    Brand_Size: Brand_Size
    Meeting_Type: str
    Meeting_Agenda: str
    Key_Discussion_Points: list[str]
    Key_Questions: list[str]
    Marketing_Assets: list[str]
    Competition_Discussion: str
    Action_Items: list[ActionItem]
    Rebuttal_Handling: str
    Rapport_Building: str
    Improvement_Areas: list[str]
    Other_Sales_Parameters: list[str]
    Budget_or_Scope: str
    Lead_Category: str
    Positive_Factors: list[str]
    Negative_Factors: list[str]
    Closure_Score: int
    Brand_Traits: list[str]
    Tone_of_Voice: str
    Values_and_Mission: str
    Customer_Needs: list[str]
    Need_Identification: str
    Sales_Pitch_Rating: int
    Client_Pain_Points: list[str]
    Value_Proposition_Articulation: str
    Product_Knowledge_Displayed: str
    Call_Effectiveness_and_Control: str
    Next_Steps_Clarity_and_Commitment: str
    Overall_Client_Sentiment: str
    Specific_Competitor_Insights: list[CompetitorInsight]
    Key_Managerial_Summary: str
    Identified_Missed_Opportunities: list[str]
    Pitched_Asset_Relevance_to_Needs: str
    Pre_vs_Post_Meeting_Score: str
    Pitch_Direction: PitchDirection
    Was_Digital_Inventory_Pitched: YesNo
    Was_Island_Banner_or_Video_Pitched: YesNo
    Was_Physical_Inventory_Pitched: YesNo
    Offline_Assets_Proposed: list[str]
    Was_Lift_Branding_Pitched: YesNo
    Were_Success_Stories_Cited: YesNo
    Which_Brand_Stories_Cited: str
    Were_ROI_Metrics_Promised: YesNo
    Details_of_Promised_Metrics: str
    Was_Pilot_Offered: YesNo
    Objection_Handling_MyGate: str
    Client_vs_NBH_Participant_Speaking_Ratio: str
    Were_Clear_Next_Steps_Established: YesNo
    Immediate_Next_Action: str
    Confidence_Score: int
    Communication_Clarity_Score: int
    Energy_Engagement_Score: int
    Rapport_Building_Capability: RapportLevel

    class Config:
        use_enum_values = True  # Use enum values in the output

audit_params = ["Brand_Size","Meeting_Type","Rebuttal_Handling", "Rapport_Building", 
                "Improvement_Areas", "Other_Sales_Parameters", 
                "Need_Identification", "Value_Proposition_Articulation", 
                "Product_Knowledge_Displayed", "Call_Effectiveness_and_Control", 
                "Next_Steps_Clarity_and_Commitment", "Identified_Missed_Opportunities", 
                "Pitched_Asset_Relevance_to_Needs", "Pre_vs_Post_Meeting_Score"
                ]  # Parameters to be audited
business_params = ["Brand_Size", "Meeting_Type", "Meeting_Agenda", "Key_Discussion_Points",
                  "Key_Questions", "Marketing_Assets", "Competition_Discussion",
                  "Action_Items", "Budget_or_Scope",
                  "Lead_Category", "Positive_Factors", "Negative_Factors",
                  "Closure_Score", "Brand_Traits", "Tone_of_Voice",
                  "Values_and_Mission", "Customer_Needs",
                  "Sales_Pitch_Rating", "Client_Pain_Points",
                  "Overall_Client_Sentiment", 
                  "Specific_Competitor_Insights",
                  "Key_Managerial_Summary"
                ]  # Parameters to be written in master sheet

def get_gemini_response_json(prompt_template, transcript_text, pm_brief_text, client):
    """Sends transcript text to Google Gemini API and retrieves raw insights text."""

#     department_prompt = department_prompts.get(department, "General Analysis")
    # meeting_duration = extract_meeting_duration(transcript_text)  # Extract duration
    
    prompt_json = prompt_template.format(
    transcript_text=transcript_text,
    pm_brief_text=pm_brief_text)

    config = types.GenerateContentConfig(
            response_mime_type="application/json",
            response_schema=Analysis
        )
    
    try:
        response = client.models.generate_content(model="gemini-2.5-flash",contents=prompt_json, config=config)
        parsed: Analysis = response.parsed
        return parsed.model_dump()  # Return the parsed JSON object as a dictionary
    except ValidationError as e:
        print(f"Validation error: {e}")
        return None
    except genai.exceptions.GoogleGenAIError as e:
        print(f"Google GenAI error: {e}")
        return None

def batch_write_two_ranges(sheets_service, spreadsheet_id, range1, values1, range2, values2, value_input_option = "USER_ENTERED"):
    """Wrapper function that uses retry logic"""
    return batch_write_two_ranges_with_retry(sheets_service, spreadsheet_id, range1, values1, range2, values2)

def main():
    transcripts = fetch_all_transcripts()
    print(f"Fetched {len(transcripts)} transcripts from Fireflies API")
    
    transcript_sheet_id = "1tEwCsqu-lThnaf_Z8i_X4-pUNzEYuy62Q-fkzsvGRzI"
    transcript_folder_id = "1EqbAFfiaKWJh051mX_fzIvig917Ofvy7"
    master_sheet_id = "1xtB1KUAXJ6IKMQab0Sb0NJfQppCKLkUERZ4PMZlNfOw"
    master_sheet_column_headers = read_data_from_sheets(sheets_service, master_sheet_id, "Meeting_data!A1:BU1")[0] #reading the column headers from master sheet
    owner_column_master = master_sheet_column_headers.index("Owner") + 1 # Getting the index of Owner column in master sheet
    owner_column_letter_master = column_index[f"{owner_column_master}"] # Getting the column letter for Owner column
    owner_update_column_master = master_sheet_column_headers.index("Owner sheet to be updated") + 1
    owner_update_column_master_letter = column_index[f"{owner_update_column_master}"] # Getting the column letter for Owner sheet to be updated column

    audit_sheet_column_headers = read_data_from_sheets(sheets_service, master_sheet_id, "Audit_and_Training!A1:BU1")[0] #reading the column headers from audit and training sheet
    owner_column_audit = audit_sheet_column_headers.index("Owner") + 1 # Getting the index of Owner column in audit sheet
    owner_column_letter_audit = column_index[f"{owner_column_audit}"] # Getting the column letter for Owner column
    owner_update_column_audit = audit_sheet_column_headers.index("Owner sheet to be updated") + 1
    owner_update_column_audit_letter = column_index[f"{owner_update_column_audit}"] # Getting the column letter for Owner sheet to be updated column

    # Read the transcript IDs from the transcript sheet
    transcript_ids = read_data_from_sheets(sheets_service, transcript_sheet_id, "Sheet1!C2:C")

    if not transcripts:
        print("Something went wrong while fetching transcripts")
        return
    # Read the prompt template from the prompts sheet
    prompts_sheet_id = "1_dKfSF_WkANgSNvFbMTR43By_sK74XKWUr9fTzire5s"
    ts_analysis_tab = "Transcript_analysis"
    rng = f"{ts_analysis_tab}!A2:A2"
    ts_analysis_prompt = read_data_from_sheets(sheets_service, prompts_sheet_id, rng)
    prompt_template = ts_analysis_prompt[0][0]

    for i, t in enumerate(transcripts):
        meeting_conducted = "Not Conducted"
        t_id = t["id"]
        t_event_id = t["calendar_id"]
        t_sentences = t["sentences"]
        t_title = t["title"]
        
        if t_sentences is None or not t_sentences:
            # If Fireflies hasn't finished transcribing, SKIP this meeting.
            # We will catch it on the next run when sentences are ready.
            print(f"Skipping {t_title} (ID: {t_id}) - Transcript still processing or empty.")
            continue 

        t_complete_text = complete_transcript(t_sentences)
        meeting_duration = (
            t_sentences[-1]["end_time"] - t_sentences[0]["start_time"]
        ) / 60

        if meeting_duration > 10.0 and len(t_complete_text) > 10:
                meeting_conducted = "Conducted"

        meeting_duration = f"{meeting_duration:.2f}"

        if [t_id] in transcript_ids:  # If the t_id exists in transcript sheet then do not process it
            continue

        doc_url = get_doc_with_t_id(drive_service, transcript_folder_id, t_id)

        if doc_url is not None:  # If there's a doc already present in the folder, just get the link and update in the sheet
            
            ff_url = f"https://app.fireflies.ai/view/{t_id}"

            # Write data into transcript record sheet

            data_ts_sheet = [[t_event_id, t_title, t_id, doc_url, ff_url, meeting_duration, meeting_conducted]]

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
                time.sleep(1.1) # <--- ADD THIS LINE to slow down writes
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
            data_ts_sheet = [[t_event_id, t_title, t_id, doc_url, ff_url, meeting_duration, meeting_conducted]]

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
    transcript_sheet_data = read_data_from_sheets(sheets_service, transcript_sheet_id, "Sheet1!A2:G")
    ts_dict = []
    for t in transcript_sheet_data:
        dict = {}
        dict["calendar_id"] = t[0]
        dict["event_name"] = t[1]
        dict["transcript_id"] = t[2]
        dict["transcript_url"] = t[3]
        dict["firefly_url"] = t[4]
        if len(t) > 5:
            dict["meeting_duration"] = t[5]
            dict["meeting_conducted"] = t[6] if len(t) > 6 else ''
        else:
            dict["meeting_duration"] = ''
            dict["meeting_conducted"] = ''
        ts_dict.append(dict)
    
    transcript_urls_from_master = read_data_from_sheets(sheets_service, master_sheet_id, "Meeting_data!I2:I")
    calendar_ids_from_master = read_data_from_sheets(sheets_service, master_sheet_id, "Meeting_data!A2:A")
    
     # Initialize rate limiter
    rate_limiter = RateLimiter(max_calls=45, period=60)
    
    # Collect all updates first for batching
    all_updates = []
    processed_count = 0
    
    for t in ts_dict:
        url = t["transcript_url"]
        cal_id = t["calendar_id"]
        meeting_done = t.get("meeting_conducted", None)
        
        if [url] in transcript_urls_from_master:
            continue
            
        if [cal_id] in calendar_ids_from_master:
            index = calendar_ids_from_master.index([cal_id]) + 2
            
            # Collect updates instead of writing immediately
            all_updates.append({
                "range": f"Meeting_data!I{index}:J{index}",
                "values": [[url, t["meeting_duration"]]]
            })
            all_updates.append({
                "range": f"Audit_and_Training!I{index}:J{index}",
                "values": [[url, t["meeting_duration"]]]
            })
            all_updates.append({
                "range": f"Meeting_data!{owner_update_column_master_letter}{index}",
                "values": [["TRUE"]]
            })
            all_updates.append({
                "range": f"Audit_and_Training!{owner_update_column_audit_letter}{index}",
                "values": [["TRUE"]]
            })
            
            if meeting_done:
                conducted_status_flag_column = column_index[str(master_sheet_column_headers.index("Meeting Done") + 1)]
                all_updates.append({
                    "range": f"Meeting_data!{conducted_status_flag_column}{index}",
                    "values": [[meeting_done]]
                })
            
            processed_count += 1
            print(f"Queued updates for {t['event_name']} at row {index}")
            
            # Write in batches of 10 to avoid rate limits
            if len(all_updates) >= 40:  # 40 ranges = ~10 transcripts worth of updates
                rate_limiter.wait_if_needed()
                success = batch_write_multiple_ranges(sheets_service, master_sheet_id, all_updates)
                if success:
                    print(f"Successfully wrote batch of {len(all_updates)} ranges")
                else:
                    print(f"Failed to write batch")
                all_updates = []
                time.sleep(2)  # Additional safety delay
    
    # Write any remaining updates
    if all_updates:
        rate_limiter.wait_if_needed()
        success = batch_write_multiple_ranges(sheets_service, master_sheet_id, all_updates)
        if success:
            print(f"Successfully wrote final batch of {len(all_updates)} ranges")
        else:
            print(f"Failed to write final batch")
        time.sleep(2)
    
    print(f"Completed updating {processed_count} transcripts in master sheet")
    
    # Here I will run an analysis on the transcript using genai and update the master sheet with the analysis
    transcript_urls_from_master = read_data_from_sheets(sheets_service, master_sheet_id, "Meeting_data!I2:I")
    transcript_urls_from_ts_sheet = read_data_from_sheets(sheets_service, transcript_sheet_id, "Sheet1!D2:D")
    pm_brief_urls_from_master = read_data_from_sheets(sheets_service, master_sheet_id, "Meeting_data!H2:H")
    t_ids = []

    for i, t in enumerate(transcript_urls_from_ts_sheet):
        t_dict = {}
        
        if len(t) == 0:
            continue
            
        if t[0] == 'Transcript not uploaded':
            continue
            
        sheet_index = transcript_urls_from_master.index(t) if t in transcript_urls_from_master else None
        if sheet_index:
            t_dict["id"] = t[0].split('/')[5]
            t_dict["sheet_index"] = sheet_index+2
            t_ids.append(t_dict)
    
    for t in t_ids[-1000:]:
        doc_id = t["id"]
        sheet_index = t["sheet_index"]
        file = drive_service.files().get(
            fileId=doc_id, 
            fields='appProperties, owners, createdTime, modifiedTime'
        ).execute()
        
        processed = file.get('appProperties').get('processed', None)
        
        if not processed:
            pm_brief_id = None
            if len(pm_brief_urls_from_master) >= sheet_index-1:
                if pm_brief_urls_from_master[sheet_index-2]:
                    pm_brief_url = pm_brief_urls_from_master[sheet_index-2][0]
                    pm_brief_id = pm_brief_url.split('/')[5] if pm_brief_url else None
            
            transcript_text = read_doc_text(docs_service, doc_id)
            if pm_brief_id:
                pm_brief_text = read_doc_text(docs_service, pm_brief_id)
            else:
                pm_brief_text = ""

            if not transcript_text:
                print(f"Transcript text is empty for doc ID: {doc_id}. Skipping analysis.")
                continue
            print(f"Running analysis for doc ID: {doc_id}")
            analysis = get_gemini_response_json(prompt_template, transcript_text, pm_brief_text, client)
            if analysis is None:
                print(f"Failed to get valid analysis for doc ID: {doc_id}. Skipping update.")
                continue
            # --- 1. DEFINE NEW KEYS (For Columns AN-BB) ---
            new_column_keys = [
                "Pitch_Direction", 
                "Was_Digital_Inventory_Pitched", 
                "Was_Island_Banner_or_Video_Pitched",
                "Was_Physical_Inventory_Pitched", 
                "Offline_Assets_Proposed", 
                "Was_Lift_Branding_Pitched",
                "Were_Success_Stories_Cited", 
                "Which_Brand_Stories_Cited", 
                "Were_ROI_Metrics_Promised",
                "Details_of_Promised_Metrics", 
                "Was_Pilot_Offered", 
                "Objection_Handling_MyGate",
                "Client_vs_NBH_Participant_Speaking_Ratio", 
                "Were_Clear_Next_Steps_Established",
                "Immediate_Next_Action"
                "Confidence_Score",
                "Communication_Clarity_Score",
                "Energy_Engagement_Score",
                "Rapport_Building_Capability"
            ]

            data = []       # Bucket 1: Old Business Data (K-AF)
            audit_data = [] # Bucket 2: Old Audit Data
            new_col_data = [] # Bucket 3: New Data (AN-BB)

            # --- 2. PROCESS DATA ---
            for key, value in analysis.items():
                formatted_val = f"{value}" if not isinstance(value, str) else value
                
                # A. Handle NEW columns separately
                if key in new_column_keys:
                    continue # Skip this loop, we handle new keys below
                
                # B. Handle OLD columns (Logic remains 100% identical to before)
                if key in audit_params:
                    audit_data.append(formatted_val)
                if key in business_params:
                    data.append(formatted_val)

            # --- 3. PROCESS NEW COLUMNS STRICTLY IN ORDER ---
            for key in new_column_keys:
                val = analysis.get(key, "")
                # Format lists (e.g., Offline Assets) into a single string
                if isinstance(val, list):
                    val = ", ".join(val)
                # Handle Enums if Pydantic didn't auto-convert
                if hasattr(val, 'value'): 
                    val = val.value 
                new_col_data.append(str(val))

            # --- 4. DEFINE WRITING RANGES ---
            # Range 1: OLD Business Data (K to AF)
            rng_existing = f"Meeting_data!K{sheet_index}:AF{sheet_index}"
            
            # Range 2: OLD Audit Data
            rng_audit = f"Audit_and_Training!K{sheet_index}:X{sheet_index}"
            
            # Range 3: NEW Data (AN to BB) - Starts after your manual columns
            rng_new = f"Meeting_data!AN{sheet_index}:BF{sheet_index}"

            # --- 5. EXECUTE BATCH WRITE ---
            updates = [
                {"range": rng_existing, "values": [data]},
                {"range": rng_audit, "values": [audit_data]},
                {"range": rng_new, "values": [new_col_data]}
            ]
            
            rate_limiter.wait_if_needed()
            
            # We use 'batch_write_multiple_ranges' (defined in your code) instead of 'batch_write_two_ranges'
            success = batch_write_multiple_ranges(sheets_service, master_sheet_id, updates)
           
            # Tagging the transcript as processed
            if success:
                print(f"Updated analysis for doc ID: {doc_id} at row {sheet_index}")
                drive_service.files().update(
                    fileId=doc_id,
                    body={
                        'appProperties': {
                            'processed': True
                        }
                    }
                ).execute()
                
                # Resetting the owner sheet update flag
                print(f"Resetting the owner sheet update flag to TRUE at row {sheet_index}")
                data = [["TRUE"]]
                rng = f"Meeting_data!{owner_update_column_master_letter}{sheet_index}:{owner_update_column_master_letter}{sheet_index}"
                audit_rnge = f"Audit_and_Training!{owner_update_column_audit_letter}{sheet_index}:{owner_update_column_audit_letter}{sheet_index}"
                
                # Use rate limiter before writing
                rate_limiter.wait_if_needed()
                success2 = batch_write_two_ranges(sheets_service, master_sheet_id, rng, data, audit_rnge, data)
                if success2:
                    print(f"Owner sheet update flag reset successfully")
                else:
                    print(f"Failed to reset owner sheet update flag")
            else:
                print(f"Failed to update analysis for doc ID: {doc_id} at row {sheet_index}")
            
            # Longer delay after analysis (these are expensive operations)
            time.sleep(3)

if __name__ == "__main__":
    main()


         
        

        

             
        

        

               

        

        
