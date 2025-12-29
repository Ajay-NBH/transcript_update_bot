import os
import time
import json
import traceback
import requests
import ssl
import enum
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google import genai
from google.genai import types
from pydantic import BaseModel, ValidationError, ConfigDict

# ==========================================
# CONFIGURATION & AUTHENTICATION
# ==========================================

# Load environment variables
FIREFLY_API_KEY = os.getenv("FIREFLY_API_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GOOGLE_TOKEN_JSON = os.getenv("GOOGLE_TOKEN_JSON") # Put the content of token.json in GitHub Secrets
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON") # Optional

SCOPES = [
    'https://www.googleapis.com/auth/calendar',
    'https://www.googleapis.com/auth/gmail.send',
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/documents'
]

def get_credentials():
    creds = None
    # 1. Try loading from Environment Variable (GitHub Actions Production)
    if GOOGLE_TOKEN_JSON:
        try:
            creds_dict = json.loads(GOOGLE_TOKEN_JSON)
            creds = Credentials.from_authorized_user_info(creds_dict, SCOPES)
        except Exception as e:
            print(f"Warning: Could not load token from env: {e}")

    # 2. Try loading from local file (Local Development)
    if not creds and os.path.exists("brand_vmeet_token.json"):
        creds = Credentials.from_authorized_user_file("brand_vmeet_token.json", SCOPES)

    # 3. Refresh or Login
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except Exception:
                print("Token expired and refresh failed. Re-authentication required.")
                return None
        else:
            # Interactive login (Local only)
            if os.path.exists("brand_vmeet_credentials.json"):
                flow = InstalledAppFlow.from_client_secrets_file(
                    "brand_vmeet_credentials.json", SCOPES
                )
                creds = flow.run_local_server(port=0)
                with open("brand_vmeet_token.json", "w") as token:
                    token.write(creds.to_json())
            else:
                return None
    return creds

creds = get_credentials()
if not creds:
    raise Exception("CRITICAL: Authentication failed. No valid token found.")

# Build Services
drive_service = build("drive", "v3", credentials=creds)
sheets_service = build("sheets", "v4", credentials=creds)
docs_service = build('docs', 'v1', credentials=creds)

# ==========================================
# DATA MODELS (GEMINI)
# ==========================================

class Brand_Size(enum.Enum):
    NATIONAL = "National"
    REGIONAL = "Regional"
    CITY_LEVEL = "City Level"
    UNKNOWN = "Unknown"

class ActionItem(BaseModel):
    owner: str
    task: str
    priority: str

class CompetitorInsight(BaseModel):
    competitor_name: str
    client_perception_or_insight: str

class Analysis(BaseModel):
    model_config = ConfigDict(use_enum_values=True)
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

audit_params = ["Brand_Size","Meeting_Type","Rebuttal_Handling", "Rapport_Building", 
                "Improvement_Areas", "Other_Sales_Parameters", 
                "Need_Identification", "Value_Proposition_Articulation", 
                "Product_Knowledge_Displayed", "Call_Effectiveness_and_Control", 
                "Next_Steps_Clarity_and_Commitment", "Identified_Missed_Opportunities", 
                "Pitched_Asset_Relevance_to_Needs", "Pre_vs_Post_Meeting_Score"]

business_params = ["Brand_Size", "Meeting_Type", "Meeting_Agenda", "Key_Discussion_Points",
                  "Key_Questions", "Marketing_Assets", "Competition_Discussion",
                  "Action_Items", "Budget_or_Scope",
                  "Lead_Category", "Positive_Factors", "Negative_Factors",
                  "Closure_Score", "Brand_Traits", "Tone_of_Voice",
                  "Values_and_Mission", "Customer_Needs",
                  "Sales_Pitch_Rating", "Client_Pain_Points",
                  "Overall_Client_Sentiment", 
                  "Specific_Competitor_Insights",
                  "Key_Managerial_Summary"]

# ==========================================
# HELPER FUNCTIONS
# ==========================================

def get_col_letter(n):
    """Converts 1 to A, 2 to B, 27 to AA, etc."""
    string = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        string = chr(65 + remainder) + string
    return string

def read_data_from_sheets(service, sheet_id, range_name):
    """Safe read that returns [] instead of crashing on None."""
    try:
        result = service.spreadsheets().values().get(spreadsheetId=sheet_id, range=range_name).execute()
        return result.get("values", [])
    except HttpError as error:
        print(f"Error reading {range_name}: {error}")
        return []
    except Exception as e:
        print(f"Unexpected error reading sheet: {e}")
        return []

def batch_update_cells(service, spreadsheet_id, updates):
    """Updates multiple scattered ranges in one API call."""
    if not updates:
        return
    body = {
        "valueInputOption": "USER_ENTERED",
        "data": updates
    }
    try:
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=spreadsheet_id, body=body
        ).execute()
        print(f"Batch updated {len(updates)} ranges.")
    except Exception as e:
        print(f"Error during batch update: {e}")

def fetch_all_transcripts(limit=50):
    API_URL = "https://api.fireflies.ai/graphql"
    query = """
    query Transcripts($limit: Int, $skip: Int) {
      transcripts(limit: $limit, skip: $skip) {
        id
        calendar_id
        transcript_url
        title
        sentences {
          index
          speaker_name
          start_time
          end_time
          text
        }
      }
    }
    """
    headers = {"Authorization": f"Bearer {FIREFLY_API_KEY}", "Content-Type": "application/json"}
    all_transcripts = []
    skip = 0
    
    # Cap at 100 to prevent infinite loops or quota issues
    while len(all_transcripts) < 100:
        payload = {"query": query, "variables": {"limit": limit, "skip": skip}}
        try:
            r = requests.post(API_URL, json=payload, headers=headers)
            r.raise_for_status()
            data = r.json()
            batch = data.get("data", {}).get("transcripts", [])
            if not batch:
                break
            all_transcripts.extend(batch)
            skip += limit
            print(f"Fetched {len(all_transcripts)} transcripts from Fireflies...")
        except Exception as e:
            print(f"Error fetching Fireflies: {e}")
            break
            
    return all_transcripts

def complete_transcript(sentences):
    if not sentences: return ""
    text = ""
    for s in sentences:
        text += f"Time: {s['start_time']} to {s['end_time']}\n{s['speaker_name']}: {s['text']}\n\n"
    return text

def create_google_doc(drive_service, docs_service, folder_id, title, text, transcript_id):
    try:
        # Create Doc
        file_metadata = {'name': title, 'mimeType': 'application/vnd.google-apps.document', 'parents': [folder_id]}
        doc = drive_service.files().create(body=file_metadata, fields='id, webViewLink').execute()
        doc_id = doc.get('id')
        
        # Insert Text
        requests_body = [{'insertText': {'location': {'index': 1}, 'text': text}}]
        docs_service.documents().batchUpdate(documentId=doc_id, body={'requests': requests_body}).execute()
        
        # Tag File
        drive_service.files().update(
            fileId=doc_id,
            body={'appProperties': {'transcript_id': transcript_id}}
        ).execute()
        
        print(f"Created Doc: {title}")
        return doc.get('webViewLink'), doc_id
    except Exception as e:
        print(f"Error creating doc: {e}")
        return None, None

def read_doc_text(docs_service, document_id):
    try:
        doc = docs_service.documents().get(documentId=document_id).execute()
        content = doc.get('body', {}).get('content', [])
        full_text = []
        for element in content:
            paragraph = element.get('paragraph')
            if paragraph:
                for elem in paragraph.get('elements', []):
                    text_run = elem.get('textRun')
                    if text_run:
                        full_text.append(text_run.get('content', ''))
        return ''.join(full_text)
    except Exception:
        return ""

def get_gemini_response_json(prompt_template, transcript_text, pm_brief_text, client):
    # Safety truncation
    safe_transcript = transcript_text[:90000] 
    prompt_json = prompt_template.format(transcript_text=safe_transcript, pm_brief_text=pm_brief_text)
    
    config = types.GenerateContentConfig(response_mime_type="application/json", response_schema=Analysis)
    try:
        response = client.models.generate_content(model="gemini-2.5-flash", contents=prompt_json, config=config)
        return response.parsed.model_dump()
    except Exception as e:
        print(f"Gemini API Error: {e}")
        return None

# ==========================================
# MAIN LOGIC
# ==========================================

def main():
    print("--- Starting Transcript Bot ---")
    
    # IDs
    transcript_sheet_id = "1tEwCsqu-lThnaf_Z8i_X4-pUNzEYuy62Q-fkzsvGRzI"
    transcript_folder_id = "1EqbAFfiaKWJh051mX_fzIvig917Ofvy7"
    master_sheet_id = "1xtB1KUAXJ6IKMQab0Sb0NJfQppCKLkUERZ4PMZlNfOw"
    prompts_sheet_id = "1_dKfSF_WkANgSNvFbMTR43By_sK74XKWUr9fTzire5s"

    # ---------------------------------------------------------
    # PART 1: FETCH TRANSCRIPTS & SYNC TO TRANSCRIPT SHEET
    # ---------------------------------------------------------
    
    # Read existing transcript IDs to avoid duplicates
    existing_ids_raw = read_data_from_sheets(sheets_service, transcript_sheet_id, "Sheet1!C2:C")
    existing_ids_set = set(row[0] for row in existing_ids_raw if row)
    
    # Fetch new from Fireflies
    transcripts = fetch_all_transcripts(limit=50)
    
    # Pre-fetch existing docs in Drive to avoid creating duplicates (Optimization)
    print("Checking existing Drive files...")
    existing_docs_map = {} # {transcript_id: webViewLink}
    page_token = None
    while True:
        try:
            q = f"'{transcript_folder_id}' in parents and mimeType='application/vnd.google-apps.document' and trashed=false"
            response = drive_service.files().list(q=q, fields="nextPageToken, files(id, webViewLink, appProperties)", pageToken=page_token).execute()
            for f in response.get('files', []):
                props = f.get('appProperties')
                if props and 'transcript_id' in props:
                    existing_docs_map[props['transcript_id']] = f.get('webViewLink')
            page_token = response.get('nextPageToken')
            if not page_token: break
        except Exception as e:
            print(f"Drive list error: {e}")
            break

    new_rows = []
    
    print("Processing Transcripts...")
    for t in transcripts:
        t_id = t["id"]
        # Skip if already in sheet
        if t_id in existing_ids_set:
            continue
            
        title = t.get("title", "Untitled")
        sentences = t.get("sentences")
        
        # Calculate details
        duration_val = 0.0
        complete_text = ""
        conducted = "Not Conducted"
        
        if sentences:
            complete_text = complete_transcript(sentences)
            if sentences[-1]["end_time"] and sentences[0]["start_time"]:
                duration_val = (sentences[-1]["end_time"] - sentences[0]["start_time"]) / 60
            if duration_val > 10.0 and len(complete_text) > 10:
                conducted = "Conducted"
        
        duration_str = f"{duration_val:.2f}"
        ff_url = f"https://app.fireflies.ai/view/{t_id}"
        
        # Check Drive cache or create new
        doc_url = existing_docs_map.get(t_id)
        if not doc_url:
            # Create new doc
            doc_url, _ = create_google_doc(drive_service, docs_service, transcript_folder_id, title, complete_text, t_id)
            if not doc_url:
                continue # Skip on failure
        
        new_rows.append([t["calendar_id"], title, t_id, doc_url, ff_url, duration_str, conducted])

    # Batch write new rows to Transcript Sheet
    if new_rows:
        try:
            body = {'values': new_rows}
            sheets_service.spreadsheets().values().append(
                spreadsheetId=transcript_sheet_id, range="Sheet1!A:G",
                valueInputOption='USER_ENTERED', insertDataOption='INSERT_ROWS', body=body
            ).execute()
            print(f"Appended {len(new_rows)} new transcripts to sheet.")
        except Exception as e:
            print(f"Error appending to sheet: {e}")

    # ---------------------------------------------------------
    # PART 2: SYNC TO MASTER SHEET (BATCH OPTIMIZED)
    # ---------------------------------------------------------
    
    print("Syncing Master Sheet...")
    # Get column headers to find indices dynamically
    master_headers = read_data_from_sheets(sheets_service, master_sheet_id, "Meeting_data!A1:ZZ1")[0]
    audit_headers = read_data_from_sheets(sheets_service, master_sheet_id, "Audit_and_Training!A1:ZZ1")[0]
    
    try:
        # Find critical columns
        col_owner_update_master = get_col_letter(master_headers.index("Owner sheet to be updated") + 1)
        col_owner_update_audit = get_col_letter(audit_headers.index("Owner sheet to be updated") + 1)
        col_meeting_done = get_col_letter(master_headers.index("Meeting Done") + 1)
    except ValueError:
        print("Error: Could not find required column headers in Master/Audit sheets.")
        return

    # Read data for matching
    ts_data = read_data_from_sheets(sheets_service, transcript_sheet_id, "Sheet1!A2:G")
    master_cal_ids = [r[0] if r else "" for r in read_data_from_sheets(sheets_service, master_sheet_id, "Meeting_data!A2:A")]
    master_urls = [r[0] if r else "" for r in read_data_from_sheets(sheets_service, master_sheet_id, "Meeting_data!I2:I")]
    
    batch_updates = []
    
    for row in ts_data:
        if len(row) < 7: continue
        cal_id, title, t_id, doc_url, ff_url, duration, conducted = row[:7]
        
        # Logic: If URL not in master but Cal ID is in master -> Update
        if doc_url in master_urls:
            continue
            
        if cal_id in master_cal_ids:
            row_idx = master_cal_ids.index(cal_id) + 2 # +2 because 0-index and header row
            
            # 1. Update Transcript URL & Duration in Meeting_data (Cols I, J)
            batch_updates.append({
                'range': f"Meeting_data!I{row_idx}:J{row_idx}",
                'values': [[doc_url, duration]]
            })
            # 2. Update Transcript URL & Duration in Audit_and_Training (Cols I, J)
            batch_updates.append({
                'range': f"Audit_and_Training!I{row_idx}:J{row_idx}",
                'values': [[doc_url, duration]]
            })
            # 3. Reset Owner Flag
            batch_updates.append({
                'range': f"Meeting_data!{col_owner_update_master}{row_idx}",
                'values': [["TRUE"]]
            })
            batch_updates.append({
                'range': f"Audit_and_Training!{col_owner_update_audit}{row_idx}",
                'values': [["TRUE"]]
            })
            # 4. Update "Meeting Done" status if applicable
            if conducted:
                batch_updates.append({
                    'range': f"Meeting_data!{col_meeting_done}{row_idx}",
                    'values': [[conducted]]
                })
    
    # Execute all master sheet updates in ONE call
    batch_update_cells(sheets_service, master_sheet_id, batch_updates)

    # ---------------------------------------------------------
    # PART 3: GEMINI ANALYSIS
    # ---------------------------------------------------------
    
    print("Starting Gemini Analysis...")
    try:
        client = genai.Client(api_key=GEMINI_API_KEY)
    except Exception:
        print("Skipping Analysis: Gemini Client init failed.")
        return

    # Get prompt
    raw_prompt = read_data_from_sheets(sheets_service, prompts_sheet_id, "Transcript_analysis!A2:A2")
    if not raw_prompt:
        print("Skipping Analysis: Prompt not found.")
        return
    prompt_template = raw_prompt[0][0]

    # Re-fetch Master URLs to identify what needs analysis
    master_urls_refresh = read_data_from_sheets(sheets_service, master_sheet_id, "Meeting_data!I2:I")
    pm_brief_urls = read_data_from_sheets(sheets_service, master_sheet_id, "Meeting_data!H2:H")
    
    # We look at the last 300 entries as per original logic
    # Find rows where Transcript is present
    
    processed_count = 0
    start_check_index = max(0, len(master_urls_refresh) - 300)
    
    for i in range(start_check_index, len(master_urls_refresh)):
        row_data = master_urls_refresh[i]
        if not row_data: continue
        
        url = row_data[0]
        if "docs.google.com" not in url: continue
        
        try:
            doc_id = url.split("/d/")[1].split("/")[0]
        except IndexError:
            continue
            
        sheet_index = i + 2
        
        # Check processing status in Drive
        try:
            f = drive_service.files().get(fileId=doc_id, fields="appProperties").execute()
            props = f.get('appProperties', {})
            if props.get('processed') == 'True':
                continue
        except Exception as e:
            print(f"Skipping doc {doc_id} due to error: {e}")
            continue

        print(f"Analyzing Doc ID: {doc_id} (Row {sheet_index})")
        
        # Get PM Brief if available
        pm_brief_text = ""
        if i < len(pm_brief_urls) and pm_brief_urls[i]:
            pm_brief_url = pm_brief_urls[i][0]
            if "docs.google.com" in pm_brief_url:
                try:
                    pm_id = pm_brief_url.split("/d/")[1].split("/")[0]
                    pm_brief_text = read_doc_text(docs_service, pm_id)
                except:
                    pass

        transcript_text = read_doc_text(docs_service, doc_id)
        if not transcript_text: continue

        # Call Gemini
        analysis = get_gemini_response_json(prompt_template, transcript_text, pm_brief_text, client)
        if not analysis: continue

        # Prepare Data for Write
        data_row = []
        audit_row = []
        
        for key, value in analysis.items():
            val_str = str(value)
            if key in audit_params: audit_row.append(val_str)
            if key in business_params: data_row.append(val_str)

        # Batch write the results for this single analysis (Analysis is slow, so writing row-by-row here is acceptable/safer)
        updates = [
            {'range': f"Meeting_data!K{sheet_index}:AF{sheet_index}", 'values': [data_row]},
            {'range': f"Audit_and_Training!K{sheet_index}:X{sheet_index}", 'values': [audit_row]},
            {'range': f"Meeting_data!{col_owner_update_master}{sheet_index}", 'values': [["TRUE"]]},
            {'range': f"Audit_and_Training!{col_owner_update_audit}{sheet_index}", 'values': [["TRUE"]]}
        ]
        
        batch_update_cells(sheets_service, master_sheet_id, updates)
        
        # Mark as processed
        drive_service.files().update(
            fileId=doc_id,
            body={'appProperties': {'processed': 'True'}}
        ).execute()
        
        processed_count += 1
        time.sleep(1) # Short pause to be nice to API

    print(f"Analysis complete. Processed {processed_count} documents.")

if __name__ == "__main__":
    main()    
        

        

        
