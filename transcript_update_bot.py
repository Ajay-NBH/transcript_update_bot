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

SCOPES = [
    'https://www.googleapis.com/auth/calendar',
    'https://www.googleapis.com/auth/gmail.send',
    'https://www.googleapis.com/auth/drive.readonly',
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/documents'
]

token = os.getenv("GOOGLE_TOKEN", "brand_vmeet_token.json")
CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS", "brand_vmeet_credentials.json")
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

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "AIzaSyBeyEkmnBeTAlHYhXpLotPyU1uG2zduDLw")
try:
    client = genai.Client(api_key = GEMINI_API_KEY)
    # Using a specific model version. 1.5 Flash is faster and cheaper for many tasks.
    # For higher quality, consider 'gemini-1.5-pro-latest'.
    print(f"Gemini model configured successfully.")
except Exception as e:
    print(f"Error configuring Gemini API: {e}")

class Analysis(BaseModel):
    """Pydantic model for the JSON structure returned by Gemini."""
    Meeting_Type: str
    Meeting_Agenda: str
    Key_Discussion_Points: list[str]
    Key_Questions: list[str]
    Marketing_Assets: list[str]
    Competition_Discussion: str
    Action_Items: list[dict]
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
    Specific_Competitor_Insights: list[dict]
    Key_Managerial_Summary: str
    Identified_Missed_Opportunities: list[str]
    Pitched_Asset_Relevance_to_Needs: str

def get_gemini_response_json(transcript_text, client):
    """Sends transcript text to Google Gemini API and retrieves raw insights text."""

#     department_prompt = department_prompts.get(department, "General Analysis")
    # meeting_duration = extract_meeting_duration(transcript_text)  # Extract duration

    prompt_template = """
        You are reading a transcript of an online call between the NoBrokerHood (NBH) Sales Team and brand representatives.
        The meeting could be online on Google Meet or similar channel or it could also be a physical meeting where the sales person has manually recorded the meeting and uploaded the recording to fireflies. It is also possible that the sales person recorded the meeting on Gmeet sitting in front of the brand representative where only the salesperson is getting recorded as the speaker.
        NBH pitches advertising solutions for gated communities while competing with MyGate (MG), Meta/Google (digital), Adonmo, and other BTL platforms.

        Your job: **return ONE JSON object** — no Markdown, no fences, no commentary — containing the fields defined below.
        If something cannot be inferred, output {{null}} (or an empty array for list fields).

        -------------------------------------------------------------------------
        🔹 VARIABLES (inserted before sending)
          • {transcript_text}    # the raw transcript
        -------------------------------------------------------------------------

        ## FIELD REFERENCE (name – description – type)

        1.  **Meeting Type** – Introductory / Follow‑up / Closure discussion / Post‑closure execution / Execution review – *string*
        2.  **Meeting Agenda** – one‑sentence goal – *string*
        3. **Key Discussion Points** – major themes – *array of strings*
        4. **Key Questions** – questions asked by brand – *array of strings*
        5. **Marketing Assets** – assets pitched by NBH – *array of strings*
        6. **Competition Discussion** – summary or “Not Discussed” – *string*
        7. **Action Items** – follow‑ups – *array of objects*
            └─  format: `{{{{ "owner": "", "task": "", "priority": "" }}}}` **`The 'priority' should be one of the following string values: 'Critical', 'Fast-Track', 'Normal', or 'Sometime/Maybe', judged based on urgency and impact on closure probability (higher impact on closure probability will lead to higher priority).`**
        8. **Rebuttal Handling** – **`Summary of objections raised by the client and an assessment of how effectively the NBH salesperson handled them (e.g., 'Effectively addressed pricing concerns by highlighting value', 'Struggled to counter objection about X feature', 'Objection Y was acknowledged but not fully resolved'). – string`**
        9. **Rapport Building** – quality of rapport building by the sales rep– *string*
        10. **Improvement Areas** – gaps to improve by the NBH salesperson – *array of strings*
        11. **Other Sales Parameters** – any extra sales observations – *array of strings*
        12. **Budget or Scope** – narrative estimate of client's budget or project scope discussed – *string*
        13. **Lead Category** – High / Medium / Low – *string* >> This categorization would need to be done on the basis of the relevance of the POC and the Brand which can lead to probable closure
        14. **Positive Factors** – deal positives – *array of strings*
        15. **Negative Factors** – deal risks – *array of strings*
        16. **Closure Score** – likelihood 0‑100 – *number*
        17. **Brand Traits** – personality traits of the client brand – *array of strings*
        18. **Tone of Voice** – client brand tone – *string*
        19. **Values & Mission** – client brand values & mission, if discussed – *string*
        **`20. Customer Needs – List the explicit needs, goals, or challenges articulated by the client during the meeting. Focus on what the client *said* they are trying to achieve or solve. This will be used to assess the relevance of pitched assets. – *array of strings*`**
        **`21. Need Identification – Assessment of the primary NBH salesperson's effectiveness in uncovering and understanding the client's core business needs and objectives. Provide a brief analysis (2-3 sentences), noting if discovery was thorough, superficial, or if key needs might have been missed. Include examples if illustrative. If not discussed or unclear, state so. – *string*`**
        **`22. Sales Pitch Rating – Overall rating of the sales pitch by the NBH salesperson on a scale of 1 - 10. Based on Rebuttal Handling, Rapport Building, Need Identification, Demo Flow, Communication Efficacy, Value Proposition Articulation, Product Knowledge Displayed and other relevant parameters observed in the transcript. – *number*`**
        **`23. Client Pain Points – Explicit problems or challenges the client is trying to solve, as stated by them. – *array of strings*`**
        **`24. Value Proposition Articulation – Summary of how effectively the NBH salesperson linked NBH's general offerings and value to the client's specific stated needs, pain points, or business situation. Note if connections were strong, weak, or missed. – *string*`**
        **`25. Product Knowledge Displayed – Assessment of the NBH salesperson's product/solution knowledge as demonstrated in the call (e.g., Confident and detailed, Generally good but hesitant on X, Lacked depth in Y). Note if a demo was given and its perceived effectiveness by the client, if mentioned. – *string*`**
        **`26. Call Effectiveness & Control – Assessment of how well the NBH salesperson managed the call flow, adhered to an agenda (if stated or apparent), and controlled the conversation. Note if the call stayed on track or frequently deviated. – *string*`**
        **`27. Next Steps Clarity & Commitment – Were the immediate next steps in the sales process clearly outlined and agreed upon by the client during the call? (e.g., 'Client agreed to a follow-up demo next Tuesday', 'Next steps unclear', 'NBH to send proposal, client to review with team by Friday'). – *string*`**
        **`28. Overall Client Sentiment – Overall mood and engagement level of the client during the meeting. Choose one or a short description: "Highly Engaged & Positive / Attentive & Inquisitive / Neutral & Reserved / Skeptical / Disengaged / Negative". – *string*`**
        **`29. Specific Competitor Insights – Insights into competitors mentioned by the client. – *array of objects*`**
            **`└─  format: {{{{ "competitor_name": "", "client_perception_or_insight": "" }}}}`**
        **`30. Key Managerial Summary – The single most important takeaway or status update from this meeting for a sales manager (1-2 sentences). – *string*`**
        **`31. Identified Missed Opportunities – Opportunities or client cues that the NBH salesperson seemed to miss or did not fully explore during the call. – *array of strings*`**
        **`32. Pitched Asset Relevance to Needs – Based *solely on the information within the transcript*, assess the relevance of the marketing assets pitched by the NBH salesperson to the client's explicitly stated needs, challenges, or goals identified during the call. Did the salesperson attempt to connect the pitched assets to these identified client needs? How clear and logical was this connection (e.g., Very clear, Somewhat clear, Unclear, Not addressed, Connection made but seemed weak/irrelevant)? Provide a brief narrative summary (2-4 sentences). If specific assets were pitched, mention if their relevance to a stated need was well-explained, poorly explained, or not explained by the salesperson. If no specific needs were clearly articulated by the client, or if no assets were pitched in direct response to an identified need, note that. – *string*`**

        
        -------------------------------------------------------------------------
        
                -------------------------------------------------------------------------
                ⚠️ STRICT RULES  
                1. **Do NOT rename, add, or omit keys.**  
                2. Lists must be JSON arrays; single items must be strings or numbers (no comma‑joined strings).  
                3. Put None in *Competition Discussion* if the topic never came up.  
                4. Keep *Meeting Duration* in “NN Min” format and *Closure Score* as a number.  
                5. Entire response = the JSON object above (no ```json fences, no explanation). 
        
        
                """
    
    prompt_json = prompt_template.format(
    transcript_text=transcript_text)

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
            print(f"Updated transcript in master sheet at row {index} for {t['event_name']}")
    
    # Here I will run an analysis on the transcript using genai and update the master sheet with the analysis
    transcript_urls_from_master = read_data_from_sheets(sheets_service, master_sheet_id, "Meeting_data!I2:I")
    t_ids = []

    for i, t in enumerate(transcript_urls_from_master):
        t_dict = {}
        
        if len(t) == 0:
            continue
            
        if t[0] == 'Transcript not uploaded':
            continue
            
        t_dict["id"] = t[0].split('/')[5]
        t_dict["sheet_index"] = i+2
        t_ids.append(t_dict)
    
    for t in t_ids[-25:]:
        doc_id = t["id"]
        sheet_index = t["sheet_index"]
        file = drive_service.files().get(
            fileId=doc_id, 
            fields='appProperties, owners, createdTime, modifiedTime'
        ).execute()
        
        processed = file.get('appProperties').get('processed', None)
        
        if not processed:
            transcript_text = read_doc_text(docs_service, doc_id)
            if not transcript_text:
                print(f"Transcript text is empty for doc ID: {doc_id}. Skipping analysis.")
                continue
            print(f"Running analysis for doc ID: {doc_id}")
            analysis = get_gemini_response_json(transcript_text, client)
            if analysis is None:
                print(f"Failed to get valid analysis for doc ID: {doc_id}. Skipping update.")
                continue
            data = []
            for value in analysis.values():
                if isinstance(value, str):
                    data.append([value])
                else:
                    data.append([f"{value}"])
            
            rng = f"Meeting_data!N{sheet_index}:AS{sheet_index}"
            
            try:
                write_data_into_sheets(sheets_service, master_sheet_id, rng, data)
                print(f"Updated analysis for doc ID: {doc_id} at row {sheet_index}")
            except Exception as e:
                print(f"An error occurred while writing analysis for doc ID: {doc_id} at row {sheet_index}: {e}")
            



if __name__ == "__main__":
    main()           
        

        

        
