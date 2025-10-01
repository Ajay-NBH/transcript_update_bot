# Meeting Transcript Analysis System

An automated system that fetches meeting transcripts from Fireflies.ai, stores them in Google Docs, and performs AI-powered analysis using Google Gemini to extract business insights and sales metrics.

## Overview

This system automates the workflow of:
1. Fetching meeting transcripts from Fireflies.ai API
2. Creating/managing Google Docs for transcript storage
3. Analyzing transcripts using Google Gemini AI
4. Updating Google Sheets with analysis results and metrics

## Features

- **Automated Transcript Fetching**: Retrieves up to 100+ transcripts from Fireflies.ai
- **Google Drive Integration**: Creates and manages transcript documents with metadata tagging
- **AI-Powered Analysis**: Uses Google Gemini 2.5 Flash to extract 40+ business and sales metrics
- **Smart Deduplication**: Tracks processed transcripts to avoid redundant processing
- **Batch Updates**: Efficiently updates multiple Google Sheets simultaneously
- **Meeting Status Tracking**: Determines if meetings were actually conducted based on duration and content

## Prerequisites

### API Keys & Credentials
- Google Cloud Project with enabled APIs:
  - Google Calendar API
  - Gmail API
  - Google Drive API
  - Google Sheets API
  - Google Docs API
- Fireflies.ai API key
- Google Gemini API key

### Python Dependencies
```
google-auth
google-auth-oauthlib
google-auth-httplib2
google-api-python-client
python-dotenv
requests
pydantic
google-genai
pandas
```

## Environment Setup

Create a `.env` file with the following variables:

```env
GOOGLE_TOKEN=brand_vmeet_token.json
GOOGLE_CREDENTIALS=brand_vmeet_credentials.json
FIREFLY_API_KEY=your_fireflies_api_key
GEMINI_API_KEY=your_gemini_api_key
```

## Configuration Files

### Required Files
1. **Google Credentials** (`brand_vmeet_credentials.json`): OAuth 2.0 credentials from Google Cloud Console
2. **Token File** (`brand_vmeet_token.json`): Auto-generated after first authentication
3. **Data Config** (`data_config.py`): Contains column index mappings

### Google Sheets Structure

The system expects three main sheets:

1. **Transcript Sheet** (ID: `1tEwCsqu-lThnaf_Z8i_X4-pUNzEYuy62Q-fkzsvGRzI`)
   - Columns: Calendar ID, Title, Transcript ID, Doc URL, Firefly URL, Duration, Meeting Status

2. **Master Sheet** (ID: `1xtB1KUAXJ6IKMQab0Sb0NJfQppCKLkUERZ4PMZlNfOw`)
   - Two tabs: `Meeting_data` and `Audit_and_Training`
   - Contains comprehensive meeting analysis and metrics

3. **Prompts Sheet** (ID: `1_dKfSF_WkANgSNvFbMTR43By_sK74XKWUr9fTzire5s`)
   - Contains AI prompt templates for analysis

## Analysis Parameters

The system extracts and categorizes metrics into two groups:

### Business Parameters (22 metrics)
- Brand Size, Meeting Type, Meeting Agenda
- Key Discussion Points, Marketing Assets
- Competition Discussion, Action Items
- Budget/Scope, Lead Category
- Closure Score, Brand Traits
- Customer Needs, Client Pain Points
- Competitor Insights, Key Managerial Summary
- And more...

### Audit Parameters (14 metrics)
- Rebuttal Handling, Rapport Building
- Need Identification, Value Proposition
- Product Knowledge, Call Effectiveness
- Pre vs Post Meeting Score
- Identified Missed Opportunities
- And more...

## How It Works

### 1. Transcript Fetching
```python
transcripts = fetch_all_transcripts(limit=50)
```
- Retrieves transcripts in batches from Fireflies.ai
- Supports pagination with limit/skip parameters
- Stops after fetching 100+ transcripts

### 2. Document Creation
```python
doc_id = create_google_doc_in_folder(drive_service, folder_id, doc_name, text, transcript_id)
```
- Creates Google Docs in specified folder
- Tags documents with transcript ID for future retrieval
- Formats transcript with timestamps and speaker names

### 3. AI Analysis
```python
analysis = get_gemini_response_json(prompt_template, transcript_text, pm_brief_text, client)
```
- Sends transcript and pre-meeting brief to Gemini
- Returns structured JSON with 40+ data points
- Uses Pydantic models for validation

### 4. Sheet Updates
```python
batch_write_two_ranges(sheets_service, spreadsheet_id, range1, values1, range2, values2)
```
- Updates both Meeting_data and Audit_and_Training tabs simultaneously
- Resets owner sheet update flags
- Handles meeting status updates

## Usage

Run the main script:

```bash
python main.py
```

The system will:
1. Authenticate with Google services
2. Fetch new transcripts from Fireflies
3. Create/locate Google Docs for each transcript
4. Update the transcript tracking sheet
5. Sync transcript URLs to master sheet
6. Analyze unprocessed transcripts with AI
7. Update master sheet with analysis results

## Key Functions

| Function | Purpose |
|----------|---------|
| `fetch_all_transcripts()` | Retrieves transcripts from Fireflies API |
| `complete_transcript()` | Formats transcript with timestamps |
| `create_google_doc_in_folder()` | Creates and tags Google Docs |
| `get_doc_with_t_id()` | Finds existing docs by transcript ID |
| `read_doc_text()` | Extracts text from Google Docs |
| `get_gemini_response_json()` | Performs AI analysis |
| `batch_write_two_ranges()` | Updates multiple sheet ranges |

## Data Models

### Analysis Model
Comprehensive Pydantic model with fields including:
- Enum for Brand_Size (National, Regional, City Level)
- ActionItem model (owner, task, priority)
- CompetitorInsight model (competitor_name, perception)
- 40+ structured fields for business metrics

## Rate Limiting & Safety

- Sleep interval: 50 seconds after every 50 transcripts
- Batch processing limits: 300 most recent transcripts for analysis
- Meeting duration threshold: 10+ minutes for valid meetings
- Error handling for API failures and validation errors

## Folder Structure

```
Google Drive Folder ID: 1EqbAFfiaKWJh051mX_fzIvig917Ofvy7
├── Transcript Documents (auto-created)
└── Tagged with transcript_id in appProperties
```

## Notes

- Transcripts are marked as "processed" in Google Drive metadata to prevent reanalysis
- System automatically detects existing documents to avoid duplicates
- Meeting duration calculated from first to last sentence timestamps
- Meetings under 10 minutes marked as "Not Conducted"

## Troubleshooting

**Authentication Issues**: Delete the token file and re-authenticate

**API Quota Exceeded**: Reduce batch sizes or add more sleep intervals

**Missing Transcripts**: Check Fireflies API permissions and date ranges

**Analysis Failures**: Verify Gemini API key and prompt template format

## License

Internal use only - Requires valid API credentials and permissions.
