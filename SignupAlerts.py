from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.service_account import Credentials
import pandas as pd
import gspread
import time
from datetime import datetime
import requests
import pytz
import os
import re

# ---------- Pandas display (optional) ----------
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', 50)

# ---------- Discord config ----------
# Prefer environment variables; fallback to hardcoded ONLY if you must.
BOT_TOKEN = ''
CHANNEL_ID = '1418330829595217992'  # Make sure this is the correct channel ID
# You can optionally hardcode as a last resort:
# BOT_TOKEN = BOT_TOKEN or "YOUR_BOT_TOKEN"
# CHANNEL_ID = CHANNEL_ID or "YOUR_CHANNEL_ID"

def send_message(token, channel_id, content):
    if not token or not channel_id:
        print("Discord token or channel_id missing. Skipping message send.")
        return
    url = f"https://discord.com/api/v10/channels/{channel_id}/messages"
    headers = {
        "Authorization": f"Bot {token}",
        "Content-Type": "application/json"
    }
    payload = {"content": content}
    try:
        response = requests.post(url, json=payload, headers=headers, timeout=15)
        if response.status_code in (200, 201):
            print("Message sent successfully!")
        else:
            print(f"Failed to send message: {response.status_code}")
            try:
                print(f"Error details: {response.json()}")
            except Exception:
                print("No JSON error body.")
            print("Please verify:")
            print("1. Channel ID is correct")
            print("2. Bot token is valid")
            print("3. Bot has access to the channel")
            print("4. Bot has message sending permissions")
    except Exception as e:
        print(f"Error sending message: {str(e)}")

# ---------- Google APIs config ----------
SCOPES = [
    'https://www.googleapis.com/auth/drive.metadata.readonly',
    'https://www.googleapis.com/auth/spreadsheets.readonly'
]
SERVICE_ACCOUNT_FILE = 'service_account.json'  # path to your service account JSON

def get_latest_responses_file(items):
    """Fallback: choose latest by createdTime among files containing 'Responses'."""
    response_files = [item for item in items if 'Responses' in item.get('name', '')]
    if not response_files:
        print('No files with "Responses" found in the folder.')
        return None
    # Sort by createdTime
    latest_file = max(response_files, key=lambda x: x['createdTime'])
    return latest_file

def list_folder_contents(folder_id):
    try:
        creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        service = build('drive', 'v3', credentials=creds)
        query = f"'{folder_id}' in parents and trashed = false"
        results = service.files().list(
            q=query,
            pageSize=1000,
            fields="files(id, name, createdTime)"
        ).execute()
        items = results.get('files', [])
        if not items:
            print('No files found in the folder.')
        else:
            print('Files in the folder:')
            for item in items:
                print(f"{item['name']} ({item['id']})")
        return items
    except HttpError as error:
        print(f'An error occurred: {error}')
        return []

def get_spreadsheet_data(file_id):
    try:
        creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(file_id)
        worksheet = spreadsheet.sheet1
        data = worksheet.get_all_records()
        df = pd.DataFrame(data)

        # Remove fully blank rows
        if not df.empty:
            df.dropna(how='all', inplace=True)

        # Remove rows containing specific strings in any column
        keywords = ["Sunday Guest", "Sunday Renewal", "Saturday Guest", "Saturday Renewal"]
        if not df.empty:
            df = df[~df.apply(lambda row: row.astype(str).str.contains('|'.join(keywords), case=False, na=False).any(), axis=1)]

        return df.reset_index(drop=True)
    except Exception as error:
        print(f'An error occurred while retrieving the spreadsheet: {error}')
        return pd.DataFrame()

def get_sunday_column(df):
    """Find the Sunday sign-up column even if wording changes slightly."""
    if df is None or df.empty:
        return None
    lower_cols = {col.lower(): col for col in df.columns}
    candidates = [orig for low, orig in lower_cols.items() if "are you playing sunday" in low]
    return candidates[0] if candidates else None

def get_sunday_count(df):
    col = get_sunday_column(df)
    if col and col in df.columns and not df.empty:
        return df[col].astype(str).str.lower().str.contains(r'\by(es)?\b').sum()
    return 0

def format_discord_message(df, message_type, content, timestamp=None):
    """Fixed: pass df into this function so we don't rely on undefined globals."""
    est = pytz.timezone('US/Eastern')
    # Normalize timestamp
    if timestamp is None:
        timestamp = datetime.utcnow()
    if timestamp.tzinfo is None:
        timestamp = pytz.utc.localize(timestamp)
    est_time = timestamp.astimezone(est)
    time_str = est_time.strftime('%I:%M %p EST')
    sunday_count = get_sunday_count(df)
    #return f"\n---------------\n**Update at {time_str}**\n**Current Sunday Signups: {sunday_count}**\n\n{content}\n---------------"
    return f"\n**Update at {time_str}**\n**Current Sunday Signups: {sunday_count}**\n\n{content}\n---------------"

def compare_dataframes(old_df, new_df):
    """
    Compare row-by-row (positional) and send updates to Discord.
    If rows can be reordered or deleted, consider switching to a key-based diff.
    """
    messages_to_send = []
    current_time = datetime.utcnow()

    # Ensure old_df has same columns to avoid spurious diffs
    if old_df is None or old_df.empty:
        old_df = pd.DataFrame(columns=new_df.columns)

    for idx in range(len(new_df)):
        new_row = new_df.iloc[idx]
        # create an empty aligned series if old row missing
        if idx < len(old_df):
            old_row = old_df.iloc[idx].reindex(new_row.index)
        else:
            old_row = pd.Series(index=new_row.index, dtype=object)

        # If identical, continue
        if old_row.equals(new_row):
            continue

        full_name = new_row.get("What's your name? (first & last)", 'Unknown Person')
        if pd.isna(full_name) or str(full_name).strip() == '':
            continue

        update_details = [f"**{full_name}**"]
        for col in new_row.index:
            old_val = old_row.get(col, pd.NA)
            new_val = new_row[col]

            old_is_nan = pd.isna(old_val)
            new_is_nan = pd.isna(new_val)
            if old_is_nan and new_is_nan:
                continue

            # changed if one is NaN and other isn't, or values differ
            if (old_is_nan != new_is_nan) or (not old_is_nan and not new_is_nan and old_val != new_val):
                old_str = "" if old_is_nan else str(old_val)
                new_str = "" if new_is_nan else str(new_val)
                if old_str.strip() == "" and new_str.strip() == "":
                    continue
                update_details.append(f"{col}: {old_str} \u2192 {new_str}")  # nice arrow

        if len(update_details) > 1:
            msg = format_discord_message(new_df, "update", "\n".join(update_details), current_time)
            messages_to_send.append(msg)

    for message in messages_to_send:
        send_message(BOT_TOKEN, CHANNEL_ID, message)

def pick_latest_responses_file_id(items):
    """
    Your original logic: pick the file whose name contains 'Responses' and has the
    latest mm/dd in the name. If that fails, fallback to createdTime.
    """
    if not items:
        return None

    df = pd.DataFrame(items)
    # Filter 'Responses' files
    df = df[df['name'].str.contains('Responses', na=False)]

    if df.empty:
        # fallback
        latest = get_latest_responses_file(items)
        return latest['id'] if latest else None

    # Extract mm/dd from name and parse with current year as default
    # Example expected pattern: "... 9/18 ..."
    date_str = df['name'].str.extract(r'(\d{1,2}/\d{1,2})', expand=False)
    # Parse with current year assumption
    this_year = datetime.now().year
    parsed_dates = []
    for s in date_str.fillna("1/1"):
        try:
            m, d = map(int, s.split('/'))
            parsed_dates.append(datetime(this_year, m, d))
        except Exception:
            parsed_dates.append(datetime(this_year, 1, 1))
    df = df.assign(parsed_date=parsed_dates)
    latest_row = df.loc[df['parsed_date'].idxmax()]
    return latest_row['id']

if __name__ == '__main__':
    # -------- USER: set this folder_id --------
    folder_id = '1QrLMuE-TA6caaaXoozkqdTuv81CIavO-'  # Replace with your folder ID

    # -------- Initial load --------
    filelist = list_folder_contents(folder_id)
    latest_file_id = pick_latest_responses_file_id(filelist)
    if not latest_file_id:
        raise SystemExit("Could not locate a 'Responses' spreadsheet in the folder.")

    previous_df = get_spreadsheet_data(latest_file_id)
    print(f"Initial data loaded at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Current number of entries: {len(previous_df)}")

    # -------- Continuous monitoring loop --------
    while True:
        try:
            time.sleep(5)  # adjust as needed
            print(f"\nChecking for updates at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}...")

            current_df = get_spreadsheet_data(latest_file_id)

            # Compare with previous and send any messages
            compare_dataframes(previous_df, current_df)

            # Update snapshot
            previous_df = current_df

        except Exception as e:
            print(f"Error during update check: {e}")
            continue
