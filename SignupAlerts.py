from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.service_account import Credentials
import pandas as pd
import gspread  # Add this import
import time
from datetime import datetime

pd.set_option('display.max_columns', None)  # Show all columns in DataFrame output
pd.set_option('display.max_rows', None)     # Show all rows in DataFrame output
pd.set_option('display.width', 1000)        # Set display width for better readability
pd.set_option('display.max_colwidth', 50)  # No limit on column width

SCOPES = [
    'https://www.googleapis.com/auth/drive.metadata.readonly',
    'https://www.googleapis.com/auth/spreadsheets.readonly'  # Add this scope
]
SERVICE_ACCOUNT_FILE = 'service_account.json'  # Replace with your service account JSON file

def get_latest_responses_file(items):
    # Filter files with "Responses" in their name
    response_files = [item for item in items if 'Responses' in item['name']]
    if not response_files:
        print('No files with "Responses" found in the folder.')
        return None

    # Sort files by creation time (latest first)
    latest_file = max(response_files, key=lambda x: x['createdTime'])
    return latest_file

def list_folder_contents(folder_id):
    try:
        # Authenticate using the service account
        creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        service = build('drive', 'v3', credentials=creds)

        # Call the Drive API to list files in the specified folder
        query = f"'{folder_id}' in parents"
        results = service.files().list(
            q=query, pageSize=100, fields="files(id, name, createdTime)").execute()
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
        # Authenticate using the service account
        creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        client = gspread.authorize(creds)

        # Open the spreadsheet by file ID
        spreadsheet = client.open_by_key(file_id)

        # Get the first worksheet
        worksheet = spreadsheet.sheet1

        # Fetch all data as a list of lists
        data = worksheet.get_all_records()

        # Convert to a pandas DataFrame
        df = pd.DataFrame(data)

        # Remove blank rows
        df.dropna(how='all', inplace=True)

        # Remove rows containing specific strings in any column
        keywords = ["Sunday Guest", "Sunday Renewal", "Saturday Guest", "Saturday Renewal"]
        df = df[~df.apply(lambda row: row.astype(str).str.contains('|'.join(keywords)).any(), axis=1)]

        return df

    except Exception as error:
        print(f'An error occurred while retrieving the spreadsheet: {error}')
        return pd.DataFrame()

def format_person_details(row):
    details = []
    for column in row.index:
        if pd.notna(row[column]):
            details.append(f"{column}: {row[column]}")
    return " | ".join(details)

def compare_dataframes(old_df, new_df):
    # Check for new entries
    if len(old_df) < len(new_df):
        new_entries = new_df.iloc[len(old_df):]
        print("\n=== New Signups ===")
        for _, row in new_entries.iterrows():
            print(f"➕ {format_person_details(row)}")

    # Check for changed data
    common_length = min(len(old_df), len(new_df))
    for idx in range(common_length):
        old_row = old_df.iloc[idx]
        new_row = new_df.iloc[idx]
        
        if not old_row.equals(new_row):
            # Get the person's name from the combined name column
            full_name = new_row.get("What's your name? (first & last)", 'Unknown Person')
            print(f"\n=== Updated Entry for {full_name} ===")
            # Compare all columns
            for col in old_row.index:
                if old_row[col] != new_row[col]):
                    if pd.notna(old_row[col]) or pd.notna(new_row[col]):
                        old_val = old_row[col] if pd.notna(old_row[col]) else "N/A"
                        new_val = new_row[col] if pd.notna(new_row[col]) else "N/A"
                        print(f"{col}: {old_val} → {new_val}")

if __name__ == '__main__':
    folder_id = '1QrLMuE-TA6caaaXoozkqdTuv81CIavO-'  # Replace with your folder ID
    
    # Initial data fetch
    filelist = list_folder_contents(folder_id)
    df = pd.DataFrame(filelist)
    filtered_df = df[df['name'].str.contains('Responses', na=False)]
    filtered_df['date'] = pd.to_datetime(filtered_df['name'].str.extract(r'(\d+/\d+)')[0], format='%m/%d', errors='coerce')
    latest_row = filtered_df.loc[filtered_df['date'].idxmax()]
    latest_file_id = latest_row['id']
    
    previous_df = get_spreadsheet_data(latest_file_id)
    print(f"Initial data loaded at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Current number of entries: {len(previous_df)}")

    # Continuous monitoring loop
    while True:
        try:
            time.sleep(15)  # Wait for 60 seconds
            print(f"\nChecking for updates at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}...")
            
            # Fetch new data
            current_df = get_spreadsheet_data(latest_file_id)
            
            # Compare with previous data
            compare_dataframes(previous_df, current_df)
            
            # Update previous_df for next iteration
            previous_df = current_df
            
        except Exception as e:
            print(f"Error during update check: {e}")
            continue

