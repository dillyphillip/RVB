from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.service_account import Credentials
import pandas as pd
import gspread  # Add this import

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

if __name__ == '__main__':
    folder_id = '1QrLMuE-TA6caaaXoozkqdTuv81CIavO-'  # Replace with your folder ID
    filelist = list_folder_contents(folder_id)
    
    # Convert filelist to a pandas DataFrame
    df = pd.DataFrame(filelist)
    print("DataFrame representation of filelist:")
    print(df)

    # Filter the DataFrame to include only rows where 'name' contains 'Responses'
    filtered_df = df[df['name'].str.contains('Responses', na=False)]
    print("Filtered DataFrame with 'Responses' in name:")
    print(filtered_df)

    # Extract the row with the latest date in the name
    filtered_df['date'] = pd.to_datetime(filtered_df['name'].str.extract(r'(\d+/\d+)')[0], format='%m/%d', errors='coerce')
    latest_row = filtered_df.loc[filtered_df['date'].idxmax()]

    print("Row with the latest date in the name:")
    print(latest_row)

    # Extract the file ID of the latest row
    latest_file_id = latest_row['id']

    # Retrieve the spreadsheet data as a DataFrame
    spreadsheet_df = get_spreadsheet_data(latest_file_id)
    print("Cleaned Spreadsheet DataFrame:")
    print(spreadsheet_df)

