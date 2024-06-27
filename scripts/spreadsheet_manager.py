import json
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe
from airflow.models import Variable

SERVICE_ACCOUNT_FILE = Variable.get("SERVICE_ACCOUNT_FILE", deserialize_json=True)
SCOPES = ['https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive']

def get_client():
    creds = Credentials.from_service_account_info(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    return gspread.authorize(creds)

def dataframe_to_spreadsheet(metadata):
    client = get_client()

    spreadsheet = client.create(metadata['spreadsheet_name'])

    spreadsheet.share(None, perm_type='anyone', role='reader')

    for sheet_data in metadata['sheets']:
        worksheet = spreadsheet.add_worksheet(title=sheet_data['sheet_name'], rows="1", cols="1")
        set_with_dataframe(worksheet, sheet_data['data'])

    first_worksheet = spreadsheet.get_worksheet(0)
    spreadsheet.del_worksheet(first_worksheet)

    print(f'Translation saved in: {spreadsheet.url}')
