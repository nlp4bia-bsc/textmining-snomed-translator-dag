from urllib.error import URLError
from datetime import datetime
import pandas as pd
import os

from snomed_automatic_translation.scripts.spreadsheet_manager import dataframe_to_spreadsheet
from snomed_automatic_translation.scripts.snomed_api import getConceptById

BASE_PATH = '/opt/airflow/storage/translations'

AVOID_ID = ['Ø', 'nan']

api_languages = [
    {
        "language": "en",
        "edition": "MAIN",
        "version": "2024-06-01",
    },
    {
        "language": "es",
        "edition": "MAIN/SNOMEDCT-ES",
        "version": "2024-03-31",
    },
    {
        "language": "nl",
        "edition": "MAIN/SNOMEDCT-NL",
        "version": "2024-03-31",
    },
    {
        "language": "sv",
        "edition": "MAIN/SNOMEDCT-SE",
        "version": "2024-05-31",
    },
]

default_args = {
    "spreadsheet_id": "1GM17jnZop0eHSYaWKccVhp4pdbX58IEuSLXsiYg4GUQ",
    "sheet_id": "0",
    "column": "SNOMEDID NUMBER",
    "api_languages": api_languages
}

def get_public_spreadsheet(spreadsheet_id, sheet_id=0):
    spreadsheet_url = f'https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?gid={sheet_id}&format=csv'
    print(f'Spreadsheet URL: {spreadsheet_url}')
    df = pd.read_csv(spreadsheet_url)
    print(f'Spreadsheet columns: {list(df.columns)}')
    return df

def get_snomedid_list(df, column):
    
    snomed_list = list(df[column])

    # Handler of typo in list
    snomed_processed = []

    for snomed_id in snomed_list:
        lines = [line.strip() for line in str(snomed_id).splitlines() if line.strip()]
        snomed_processed.extend(lines)

    snomed_processed = list(set(snomed_processed))

    print(f'Spreadsheet SNOMED IDs: {snomed_processed}')
    return snomed_processed

def bulk_translate(snomed_list, api_languages):
    total_concepts = len(snomed_list)

    snomed_no_processed = snomed_list[:]
    concepts_dict = {}
    synonyms_dict = {}
    for index, concept in enumerate(snomed_list):
        try:
            print(f'Concept {index+1}/{total_concepts}: {concept}')
            if concept not in AVOID_ID:
                concepts_dict[concept] = {}
                synonyms_dict[concept] = {}
                for language in api_languages:
                    concepts_dict[concept][language["language"]], synonyms_dict[concept][language["language"]] = getConceptById(
                        concept,
                        language["language"],
                        language["edition"],
                        language["version"]
                    )
                print()
            snomed_no_processed.pop(0)
        except URLError as e:
            print('Our IP was blocked from SNOMED')
            print(e)
            break
        except Exception as e:
            print('There is an error to get the translation')
            print(e)

    print(f'SNOMED IDs no processed: {snomed_no_processed}')
    return concepts_dict, synonyms_dict, snomed_no_processed

def create_synonym_dataframe(data, language):
    rows = []
    for main_id, translations in data.items():
        if language in translations:
            concept = translations[language].get('concept', None)
            for entry in translations[language].get('synonyms', []):
                rows.append({
                    'SNOMED_ID': main_id,
                    'Definition': concept,
                    'Description ID': entry['descriptionId'],
                    'Synonym': entry['synonym']
                })
    return pd.DataFrame(rows)


def save_snomed_dataframe(concepts_dict, synonyms_dict, no_processed, languages):
    df_processed = pd.DataFrame.from_dict(concepts_dict, orient='index')
    df_processed = df_processed.rename_axis('SNOMED_ID').reset_index()

    df_no_processed = pd.DataFrame(no_processed, columns=['SNOMED_ID'])

    now = datetime.now()
    current_time = now.strftime("%Y-%m-%d %H:%M:%S")

    metadata = {
        'spreadsheet_name': f'SNOMED_TRANSLATION_{current_time}',
        'sheets': [
            {
                'sheet_name': 'SNOMED TRANSLATION',
                'data': df_processed,
            },
            {
                'sheet_name': 'SNOMED NO_PROCESSED',
                'data': df_no_processed,
            },
        ]
    }

    for language in languages:
        metadata['sheets'].append({
            'sheet_name': f'{language["language"].upper()}_SYNONYMS',
            'data': create_synonym_dataframe(synonyms_dict, language["language"]),
        })

    dataframe_to_spreadsheet(metadata)

def translate_snomedid(**kwargs):
    config_dict = kwargs['dag_run'].conf if kwargs['dag_run'].conf != {} else default_args
    spreadsheet_id = config_dict.get('spreadsheet_id')
    sheet_id = config_dict.get('sheet_id')
    column = config_dict.get('column')
    languages = config_dict.get('api_languages', api_languages)

    df = get_public_spreadsheet(spreadsheet_id, sheet_id)
    snomed_list = get_snomedid_list(df, column)

    concepts_dict, synonyms_dict, snomed_no_processed = bulk_translate(snomed_list, languages)
    save_snomed_dataframe(concepts_dict, synonyms_dict, snomed_no_processed, languages)
