import sys
import json
import pandas as pd
from gcsfs import GCSFileSystem
from itertools import zip_longest
from google.oauth2 import service_account
from datetime import datetime, timedelta
from Etl_utils import etl_silver_uploader, etl_processed_files_pipeline, etl_uploader

LANDING = 'gs://databricks-1426426786865093/1426426786865093/data-lakes/airflow-pipeline/volix-dominos/ifood/scraping_landing/'
BRONZE = 'gs://databricks-1426426786865093/1426426786865093/data-lakes/airflow-pipeline/volix-dominos/ifood/bronze/'
SILVER = 'gs://databricks-1426426786865093/1426426786865093/data-lakes/airflow-pipeline/volix-dominos/ifood/silver/'
ERRORS = 'gs://databricks-1426426786865093/1426426786865093/data-lakes/airflow-pipeline/volix-dominos/ifood/scraping_errors/'

CREDENTIALS = r'C:\Users\igor_\Documents\mycodes\mycodes\scrapings\ifood\credentials.json'

storage_options = {
    'project': 'dw-volix',
    'bucket': 'databricks-1426426786865093',
    'token': CREDENTIALS
    }

destination_table_catalog = 'dominos_silver.scrapings_ifood_catalog'
destination_table_details = 'dominos_silver.scrapings_ifood_details'

scraping_date = (datetime.now()).strftime('%Y-%m-%d')
credentials = service_account.Credentials.from_service_account_file(storage_options['token'])
GcsFileExplorer = GCSFileSystem(project = storage_options['project'], token = storage_options['token'])

files = GcsFileExplorer.glob(LANDING + '*.json')


def etl_catalog_files(GcsClient, json_file_path, store_id):

    with GcsClient.open(json_file_path, 'r') as file:
        data = json.load(file)

    df_menus = pd.json_normalize(data['data']['menu'])

    items_list = []
    for menu in data['data']['menu']:
        for item in menu['itens']:
            item['menu_code'] = menu['code']
            items_list.append(item)

    df_items = pd.DataFrame(items_list)

    choices_list = []
    garnish_items_list = []
    for item in items_list:
        for choice in item.get('choices', []):
            choice['item_code'] = item['code']
            choices_list.append(choice)

            for garnish_item in choice.get('garnishItens', []):
                garnish_item['choice_code'] = choice['code']
                garnish_item['item_code'] = item['code']
                garnish_items_list.append(garnish_item)

    df_choices = pd.DataFrame(choices_list)
    df_garnish_items = pd.DataFrame(garnish_items_list)

    df_items = df_items.add_prefix('item_')
    df_choices = df_choices.add_prefix('choice_')
    df_garnish_items = df_garnish_items.add_prefix('garnish_')

    merged_df = pd.merge(df_items, df_menus, left_on='item_menu_code',
                            right_on='code', suffixes=('_item', '_menu'))

    try:
        merged_df = pd.merge(merged_df, df_choices, left_on='item_code',
                                right_on='choice_item_code', suffixes=('_menu_item', '_choice'))
        
        merged_df = pd.merge(merged_df, df_garnish_items, left_on='choice_code',
                                right_on='garnish_choice_code', suffixes=('_choice', '_garnish'))
    except Exception as e:
        print(e)

    merged_df = merged_df[[
        'item_id', 'item_code', 'item_description', 'item_details', 'item_unitPrice', 'item_unitMinPrice', 'item_menu_code',
        'code', 'name', 'choice_code', 'choice_name', 'choice_min', 'choice_max', 'choice_item_code',
        'garnish_id', 'garnish_code', 'garnish_description', 'garnish_unitPrice', 'garnish_choice_code',
        'garnish_item_code'
        ]].copy()

    merged_df.insert(0, 'store_id', store_id)

    merged_df['updated_at'] = pd.to_datetime(scraping_date)

    return merged_df



def etl_details_files_ifood(GcsClient, json_file_path):

    with GcsClient.open(json_file_path, 'r') as file:
        data = json.load(file)

    df_details = pd.json_normalize(data['data']['merchantExtra'])

    df_details.columns = [x.replace('.', '_') for x in df_details.columns]

    df_details = df_details[[
        'description', 'id', 'locale', 'name', 'address_city', 'address_country', 'address_district',
        'address_state', 'address_streetName', 'address_streetNumber', 'address_zipCode', 'documents_CNPJ_value'
        ]].copy()
    
    df_details.rename(columns={'id': 'store_id'}, inplace=True)

    return df_details


if files:

    print(f"\n||Files found for Ifood\n||Total files: {len(files)}\n")

    files_processed, files_errors = [], []
    catalog_files, details_files = [], []
    df_silver_catalog, df_silver_details = pd.DataFrame(), pd.DataFrame()

    for file in files:

        if 'catalog' in file:
            try:
                catalog_files.append(file)
                store_id = file.split('_')[-2]
                df_catalog = etl_catalog_files(GcsFileExplorer, file, store_id)
                df_silver_catalog = pd.concat([df_silver_catalog, df_catalog], ignore_index=True)
                files_processed.append(file)
            except Exception as e:
                print(f"||--> Error processing catalog file {file}: {e}")
                files_errors.append(file)

        if 'details' in file:
            try:
                details_files.append(file)
                df_details = etl_details_files_ifood(GcsFileExplorer, file)
                df_silver_details = pd.concat([df_silver_details, df_details], ignore_index=True)
                files_processed.append(file)
            except Exception as e:
                print(f"||--> Error processing details file {file}: {e}")
                files_errors.append(file)

    print(f"||Total catalog files processed: {len(catalog_files)}\n||Total details files processed: {len(details_files)}\n")

    etl_uploader(GcsFileExplorer, df_silver_catalog, 'Silver', destination_table_catalog, scraping_date, SILVER, credentials, storage_options)

    etl_uploader(GcsFileExplorer, df_silver_details, 'Silver', destination_table_details, scraping_date, SILVER, credentials, storage_options)

    etl_processed_files_pipeline(GcsFileExplorer, LANDING, BRONZE, ERRORS, files_processed, files_errors, scraping_date)

