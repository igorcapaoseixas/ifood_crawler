import json
from store_ids import store_ids
from datetime import datetime, timedelta
from ifood_crawler import HttpClient, IfoodCrawler
from json_writer import GCSStorageWriter, JsonStorageWriter
from google.oauth2 import service_account
from Scraping_utils import scraping_bronze_uploader_json


def main():

    access_key = '69f181d5-0046-4221-b7b2-deef62bd60d5'
    secret_key = '9ef4fb4f-7a1d-4e0d-a9b1-9b82873297d8'
    user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.54 Safari/537.36'
    proxies = {'http': f'http://brd-customer-hl_4ba48506-zone-data_center:zkus3rbg4d1v@brd.superproxy.io:22225'}
    landing_folder = 'gs://databricks-1426426786865093/1426426786865093/data-lakes/airflow-pipeline/volix-dominos/ifood/scraping_landing'
    scraping_date = (datetime.now() - timedelta(hours = 3)).strftime('%Y-%m-%d')
    credentials = r'C:\Users\igor_\Documents\mycodes\mycodes\scrapings\ifood\credentials.json'

    credentials_bq = service_account.Credentials.from_service_account_file(credentials)

    http_client = HttpClient(user_agent=user_agent, proxies=proxies)

    crawler = IfoodCrawler(access_key=access_key, secret_key=secret_key, http_client=http_client, credentials=credentials_bq)

    gcs_writer = GCSStorageWriter(storage_path=landing_folder, project_id='dw-volix', credentials=credentials)

    json_writer = JsonStorageWriter(storage_writer=gcs_writer)

    stores = store_ids()
    stores = json.dumps(stores)
    stores = json.loads(stores)

    for store, states in stores['Ifood'].items():
        for state, store_id in states.items():
            print(f"Store: {store}, State: {state}")

            catalog = crawler.search_store_catalog(store_id=store_id)

            if catalog:
                catalog_file_path = f'{landing_folder}/{store}_{state}_catalog_{store_id}_{scraping_date}.json'
                json_writer.write_json_to_storage(file_path=catalog_file_path, json_data=catalog)

            details = crawler.search_store_details(store_id=store_id)

            if details:
                details_file_path = f'{landing_folder}/{store}_{state}_details_{scraping_date}.json'
                json_writer.write_json_to_storage(file_path=details_file_path, json_data=details)


if __name__ == "__main__":
    main()

