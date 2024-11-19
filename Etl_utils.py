import pandas as pd
from datetime import datetime, timedelta
from google.oauth2 import service_account
from urllib.parse import urlparse
from urllib.parse import parse_qs
from itertools import zip_longest
from gcsfs import GCSFileSystem
from google.cloud import storage
from dataclasses import dataclass
import re
import os
import json
import pandas_gbq

@dataclass
class EtlPipeline:
    
    client_name: str = None
    run: str = None
    
    landing_folder: str = None
    errors_folder: str = None
    bronze_folder: str = None
    silver_folder: str = None
    gold_folder: str = None
    gold_ref_folder: str = None 
    credentials_folder: str = None
    machine = os.getenv("Machine")
    reprocess = os.getenv("ETL_Reprocess")
    
    sellers: bool = False
    products: bool = False
    prices: bool = False
    
    data_layer: str = None
    
    table_name_sellers: str = None
    table_name_products: str = None
    table_name_prices: str = None
    
    # def set_tables(self):
        
    #     self.table_name_sellers = f"{self.data_layer}_{self.client_name}_vendedores"
    #     self.table_name_products = f"{self.data_layer}_{self.client_name}_produtos"
    #     self.table_name_prices = f"{self.data_layer}_{self.client_name}_precos"
        
    # def __post_init__(self):
    #     self.scraping_date = (datetime.now() - timedelta(hours = 3)).strftime('%Y-%m-%d-%H')
        
    #     self._set_client_credentials_
        
    # def _set_client_credentials_(self):
        
    #     self.credentials = service_account.Credentials.from_service_account_file(self.credentials_path)
    #     self.GcsFileExplorer = GCSFileSystem(project='volix-msd', token=self.credentials_path)
        
    # def _set_files_report(self):
        
    #     self.seller_files = self.GcsFileExplorer.glob(self.landing_path + f'scraping_{self.client_name}_vendedores_*.parquet')
    #     self.product_files = self.GcsFileExplorer.glob(self.landing_path + f'scraping_{self.client_name}_produtos_*.parquet')
    #     self.price_files = self.GcsFileExplorer.glob(self.landing_path + f'scraping_{self.client_name}_precos_*.parquet')

    def set_etl_to_run(self, etl_to_run):
        """
        For very specific cases, we can set the etl_to_run to execute the same ETL file considering it would be called multiple times, 
        but for different data, and even in a different process/pipeline.
        
        Usage example in the ETLs on the current form:
        
        On the main/caller file that will call the ETL file explicitly:
        
            import etl_silver_client        - Import the ETL file main function
            etl_silver_client("Sellers")    - Call for the specific ETL to run inside the ETL file
        
        On ETL: 
            import EtlPipeline          - Import this class
            etl_silver_client(run_etl): - Enclose the whole flow of the ETL, as a "main"
            Etl = EtlPipeline()         - Call this class
            Etl.set_etl_to_run(run_etl) - Pass to this method the etl_to_run coming from the ETL main
            [...] if seller_files and Etl.sellers: [...] - Add to all internal ETLs the check by attribute/data name, on the specific 
                                                            attribute that fits each internal ETLs
        """
        match etl_to_run.capitalize():
            
            case 'Sellers':
                self.sellers = True
                
            case 'Products':
                self.products = True
                
            case 'Prices':
                self.prices = True
                
    def unset_etl_to_run(self):
        
        self.sellers = False
        self.products = False
        self.prices = False
        
    def get_credentials(self):
        
        if not self.credentials:
            
            self.credentials = json.loads(os.getenv("Credentials"))
            
            return self.credentials
        
        else:
            
            return self.credentials

#if didnt find anything inside the bucket or BG table, run anyway
def etl_pipeline_opening(bq_client, table_name, max_runs, reprocess = False):
    
    print(f"||ETL Pipeline Opening\n")
    start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    current_date = start_time.split(' ')[0]
    
    print(f"||Checking if ETL has already been performed today\n")
    
    query = f"SELECT `ETL Silver Runs Performed` FROM `{table_name}` WHERE DATE(`ETL Silver Runs Performed`) = '{current_date}' ORDER BY `ETL Silver Runs Performed` DESC"
    query_job = bq_client.query(query)
    rows = query_job.result()
    
    if rows.total_rows < max_runs:
        
        return True, start_time
    
    elif rows.total_rows >= max_runs and reprocess:
        
        print(f"||ETL process has already been performed max {max_runs} times today and {rows.total_rows} extra reprocesses.\n||ETL will now be reprocessed again\n")
        
        return True, start_time
    
    elif rows.total_rows >= max_runs and not reprocess:
        
        print(f"||ETL process has already been performed max {max_runs} times today\n")
        
        if rows.total_rows - max_runs > 0:
            
            print(f"||Extra reprocesses: {rows.total_rows - max_runs} last runs\n")
            
            for row in rows[:rows.total_rows - max_runs]:
                
                print(f"||Reprocessed at {row['ETL Silver Runs Performed']}")
                
        return False, start_time


def get_most_recent_gcs_dir(storage_client, bucket_name, folder, search):
    
    folder = folder[5:].split('/', 1)[1]
    
    bucket = storage_client.get_bucket(bucket_name)
    
    blobs = bucket.list_blobs(prefix = folder)
    
    dirs = set()
    
    for blob in blobs:
        
        folder_path = '/'.join(blob.name.split('/')[:-1])
        dirs.add(folder_path)
        
    full_path_length = 10
    
    dirs_with_dates = [dir for dir in dirs if len(dir.split('/')) == full_path_length]
    
    if search == 'collection':
        
        dirs_with_dates = [dir for dir in dirs_with_dates if 'processed_history' not in dir]
        
    elif search == 'processed':
        
        dirs_with_dates = [dir for dir in dirs_with_dates if 'collections_history' not in dir]
    
    most_recent_dir = max(dirs_with_dates, key = lambda dir: datetime.strptime('/'.join(dir.rsplit('/', 3)[-3:]), "%Y/%m/%d"))
    most_recent_dir =  'collections_history/' + '/'.join(most_recent_dir.rstrip('/').rsplit('/', 3)[-3:]) + '/'
    
    return most_recent_dir


def etl_silver_uploader(silver_table, table_name, scraping_date, layer_folder, credentials, storage_options):
    
    print(f"\n||ETL Silver Uploader\n")
    
    get_table_specs(silver_table, table_name)
    
    default_cols = ['produto', 'link', 'vendedor', 'preco', 'fonte', 'data']
    missing_cols = set(default_cols) - set(silver_table.columns)
    
    if not missing_cols:
        
        print("||All columns are present on the Silver Table.")
        print(table_name)
        pandas_gbq.to_gbq(silver_table, table_name, if_exists='append', credentials=credentials)
        silver_table.to_parquet(layer_folder + f'{table_name}_{scraping_date}.parquet', storage_options = storage_options)
        
        print(f"||Table Silver {table_name} uploaded to BQ and History")
        
    else:
        
        print("||Some columns are absent from the Silver Table:")
        
        for col in missing_cols:
            
            print(f"||    - {col}")
            
        print(f"||Failed to upload Table Silver {table_name} to BQ")


def etl_uploader(GcsClient, table, table_type, table_name, scraping_date, layer_folder, credentials, storage_options, clean_temp = False):
    
    print(f"\n||ETL {table_type.capitalize()} Uploader\n")
    
    get_table_specs(table, table_name)
    
    formatted_scraping_date = '/'.join(scraping_date.split('-')[0:3])
    
    collected_folder = 'collections_history/' + formatted_scraping_date + '/'
    
    reference_temp_folder = 'reference/temp/'
    
    missing_cols = False
    
    if table.empty:
        
        print(f"||Table {table_type.capitalize()} \"|{table_name}|\" is empty")
        
        return
    
    if table_type.lower() == 'silver':
        if 'msd' in table_name:
        
            default_cols = ['produto', 'link', 'vendedor', 'preco', 'fonte', 'data']
            
            missing_cols = set(default_cols) - set(table.columns)
        
    elif table_type.lower() == 'gold':
        
        pass
    
    if not missing_cols:
        
        print(f"||All columns are present on the {table_type.capitalize()} \"|{table_name}|\" Table.")
        
        match table_type.lower():
        
            case 'bronze':
                
                table.to_parquet(layer_folder + collected_folder + f'{table_name}_{scraping_date}.parquet', storage_options = storage_options)
                
                print(f"||Table Bronze {table_name} uploaded to GCS\n||Folder: {layer_folder + collected_folder + f'{table_name}_{scraping_date}.parquet'}")
                
            case 'silver':
                
                pandas_gbq.to_gbq(table, table_name, storage_options['project'], if_exists = 'append', credentials = credentials)
                
                table.to_parquet(layer_folder + collected_folder + f'{table_name}_{scraping_date}.parquet', storage_options = storage_options)
                
                print(f"||Table Silver {table_name} uploaded to BQ and GCS\n||Folder: {layer_folder + collected_folder + f'{table_name}_{scraping_date}.parquet'}")
                
            case 'gold':
                
                pandas_gbq.to_gbq(table, table_name, storage_options['project'],  if_exists = 'append', credentials = credentials)
                
                table.to_parquet(layer_folder + collected_folder + f'{table_name}_{scraping_date}.parquet', storage_options = storage_options)
                
                print(f"||Table Gold {table_name} updated in BQ and uploaded to GCS\n||Folder: {layer_folder + collected_folder + f'{table_name}_{scraping_date}.parquet'}")
                
            case 'temp_ref':
                
                if not clean_temp:
                    
                    table.to_parquet(layer_folder + reference_temp_folder + f'temp_ref_{table_name}.parquet', storage_options = storage_options)
                    
                    print(f"||Table Temp Reference {table_name} uploaded to GCS\n||Folder: {layer_folder + reference_temp_folder + f'temp_ref_{table_name}.parquet'}")
                    
                else:
                    
                    temp_table_file_path = layer_folder + reference_temp_folder + f'temp_ref_{table_name}.parquet'
                    
                    if GcsClient.exists(temp_table_file_path):
                        
                        GcsClient.rm(temp_table_file_path)
                        
                        print(f"||Table Temp Reference {table_name} deleted from GCS")
                        
                    else:
                        
                        print(f"||Table Temp Reference {table_name} does not exist in GCS")
                        
    else:
        
        print(f"||Some columns are absent from the {table_type.capitalize()} {table_name.capitalize()} Table:")
        
        for col in missing_cols:
            
            print(f"||    - {col}")
            
        print(f"||Failed to upload Table {table_type.capitalize()} {table_name} to BQ")
        
        
def etl_processed_files_pipeline(GcsFileExplorer, Landing, Processed, Errors, files_processed, files_errors, scraping_date):
    
    print("\n||ETL Pipeline Closing\n")
    
    formatted_scraping_date = '/'.join(scraping_date.split('-')[0:3])
    
    processed_folder = 'processed_history/' + formatted_scraping_date + '/'
    
    Concluded = Processed + processed_folder
    
    for done, error in zip_longest(files_processed, files_errors):
        
        for file, destiny in zip([done, error], [Concluded, Errors]):
            
            try:
                
                file_name = file.split('/')[-1]
                
                GcsFileExplorer.mv(Landing + file_name, destiny + file_name)
                
                print(f"||Moved {Landing + file_name} to folder {destiny + file_name}")
                
            except:
                
                pass


def etl_pipeline_closing(start_time, table_name, layer_etl, credentials, scraping_date, reprocess = False):
    
    print(f"||ETL Pipeline Closing\n")
    
    final_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    print(f"||ETL {layer_etl.capitalize()} Process Completed at {final_time}. Took {round((final_time - start_time).total_seconds()/60, 4)} minutes to run")
    print(f"||Reprocess?: {reprocess}")
    print(f"||Registering ETL {layer_etl.capitalize()} Run\n")
    
    df_etl_run = pd.DataFrame({f'ETL {layer_etl.capitalize()} Runs Performed': [scraping_date], 'Force Reprocess': [reprocess]})
    
    pandas_gbq.to_gbq(df_etl_run, table_name, if_exists='append', credentials = credentials)
    
    print(f"||ETL {layer_etl.capitalize()} Run registered\n")


def extrai_data_arquivo(arquivo):
    try:
        return datetime.strptime(arquivo.split('_')[-1].split('.')[0],'%Y-%m-%d-%H')
    except:
        return datetime.strptime(arquivo.split('_')[-1].split('.')[0],'%Y-%m-%d')


def infos_seller(info_seller):
    try: 
        info_seller = [x.strip() for x in info_seller]

        try:
            razao_social = info_seller[info_seller.index('Nome Comercial:')+1]
        except:
            razao_social = None

        try:
            cnpj = info_seller[info_seller.index('CNPJ:')+1]
        except:
            cnpj = None

        try:
            cidade, estado, cep, pais = info_seller[info_seller.index('Endereço:')+1:][-4:]
        except:
            cidade, estado, cep, pais = None, None, None, None

        return {'razao_social': razao_social,
                'cnpj': cnpj,
                'cidade': cidade,
                'estado': estado,
                'cep': cep,
                'pais': pais
                }
    except:
        return {'razao_social': None,
                'cnpj': None,
                'cidade': None,
                'estado': None,
                'cep': None,
                'pais': None
                }

def gera_permalink(url):
    try:

        parsed_url = urlparse(url)
        seller_id = parse_qs(parsed_url.query)['seller'][0]
        cod_produto = parse_qs(parsed_url.query)['asin'][0]

        return f'https://www.amazon.com.br/dp/{cod_produto}/?m={seller_id}'

    except:
        return None


def get_table_specs(df, kind):
    
    print("||Get DF Specs")
    try:
        
        print(f"\n||DF {kind} type: {type(df)}")
        if isinstance(df, pd.DataFrame):
            
            print(f"||DF Info:")
            df.info()
    except Exception as error:
        print(f"||Error checking DF {kind} specs:\n{error}\n")
        
    return

def volix_logo():
    
    volix_logo = '''
    X================================================================X
    ||&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&||
    ||&&(//////)&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&(//////)&&||
    ||&&(/////////////)&&&&&&&&&&&&&&&&&&&&&&&&&&&&(/////////////)&&||
    ||&&(/////////////////)&&&&&&&&&&&&&&&&&&&&(/////////////////)&&||
    ||&&(////////////////////)&&&&&&&&&&&&&&(////////////////////)&&||
    ||&&&&&&(//////////////////)&&&&&&&&&&(//////////////////)&&&&&&||
    ||&&&&&&&&&&&(///////////////)&&&&&&&(///////////////)&&&&&&&&&&||
    ||&&&&&&&&&&&&&&&(//////////////)&(//////////////)&&&&&&&&&&&&&&||
    ||&&&&&&&&&&&&&&&&&(/////////////^/////////////)&&&&&&&&&&&&&&&&||
    ||&&&&&&&&&&&&&&&&&&(////////////////////////)&&&&&&&&&&&&&&&&&&||
    ||&&&&&&&&&&&&&&&&&&&(//////////////////////)&&&&&&&&&&&&&&&&&&&||
    ||&&&&&&&&&&&&&&&&&&&&(////////////////////)&&&&&&&&&&&&&&&&&&&&||
    ||&&&&&&&&&&&&&&&&&&&&&(//////////////////)&&&&&&&&&&&&&&&&&&&&&||
    ||&&&&&&&&&&&&&&&&&&&&&&(////////////////)&&&&&&&&&&&&&&&&&&&&&&||
    ||&&&&&&&&&&&&&&&&&&&&&(//////////////////)&&&&&&&&&&&&&&&&&&&&&||
    ||&&&&&&&&&&&&&&&&&&&(//////////////////////)&&&&&&&&&&&&&&&&&&&||
    ||&&&&&&&&&&&&&&&&&&(////////////////////////)&&&&&&&&&&&&&&&&&&||
    ||&&&&&&&&&&&&&&&&&(/////////////^/////////////)&&&&&&&&&&&&&&&&||
    ||&&&&&&&&&&&&&&&(//////////////)&(//////////////)&&&&&&&&&&&&&&||
    ||&&&&&&&&&&&(///////////////)&&&&&&&(///////////////)&&&&&&&&&&||
    ||&&&&&&(//////////////////)&&&&&&&&&&(//////////////////)&&&&&&||
    ||&&(////////////////////)&&&&&&&&&&&&&&(////////////////////)&&||
    ||&&(/////////////////)&&&&&&&&&&&&&&&&&&&&(/////////////////)&&||
    ||&&(/////////////)&&&&&&&&&&&&&&&&&&&&&&&&&&&&(/////////////)&&||
    ||&&(//////)&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&(//////)&&||
    ||&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&||
    X================================================================X
    '''
    print(volix_logo)
