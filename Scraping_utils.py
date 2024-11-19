import pandas as pd
import json
import os
from time import sleep as wait
from datetime import datetime, timedelta
import argparse


def startup_cli():
    
    cli_call = argparse.ArgumentParser(description = 'Scraping Bronze Uploader')
    
    cli_call.add_argument('--table_name', type = str, required = True, help = 'Table name to be uploaded to GCS')
    cli_call.add_argument('--scraping_date', type = str, required = True, help = 'Scraping date to be used in the table name')
    cli_call.add_argument('--landing_folder', type = str, required = True, help = 'GCS landing folder for the table')
    cli_call.add_argument('--storage_options', type = str, required = True, help = 'Storage options to be used in the upload')
    cli_call.add_argument('--folder_path', type = str, required = True, help = 'Local folder path to extract the JSONs')
    
    entry_args = cli_call.parse_args()
    
    entry_args.storage_options = json.loads(entry_args.storage_options)
    
    return entry_args


# class ScrapingExecutor
# cli, python function

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


def scrapy_run(spider):
    
    import scrapy.crawler as crawler
    from multiprocessing import Process, Queue
    from twisted.internet import reactor
        
    def runner(process_queue):
        
        try:
            
            scraping = crawler.CrawlerRunner()
            
            deferred = scraping.crawl(spider)
            
            deferred.addBoth(lambda result_or_failure: reactor.stop())
            
            reactor.run()
            
            process_queue.put(None)
            
        except Exception as error:
            
            process_queue.put(error)
            
    process_queue = Queue()
    scraping_process = Process(target = runner, args = (process_queue,))
    
    scraping_process.start()
    
    process_result = process_queue.get()
    
    scraping_process.join()
    
    if process_result is not None:
        raise process_result


def rename_columns_without_dots(column_name):
    
    if '.' in column_name:
        
        new_column_name = column_name.replace('.', '_')
        
        return new_column_name
    
    else:
        
        return column_name


def extract_json_to_table(folder_path, table_name):
    
    table_type = table_name.split('_')[-1]
    client_name = table_name.rsplit('_', 1)[0].split('.')[-1]
    
    prefix = f'scraping_{client_name}_{table_type}_'
    
    files = os.listdir(folder_path)
    
    json_files = [file for file in files if file.endswith('.json') and prefix in file]
    
    print(f"||Folder: {folder_path}||Prefix: {prefix}||Table Type: {table_type}||Client Name: {client_name}\n\
            ||Files and folders list:\n{files}")
    
    if not json_files:
        
        raise FileNotFoundError(f"||No JSON files found in the specified folder\n\
                                ||Folder: {folder_path}||Prefix: {prefix}||Table Type: {table_type}||Client Name: {client_name}\n\
                                ||Files and folders list:\n{files}")
        
    print(f"||Total JSON files found: {len(json_files)}.")
    
    json_data_list = []
    
    print(f"||Accumulating JSONs into Bronze Table")
    
    for json_file in json_files:
        
        print(f"||File: {json_file}")
        
        file_path = os.path.join(folder_path, json_file)
        
        with open(file_path, 'r') as json_file:
            
            try:
                
                json_data = json.load(json_file)
                
                for item in json_data:
                    
                    json_data_list.append(item)
                    
            except Exception as e:
                print(f"||Error processing JSON file {json_file}: {e}")
                
        try:
            
            if os.path.isfile(file_path):
                
                os.remove(file_path)
                
                print(f"||Deleted file: {file_path}")
                
        except Exception as e:
            
            print(f"||Error deleting file {file_path}: {e}")
            
            
    json_dataframe = pd.json_normalize(json_data_list)
    
    print(f"||Successfully joined {len(json_files)} non-empty JSON files into Bronze Table")
    
    return json_dataframe


def scraping_bronze_uploader(table_name, scraping_date, landing_folder, storage_options, bronze_table = None, folder_path = None):
    
    print(f"\n||Scraping Bronze Uploader\n")
    
    if bronze_table is None:
        
        bronze_table = extract_json_to_table(folder_path, table_name)
        
    get_table_specs(bronze_table, table_name)
    
    table_type = table_name.split('_')[-1]
    client_name = table_name.rsplit('_', 1)[0].split('.')[-1]
    
    print(f"||Succesfully loaded Table Bronze {table_type.capitalize()} \"|{table_name}|\"")
    
    if bronze_table.empty:
        
        print(f"||Table Bronze \"|{table_name}|\" is empty")
        
        return
    
    try:
        
        bronze_table.to_parquet(landing_folder + f'scraping_{client_name}_{table_type}_{scraping_date}.parquet', engine = 'pyarrow', index = False, compression = 'gzip', storage_options = storage_options)
        print(f"||Table Bronze {table_type.capitalize()} \"|{table_name}|\" uploaded to GCS||\n||Folder: {landing_folder + f'scraping_{client_name}_{table_type}_{scraping_date}.parquet'}")
        
    except Exception as error:
        
        print(f"||Failed to upload Table Bronze {table_type.capitalize()} \"|{table_name}|\" to GCS:\n{error}\n")
        
    finally:
        
        print(f'|| ↑ Scraping date: {scraping_date} ↑ ')
        
    return


def scraping_bronze_uploader_json(table_name, scraping_date, landing_folder, storage_options, bronze_table=None, folder_path = None):
    
    print(f"\n||Scraping Bronze Uploader Json\n")
    
    table_type = table_name.split('_')[-1]
    client_name = table_name.rsplit('_', 1)[0].split('.')[-1]
    
    print(f"||Succesfully loaded Table Bronze {table_type.capitalize()} \"|{table_name}|\"")
    
    if bronze_table.empty:
        
        print(f"||Table Bronze \"|{table_name}|\" is empty")
        
        return
    
    try:
        
        bronze_table.to_json(landing_folder + f'scraping_{client_name}_{table_type}_{scraping_date}.json', orient='records', lines=True, force_ascii=False, storage_options = storage_options)
        
        print(f"||Table Bronze {table_type.capitalize()} \"|{table_name}|\" uploaded to GCS||\n||Folder: {landing_folder + f'scraping_{client_name}_{table_type}_{scraping_date}.json'}")
        
    except Exception as error:
        
        print(f"||Failed to upload Table Bronze {table_type.capitalize()} \"|{table_name}|\" to GCS:\n{error}\n")
        
    finally:
        
        print(f'|| ↑ Scraping date: {scraping_date} ↑ ')
        
    return


if __name__ == '__main__':
    
    entry_args = startup_cli()
    
    scraping_bronze_uploader(entry_args.table_name, entry_args.scraping_date, entry_args.landing_folder, entry_args.storage_options,  bronze_table = None, folder_path = entry_args.folder_path)
