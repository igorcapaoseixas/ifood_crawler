import json
import gcsfs
from abc import ABC, abstractmethod

class StorageWriter(ABC):
    """
    Interface abstrata que define o contrato para classes de escrita de arquivos.
    Força a implementação de um método 'write', garantindo que subclasses sigam o mesmo padrão.
    """
    
    @abstractmethod
    def write(self, file_path, data):
        pass
    

class GCSStorageWriter(StorageWriter):
    """
    Implementação de StorageWriter para escrita de arquivos no Google Cloud Storage.
    Usa a biblioteca gcsfs para interagir com o bucket GCS.
    """
    
    def __init__(self, storage_path, project_id, credentials):
        self.storage_path = storage_path
        self.project_id = project_id
        self.credentials = credentials
        self.fs = gcsfs.GCSFileSystem(project=project_id, token=credentials)

    def write(self, file_path, data):
        try:
            with self.fs.open(file_path, 'w') as f:
                f.write(data)
            print(f"File successfully written to {file_path}.")
        except Exception as e:
            print(f"Error writing file to bucket: {e}")


class JsonStorageWriter:
    """
    Classe responsável por converter dados em formato JSON e delegar a escrita
    do arquivo para uma implementação de StorageWriter, como o GCSStorageWriter.
    """
    
    def __init__(self, storage_writer):
        self.storage_writer = storage_writer


    def write_json_to_storage(self, file_path, json_data):
        json_string = json.dumps(json_data, indent=4)
        
        self.storage_writer.write(file_path, json_string)

