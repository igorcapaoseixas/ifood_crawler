import json
import requests
from google.cloud import bigquery

class HttpClient:
    """
    Classe responsável por abstrair a funcionalidade de requisições HTTP.
    Permite a configuração de cabeçalhos e proxies, facilitando a reutilização
    em diferentes tipos de requisições, como GET e POST.
    """
    
    def __init__(self, user_agent, proxies=None):
        self.user_agent = user_agent
        self.proxies = proxies


    def get(self, url, headers=None):
        headers = headers or {}
        headers['User-Agent'] = self.user_agent
        return requests.get(url, headers=headers, proxies=self.proxies)


    def post(self, url, headers=None, payload=None):
        headers = headers or {}
        headers['User-Agent'] = self.user_agent
        return requests.post(url, headers=headers, json=payload, proxies=self.proxies)


class IfoodCrawler:
    """
    Classe responsável por realizar o scraping de dados do site da iFood.
    Utiliza o HttpClient para fazer as requisições e oferece métodos específicos
    para buscar catálogo e detalhes das lojas.
    """
    
    def __init__(self, access_key, secret_key, http_client, credentials):
        self.access_key = access_key
        self.secret_key = secret_key
        self.http_client = http_client
        self.client = bigquery.Client(credentials=credentials)


    def search_store_catalog(self, store_id):
        base_url = 'https://wsloja.ifood.com.br/ifood-ws-v3/v1/merchants'
        headers = {
            'acess_key': self.access_key,
            'secret_key': self.secret_key
        }
        try:
            response = self.http_client.get(f'{base_url}/{store_id}/catalog', headers=headers)
            if response.status_code == 200:
                return json.loads(response.content)
        except Exception as e:
            print(f'|--> Exception raised {e} when trying to access {base_url}/{store_id}/catalog')


    def store_exists_in_bigquery(self, store_id):
        """
        Check if the store_id exists in the BigQuery table.
        """
        query = f"""
        SELECT COUNT(1) as count
        FROM `dw-volix.dominos_silver.scrapings_ifood_details`
        WHERE store_id = @store_id
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("store_id", "STRING", store_id)
            ]
        )
        query_job = self.client.query(query, job_config=job_config)
        result = query_job.result()
        return result.total_rows > 0 and next(iter(result)).count > 0


    def search_store_details(self, store_id):
        """
        Fetch store details if not already present in BigQuery.
        """
        if self.store_exists_in_bigquery(store_id):
            print(f"|--> Store {store_id} already exists in BigQuery. Skipping scraping.")
            return None
        
        base_url = 'https://marketplace.ifood.com.br/v1/merchant-info/graphql?latitude=&longitude=&channel=IFOOD'
        payload = {
            f"query": "query ($merchantId: String!) { merchant (merchantId: $merchantId, required: true) { available availableForScheduling contextSetup { catalogGroup context regionGroup } currency deliveryFee { originalValue type value } deliveryMethods { catalogGroup deliveredBy id maxTime minTime mode originalValue priority schedule { now shifts { dayOfWeek endTime interval startTime } timeSlots { availableLoad date endDateTime endTime id isAvailable originalPrice price startDateTime startTime } } subtitle title type value state } deliveryTime distance features id mainCategory { code name } minimumOrderValue name paymentCodes preparationTime priceRange resources { fileName type } slug tags takeoutTime userRating } merchantExtra (merchantId: $merchantId, required: false) { address { city country district latitude longitude state streetName streetNumber timezone zipCode } categories { code description friendlyName } companyCode configs { bagItemNoteLength chargeDifferentToppingsMode nationalIdentificationNumberRequired orderNoteLength } deliveryTime description documents { CNPJ { type value } MCC { type value } } enabled features groups { externalId id name type } id locale mainCategory { code description friendlyName } merchantChain { externalId id name } metadata { ifoodClub { banner { action image priority title } } } minimumOrderValue minimumOrderValueV2 name phoneIf priceRange resources { fileName type } shifts { dayOfWeek duration start } shortId tags takeoutTime test type userRatingCount } }",
            "variables": {"merchantId": store_id}
        }
        try:
            response = self.http_client.post(base_url, payload=payload)
            if response.status_code == 200:
                return json.loads(response.content)
        except Exception as e:
            print(f'|--> Exception raised {e} when trying to access {base_url}')

