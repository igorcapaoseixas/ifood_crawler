import json
import requests

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
    
    def __init__(self, access_key, secret_key, http_client):
        self.access_key = access_key
        self.secret_key = secret_key
        self.http_client = http_client


    def search_store_catalog(self, store_id):
        base_url = 'https://wsloja.ifood.com.br/ifood-ws-v3/v1/merchants'
        headers = {
            'acess_key': self.access_key,
            'secret_key': self.secret_key
        }
        response = self.http_client.get(f'{base_url}/{store_id}/catalog', headers=headers)
        if response.status_code == 200:
            return json.loads(response.content)


    def search_store_details(self, store_id):
        base_url = 'https://marketplace.ifood.com.br/v1/merchant-info/graphql'
        payload = {
            f"query": "query ($merchantId: String!) { merchant (merchantId: $merchantId, required: true) { available availableForScheduling contextSetup { catalogGroup context regionGroup } currency deliveryFee { originalValue type value } deliveryMethods { catalogGroup deliveredBy id maxTime minTime mode originalValue priority schedule { now shifts { dayOfWeek endTime interval startTime } timeSlots { availableLoad date endDateTime endTime id isAvailable originalPrice price startDateTime startTime } } subtitle title type value state } deliveryTime distance features id mainCategory { code name } minimumOrderValue name paymentCodes preparationTime priceRange resources { fileName type } slug tags takeoutTime userRating } merchantExtra (merchantId: $merchantId, required: false) { address { city country district latitude longitude state streetName streetNumber timezone zipCode } categories { code description friendlyName } companyCode configs { bagItemNoteLength chargeDifferentToppingsMode nationalIdentificationNumberRequired orderNoteLength } deliveryTime description documents { CNPJ { type value } MCC { type value } } enabled features groups { externalId id name type } id locale mainCategory { code description friendlyName } merchantChain { externalId id name } metadata { ifoodClub { banner { action image priority title } } } minimumOrderValue minimumOrderValueV2 name phoneIf priceRange resources { fileName type } shifts { dayOfWeek duration start } shortId tags takeoutTime test type userRatingCount } }",
            "variables": {"merchantId": store_id}
        }
        response = self.http_client.post(base_url, payload=payload)
        if response.status_code == 200:
            return json.loads(response.content)

