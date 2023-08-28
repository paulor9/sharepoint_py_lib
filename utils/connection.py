import requests

from .functions import *
from .parameters import *


class Connection:
    cookies = None
    site_url = SHAREPOINT_SITE_URL
    list_name = SHAREPOINT_LIST_NAME
    list_name_for_create = SHAREPOINT_LIST_NAME_FOR_CREATE
    digest_header = None
    logger = None

    def __init__(self, logger: object) -> object:
        self.logger = logger
        self.digest_header = ''
        config = get_config(CREDENTIALS_PATH)

        # Uso da função
        # Cookie de sessão
        self.cookies = {
            'FedAuth': config['sharepoint_fed_auth'],
            'SIMI': config['sharepoint_simi'],
            'rtFa': config['sharepoint_rtfa']
        }
        self.get_sharepoint_digest()
    def get_sharepoint_digest(self):
       # URL da API para obter o valor do Digest Value
       api_url = f"{self.site_url}/_api/contextinfo"

       # Cabeçalhos para a requisição POST
       headers = {
        "accept": "application/json;odata=verbose",
       }

       # Realizando a requisição POST
       response = requests.post(api_url, headers=headers, cookies=self.cookies)
       self.logger.debug(response)

       # Verificando a resposta
       if response.status_code == 200:
          digest_value = response.json()["d"]["GetContextWebInformation"]["FormDigestValue"]
          self.logger.debug(f"Digest Value: {digest_value}")
          self.digest_header = digest_value
       else:
          raise requests.exceptions.HTTPError(f"Falha ao obter Digest Value. {response.status_code}", response=response)
