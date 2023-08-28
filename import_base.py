import datetime
import requests
import pandas as pd
import sys
import os
import coloredlogs
import logging
from time import sleep
from utils import sharepoint_functions, functions
from utils.parameters import *
from utils import connection as conn

config = functions.get_config(CREDENTIALS_PATH)


# Leitura dos itens do arquivo
current_date = (datetime.datetime.now()).strftime("%d_%m_%Y")
errors_file_path = os.path.join(
    DATA_DIR_PATH, f'linhas_com_problemas_{current_date}.txt')


def main():
    logger = logging.getLogger(__name__)
    coloredlogs.install(level='DEBUG', logger=logger)

    connection = conn.Connection(logger)

    nexus_user = config['nexus_user']
    nexus_password = config['nexus_pwd']
    version = '07_08_2023'

    functions.get_database(version, DATA_DIR_PATH, nexus_user, nexus_password)
    functions.unzip_file_and_remove(os.path.join(DATA_DIR_PATH, 'dados_migracao.zip'), DATA_DIR_PATH)

    import_file_path = os.path.join(
        DATA_DIR_PATH, 'dados_migracao', 'importacao_sharepoint.csv')

    items = functions.read_csv_items(import_file_path)

    if connection.digest_header != '':
        field_af = HEADER_SHAREPOINT[FIELD_AF]
        field_tech = HEADER_SHAREPOINT[FIELD_TECH]
        field_classif = HEADER_SHAREPOINT[FIELD_CLASSIF]
        field_url_repo = HEADER_SHAREPOINT[FIELD_URL_REPO]
        cont = 0
        start_at = 0
        for item in items:
            cont = cont + 1
            logger.info(f'lendo a linha: {cont}')
            if cont >= start_at:
                # Dados para criar um novo item
                json_url_git = {
                    '__metadata': {
                        'type': 'SP.FieldUrlValue'
                    },  # Substitua o NomeDaLista pelo nome da sua lista
                    'Url': item["UrldoGIT"]
                }

                if str(item[field_af]).strip() == "":
                    item.pop(field_af)
                elif not str(item[field_af]).isdigit():
                    try:
                        field_value = item[field_af]
                        functions.add_line_to_file(
                            errors_file_path,
                            f"{cont}\n{field_af}: {field_value}\n\n",
                            logger)
                        continue
                    except Exception as err:
                        logger.error(err)
                        break

                if str(item[field_tech]).strip() == "":
                    item[field_tech] = ""

                if str(item[field_classif]).strip() == "":
                    item[field_classif] = ""

                item[field_url_repo] = json_url_git

                max_retries = 5
                for retry in range(1, max_retries + 1):
                    logger.info(f'processando a linha: {cont}')
                    try:
                        sharepoint_functions.create_sharepoint_list_item(item, connection=connection)
                        break
                    except requests.exceptions.HTTPError as err:
                        status_code = err.response.status_code
                        if status_code == 503 or status_code == 403:
                            logger.error(
                                f'Falha ao criar item. Erro {status_code}: {err}')
                            if retry < max_retries:
                                logger.debug(
                                    f'Tentativa {retry} de {max_retries}...')
                                connection.get_sharepoint_digest()
                            else:
                                logger.error(
                                    f'Máximo de tentativas alcançadas. Falha ao criar item. {status_code}'
                                )
                        else:
                            logger.error(
                                f'Falha ao criar item. Erro {status_code}: {err}')
                            sys.exit()
                sleep(0.05)
        logger.info('Finalizado')


if __name__ == "__main__":
    main()
