import requests
import pandas as pd
import re
from .parameters import *
from .data_metrics import *

dataMetrics = DataMetrics()


def update_sharepoint_list_item_field2(item_id, update_field_name, update_field_value, connection):
    if not str(item_id).isdigit():
        raise Exception(
            f"Falha ao atualizar o campo {update_field_name}. Id do item inválido.")

    api_url = f"{connection.site_url}/_api/web/lists/getbytitle('{connection.list_name}')/items({item_id})"

    # Cabeçalhos para a requisição POST
    headers = {
        "Accept": "application/json;odata=verbose",
        "Content-Type": "application/json;odata=verbose",
        "X-HTTP-Method": "MERGE",
        "IF-MATCH": "*",
        "X-RequestDigest": connection.digest_header,
    }

    # Dados para atualizar um novo item
    data = {
        "__metadata": {
            "type": f"SP.Data.{connection.list_name_for_create}ListItem"
        },
    }

    data.update({update_field_name: update_field_value})

    response = requests.post(
        api_url, headers=headers, json=data, cookies=connection.cookies)

    if response.status_code == 204:
        connection.logger.info(
            f"Campo {update_field_name} do item atualizado com sucesso. <{response.status_code}.>")
    else:
        connection.logger.error(response.text)
        raise requests.exceptions.HTTPError(
            f"Falha ao atualizar o campo {update_field_name}. Erro {response.status_code}.",
            response=response)


def update_aux_filter_value(p_id, p_token, connection):
    if p_id == -1:
        try:
            max_retries = 5
            for retry in range(1, max_retries + 1):
                try:
                    update_sharepoint_list_item_field2(item_id=p_id, update_field_name='aux_filter_data',
                                                       update_field_value=p_token, connection=connection)
                    break
                except requests.exceptions.HTTPError as err:
                    status_code = err.response.status_code
                    if status_code == 503 or status_code == 403:
                        connection.logger.error(
                            f"Falha ao atualizar o campo. Erro {status_code}.")
                        connection.logger.error(err)
                        if retry < max_retries:
                            connection.logger.debug(
                                f"Tentativa {retry} de {max_retries}...")
                            connection.get_sharepoint_digest()
                        else:
                            connection.logger.error(
                                f"Máximo de tentativas alcançadas. Falha ao atualizar o campo. Erro {status_code}."
                            )
                            return
                    else:
                        connection.logger.error(
                            f"Falha ao criar item. Erro {status_code}.")
                        connection.logger.error(err)
                        return
        except Exception as err:
            connection.logger.error(err)
            return
        connection.logger.info("Campo atualizado com sucesso.")


def generate_csv_file(p_path, p_list):
    v_df = pd.DataFrame(p_list)
    v_df.to_csv(p_path,
                columns=["ID", "Sistema_x0028_AF_x0029_Id", "SistemaGitlab", "DiretoriadoOwner",
                         "GerenciaSrdoOwner", "Owner", "Linguagem", "Title", "TipodeBuild",
                         "CIServer", "TipoFerramentaSCM_x0028_Controle",
                         "DatadeAtualiza_x00e7__x00e3_o", "UrldoGIT", "URLPipeline",
                         "StatusdaMigra_x00e7__x00e3_o", "Ignorar",
                         "A_x00e7__x00e3_oap_x00f3_sRevis_", "VALIDADO"])


def check_hub_pagamentos_filter(url_value, item, connection):
    aux_id = item.get('ID')
    validado = item.get('VALIDADO')
    if re.search('https://gitlab.redecorp.br/hubdepagamento', url_value):
        dataMetrics.all_hub_pag.append(item)
        if validado:
            dataMetrics.all_hub_pag_done.append(item)
        # update_aux_filter_value(aux_id, 'HUB_PAGAMENTO', connection)


def check_b2b_filter(url_value, item, connection):
    aux_id = item.get('ID')
    validado = item.get('VALIDADO')
    v_diretor = item.get('DiretoriadoOwner')
    if isinstance(v_diretor, str):
        if re.search('Gabriel Simoes Goncalves Da Silva', v_diretor):
            dataMetrics.all_b2b.append(item)
            if validado:
                dataMetrics.all_b2b_done.append(item)
            # update_aux_filter_value(aux_id, 'HUB_PAGAMENTO', connection)


def check_loja_online_filter(url_value, item, connection):
    aux_id = item.get('ID')
    validado = item.get('VALIDADO')
    if re.search('https://gitlab.redecorp.br/LojaOnline', url_value):
        dataMetrics.all_loja_online.append(item)
        if validado:
            dataMetrics.all_loja_online_done.append(item)
        # update_aux_filter_value(aux_id, 'HUB_PAGAMENTO', connection)


def check_integracao_dip_filter(url_value, item, connection):
    aux_id = item.get('ID')
    validado = item.get('VALIDADO')
    if re.search('https://gitlab.redecorp.br/business-partner-domain', url_value):
        dataMetrics.dip_Business_Partner_Domain.append(item)
        if validado:
            dataMetrics.dip_Business_Partner_Domain_done.append(item)
        # update_aux_filter_value(aux_id, 'DIP-Business-Partner-Domain', connection)
    elif re.search('https://gitlab.redecorp.br/common-domain', url_value):
        dataMetrics.dip_Common_Domain.append(item)
        if validado:
            dataMetrics.dip_Common_Domain_done.append(item)
        # update_aux_filter_value(aux_id, 'DIP-Common-Domain', connection)
    elif re.search('https://gitlab.redecorp.br/customer-domain', url_value):
        dataMetrics.dip_Customer_Domain.append(item)
        if validado:
            dataMetrics.dip_Customer_Domain_done.append(item)
        # update_aux_filter_value(aux_id, 'DIP-Customer-Domain', connection)
    elif re.search('https://gitlab.redecorp.br/enterprise-domain', url_value):
        dataMetrics.dip_Enterprise_Domain.append(item)
        if validado:
            dataMetrics.dip_Enterprise_Domain_done.append(item)
        # update_aux_filter_value(aux_id, 'DIP-Enterprise-Domain', connection)
    elif re.search('https://gitlab.redecorp.br/integration-domain', url_value):
        dataMetrics.dip_Integration_Domain.append(item)
        if validado:
            dataMetrics.dip_Integration_Domain_done.append(item)
        # update_aux_filter_value(aux_id, 'DIP-Integration-Domain', connection)
    elif re.search('https://gitlab.redecorp.br/market-sales-domain', url_value):
        dataMetrics.dip_Market_Sales_Domain.append(item)
        if validado:
            dataMetrics.dip_Market_Sales_Domain_done.append(item)
        # update_aux_filter_value(aux_id, 'DIP-Market-Sales-Domain', connection)
    elif re.search('https://gitlab.redecorp.br/product-domain', url_value):
        dataMetrics.dip_Product_Domain.append(item)
        if validado:
            dataMetrics.dip_Product_Domain_done.append(item)
        # update_aux_filter_value(aux_id, 'DIP-Product-Domain', connection)
    elif re.search('https://gitlab.redecorp.br/resource-domain', url_value):
        dataMetrics.dip_Resource_Domain.append(item)
        if validado:
            dataMetrics.dip_Resource_Domain_done.append(item)
        # update_aux_filter_value(aux_id, 'DIP-Resource-Domain', connection)
    elif re.search('https://gitlab.redecorp.br/service-domain', url_value):
        dataMetrics.dip_Service_Domain.append(item)
        if validado:
            dataMetrics.dip_Service_Domain_done.append(item)
        # update_aux_filter_value(aux_id, 'DIP-Service-Domain', connection)
    elif re.search('https://gitlab.redecorp.br/osb', url_value):
        dataMetrics.l_osb.append(item)
        if validado:
            dataMetrics.l_osb_done.append(item)
        # update_aux_filter_value(aux_id, 'OSB', connection)
    elif re.search('https://gitlab.redecorp.br/soa', url_value):
        dataMetrics.l_soa.append(item)
        if validado:
            dataMetrics.l_soa_done.append(item)
        # update_aux_filter_value(aux_id, 'SOA', connection)
    elif re.search('https://gitlab.redecorp.br/api-management/src-77', url_value):
        dataMetrics.l_api_gateway.append(item)
        if validado:
            dataMetrics.l_api_gateway_done.append(item)
        # update_aux_filter_value(aux_id, 'API-Gateway', connection)


def export_csv_integracao_dip_filter(connection):
    generate_csv_file('c:/Temp/dip_Business_Partner_Domain.csv', dataMetrics.dip_Business_Partner_Domain)
    generate_csv_file('c:/Temp/dip_Common_Domain.csv', dataMetrics.dip_Common_Domain)
    generate_csv_file('c:/Temp/dip_Customer_Domain.csv', dataMetrics.dip_Customer_Domain)
    generate_csv_file('c:/Temp/dip_Enterprise_Domain.csv', dataMetrics.dip_Enterprise_Domain)
    generate_csv_file('c:/Temp/dip_Integration_Domain.csv', dataMetrics.dip_Integration_Domain)
    generate_csv_file('c:/Temp/dip_Market_Sales_Domain.csv', dataMetrics.dip_Market_Sales_Domain)
    generate_csv_file('c:/Temp/dip_Product_Domain.csv', dataMetrics.dip_Product_Domain)
    generate_csv_file('c:/Temp/dip_Resource_Domain.csv', dataMetrics.dip_Resource_Domain)
    generate_csv_file('c:/Temp/dip_Service_Domain.csv', dataMetrics.dip_Service_Domain)

    total_dip = len(dataMetrics.dip_Business_Partner_Domain) + len(dataMetrics.dip_Common_Domain) + len(
        dataMetrics.dip_Customer_Domain) + len(
        dataMetrics.dip_Enterprise_Domain) + len(dataMetrics.dip_Integration_Domain)
    total_dip = total_dip + len(dataMetrics.dip_Market_Sales_Domain) + len(dataMetrics.dip_Product_Domain) + len(
        dataMetrics.dip_Resource_Domain) + len(
        dataMetrics.dip_Service_Domain)

    total_osb = len(dataMetrics.l_osb)
    total_soa = len(dataMetrics.l_soa)
    total_gateway = len(dataMetrics.l_api_gateway)

    connection.logger.info(f"{total_dip} total dip.")
    connection.logger.info(f"{total_osb} total osb.")
    connection.logger.info(f"{total_soa} total soa.")
    connection.logger.info(f"{total_gateway} total api gateway.")

    total_area = total_dip + total_soa + total_gateway + total_osb
    connection.logger.info(f"{total_area} total area integrações.")

    generate_csv_file('c:/Temp/osb.csv', dataMetrics.l_osb)
    generate_csv_file('c:/Temp/soa.csv', dataMetrics.l_soa)
    generate_csv_file('c:/Temp/api_gateway.csv', dataMetrics.l_api_gateway)


def get_all_sharepoint_list_items(connection):
    api_url = f"{connection.site_url}/_api/web/lists/getbytitle('{connection.list_name}')/items"

    headers = {
        "accept": "application/json;odata=verbose",
        "X-RequestDigest": connection.digest_header
    }

    validado = False
    count = 0
    while True:
        response = requests.get(api_url, cookies=connection.cookies, headers=headers)
        url_repo_internal_field = HEADER_SHAREPOINT[FIELD_URL_REPO]

        if response.status_code == 200:
            data = response.json()["d"]["results"]
            count += len(data)
            for item in data:
                id = item['__metadata']['id']
                url_value = item.get(url_repo_internal_field)
                if url_value is not None:
                    url_value = url_value["Url"]
                    item[url_repo_internal_field] = url_value
                dataMetrics.all_items.append(item)

                validado = item.get('VALIDADO')
                if validado:
                    dataMetrics.all_items_validados.append(item)
                else:
                    dataMetrics.all_items_not_validados.append(item)
                check_integracao_dip_filter(url_value, item, connection)
                check_hub_pagamentos_filter(url_value, item, connection)
                check_b2b_filter(url_value, item, connection)
                check_loja_online_filter(url_value, item, connection)
            connection.logger.info(f"{count} itens processados.")

            # Check if more items are available
            next_link = response.json()["d"].get("__next")
            if next_link:
                api_url = next_link
            else:
                break
        else:
            raise requests.exceptions.HTTPError(
                f"Falha ao recuperar itens. {response.status_code}",
                response=response)

    export_csv_integracao_dip_filter(connection)
    generate_csv_file('c:/Temp/validados.csv', dataMetrics.all_items_validados)
    generate_csv_file('c:/Temp/not_validados.csv', dataMetrics.all_items_not_validados)

    return dataMetrics.all_items


def get_sharepoint_filtered_items(filter_query, connection, select_fields=None):
    api_url = f"{connection.site_url}/_api/web/lists/getbytitle('{connection.list_name}')/items"

    headers = {
        "accept": "application/json;odata=verbose",
        "X-RequestDigest": connection.digest_header
    }

    url_repo_internal_field = HEADER_SHAREPOINT[FIELD_URL_REPO]
    items = []
    next_link = None
    count = 0
    while True:
        params = {"$filter": filter_query}
        if select_fields:
            params["$select"] = ",".join(select_fields)

        connection.logger.debug(params)
        if not next_link:
            response = requests.get(api_url, cookies=connection.cookies, headers=headers, params=params)
        else:
            response = requests.get(api_url, cookies=connection.cookies, headers=headers)

        if response.status_code == 200:
            data = response.json()["d"]["results"]
            count += len(data)
            for item in data:
                url_value = item.get(url_repo_internal_field)
                if url_value is not None:
                    url_value = url_value["Url"]
                    item[url_repo_internal_field] = url_value
                items.append(item)
            connection.logger.info(f"{count} itens processados.")

            # Check if more items are available
            next_link = response.json()["d"].get("__next")
            if next_link:
                api_url = next_link
            else:
                break
        else:
            connection.logger.error(response.text)
            raise requests.exceptions.HTTPError(
                f"Falha ao recuperar itens. {response.status_code}",
                response=response)

    return items


def get_sharepoint_list_item(field_name, field_value, connection):
    api_url = f"{connection.site_url}/_api/web/lists/getbytitle('{connection.list_name}')/items?$filter={field_name} eq '{field_value}'"

    headers = {
        "accept": "application/json;odata=verbose",
        "X-RequestDigest": connection.digest_header
    }

    response = requests.get(api_url, headers=headers, cookies=connection.cookies)
    data = response.json()

    # Verificar a resposta
    if response.status_code == 200:
        connection.logger.info(f"Item recuperado com sucesso. {response.status_code}")
        return data["d"]["results"]
    else:
        connection.logger.error(response.text)
        raise requests.exceptions.HTTPError(
            f"Falha ao recuperar item. Erro {response.status_code}.",
            response=response)


def get_sharepoint_list_item_id(field_name, field_value, connection):
    item_data = get_sharepoint_list_item(field_name, field_value, connection)
    item_data_size = len(item_data)
    if item_data and item_data_size == 1:
        item_id = item_data[0]["Id"]
        connection.logger.info(f"O item com Id {item_id} foi recuperado com sucesso.")
        return item_id
    elif item_data_size == 0:
        raise Exception(
            f"Nenhum item foi recuperado para o campo {field_name} com valor {field_value}.")
    elif item_data_size > 1:
        raise Exception(
            f"Mais de um item foi recuperado para o campo {field_name} com valor {field_value}.")


def update_sharepoint_list_item_field(item_id, update_field_name, update_field_value, connection):
    if not str(item_id).isdigit():
        raise Exception(
            f"Falha ao atualizar o campo {update_field_name}. Id do item inválido.")

    api_url = f"{connection.site_url}/_api/web/lists/getbytitle('{connection.list_name}')/items({item_id})"

    # Cabeçalhos para a requisição POST
    headers = {
        "Accept": "application/json;odata=verbose",
        "Content-Type": "application/json;odata=verbose",
        "X-HTTP-Method": "MERGE",
        "IF-MATCH": "*",
        "X-RequestDigest": connection.digest_header,
    }

    # Dados para atualizar um novo item
    data = {
        "__metadata": {
            "type": f"SP.Data.{connection.list_name_for_create}ListItem"
        },
    }

    data.update({update_field_name: update_field_value})

    response = requests.post(api_url, headers=headers, json=data, cookies=connection.cookies)

    if response.status_code == 204:
        connection.logger.info(
            f"Campo {update_field_name} do item atualizado com sucesso. <{response.status_code}.>")
    else:
        connection.logger.error(response.text)
        raise requests.exceptions.HTTPError(
            f"Falha ao atualizar o campo {update_field_name}. Erro {response.status_code}.", response=response)


def update_migration_status(item_component_name, state_field_value, connection):
    valid_status = ["Priorizado", "Iniciado", "Migrado", "Sanitizado"]
    if state_field_value not in valid_status:
        connection.logger.error(
            f"Os valores válidos para o campo são: {valid_status}. Foi passado o valor '{state_field_value}'.")
    else:
        try:
            digest_header = connection.digest_header

            item_id = get_sharepoint_list_item_id(field_name=HEADER_SHAREPOINT[FIELD_NAME_COMP],
                                                  field_value=item_component_name, connection=connection)
        except Exception as err:
            connection.logger.error(err)
        else:
            max_retries = 5
            for retry in range(1, max_retries + 1):
                try:
                    update_sharepoint_list_item_field(
                        item_id=item_id,
                        update_field_name=HEADER_SHAREPOINT[FIELD_STATUS],
                        update_field_value=state_field_value,
                        connection=connection)
                    break
                except requests.exceptions.HTTPError as err:
                    status_code = err.response.status_code
                    if status_code == 503 or status_code == 403:
                        connection.logger.error(
                            f"Falha ao atualizar o campo. Erro {status_code}.")
                        connection.logger.error(err)
                        if retry < max_retries:
                            connection.logger.debug(
                                f"Tentativa {retry} de {max_retries}...")
                            connection.get_sharepoint_digest()
                        else:
                            connection.logger.error(
                                f"Máximo de tentativas alcançadas. Falha ao atualizar o campo. Erro {status_code}."
                            )
                            return
                    else:
                        connection.logger.error(
                            f"Falha ao criar item. Erro {status_code}.")
                        connection.logger.error(err)
                        return
                except Exception as err:
                    connection.logger.error(err)
                    return
            connection.logger.info("Campo atualizado com sucesso.")


# Função para criar um item na lista do SharePoint
def create_sharepoint_list_item(item, connection):
    # URL da API para criar um novo item na lista
    api_url = f"{connection.site_url}/_api/web/lists/getbytitle('{connection.list_name}')/items"

    # Cabeçalhos para a requisição POST
    headers = {
        "accept": "application/json;odata=verbose",
        "content-type": "application/json;odata=verbose",
        "odata-version": "",
        "IF-MATCH": "*",
        "X-RequestDigest": connection.digest_header,
    }

    # Dados para criar um novo item
    data = {
        "__metadata": {
            "type": f"SP.Data.{connection.list_name_for_create}ListItem"
        },  # Substitua o NomeDaLista pelo nome da sua lista
    }

    # Adicionar os campos do item
    data.update(item)
    connection.logger.debug(f"json. {data}")

    # Realizar a requisição POST
    response = requests.post(api_url,
                             cookies=connection.cookies,
                             headers=headers,
                             json=data)

    # Verificar a resposta
    if response.status_code == 201:
        connection.logger.info("Item criado com sucesso.")
    else:
        response.raise_for_status()


def generate_sharepoint_list_all_items_csv(csv_file_path, connection):
    dataMetrics.logger = connection.logger
    df_sharepoint = pd.DataFrame(get_all_sharepoint_list_items(connection))
    connection.logger.debug(f'{len(df_sharepoint.index)} itens recuperados do sharepoint.')
    connection.logger.debug(df_sharepoint.columns)

    df_sharepoint.to_csv(csv_file_path)

    connection.logger.debug(f"Os items foram escritos no arquivo {csv_file_path}")
    dataMetrics.log_resume()
