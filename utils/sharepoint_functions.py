import requests
import pandas as pd
import re
import numpy as np
import sys

from time import sleep
from .parameters import *
from .data_metrics import *
from .functions import *


def get_sharepoint_digest(cookies, site_url, logger):
    # URL da API para obter o valor do Digest Value
    api_url = f"{site_url}/_api/contextinfo"

    # Cabeçalhos para a requisição POST
    headers = {
        "accept": "application/json;odata=verbose",
    }

    # Realizando a requisição POST
    response = requests.post(api_url, headers=headers, cookies=cookies)
    logger.debug(response)

    # Verificando a resposta
    if response.status_code == 200:
        digest_value = response.json(
        )["d"]["GetContextWebInformation"]["FormDigestValue"]
        logger.debug(f"Digest Value: {digest_value}")
        return digest_value
    else:
        raise requests.exceptions.HTTPError(
            f"Falha ao obter Digest Value. {response.status_code}", response=response)


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
    if p_id != -1:
        try:
            max_retries = 5
            for retry in range(1, max_retries + 1):
                try:
                    connection.get_sharepoint_digest()
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
                            f"Falha ao Atualizar item. Erro {status_code}.")
                        connection.logger.error(err)
                        return
        except Exception as err:
            connection.logger.error(err)
            return
        connection.logger.info("Campo atualizado com sucesso.")


def update_aux_filter3_value(p_id, p_token, connection):
    if p_id != -1:
        try:
            max_retries = 5
            for retry in range(1, max_retries + 1):
                try:
                    connection.get_sharepoint_digest()
                    update_sharepoint_list_item_field2(item_id=p_id, update_field_name='aux_token_3',
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
                            f"Falha ao Atualizar item. Erro {status_code}.")
                        connection.logger.error(err)
                        return
        except Exception as err:
            connection.logger.error(err)
            return
        connection.logger.info("Campo atualizado com sucesso.")


def update_aux_filter2_value(p_id, p_token, connection):
    if p_id != -1:
        try:
            max_retries = 5
            for retry in range(1, max_retries + 1):
                try:
                    connection.get_sharepoint_digest()
                    update_sharepoint_list_item_field2(item_id=p_id, update_field_name='aux_url_scm',
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
                            f"Falha ao Atualizar item. Erro {status_code}.")
                        connection.logger.error(err)
                        return
        except Exception as err:
            connection.logger.error(err)
            return
        connection.logger.info("Campo atualizado com sucesso.")


def update_aux_toke2_value(p_id, p_token, connection):
    if p_id != -1:
        try:
            max_retries = 5
            for retry in range(1, max_retries + 1):
                try:
                    connection.get_sharepoint_digest()
                    update_sharepoint_list_item_field2(item_id=p_id, update_field_name='aux_token_2',
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
                            f"Falha ao Atualizar item. Erro {status_code}.")
                        connection.logger.error(err)
                        return
        except Exception as err:
            connection.logger.error(err)
            return
        connection.logger.info("Campo atualizado com sucesso.")


def update_diretor_value(p_id, p_token, connection):
    if p_id != -1:
        try:
            max_retries = 5
            for retry in range(1, max_retries + 1):
                try:
                    connection.get_sharepoint_digest()
                    update_sharepoint_list_item_field2(item_id=p_id, update_field_name='DiretoriadoOwner',
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
                            f"Falha ao Atualizar item. Erro {status_code}.")
                        connection.logger.error(err)
                        return
        except Exception as err:
            connection.logger.error(err)
            return
        connection.logger.info("Campo atualizado com sucesso.")


def update_cronograma_value(p_id, p_token, connection):
    if p_id != -1:
        try:
            max_retries = 5
            for retry in range(1, max_retries + 1):
                try:
                    connection.get_sharepoint_digest()
                    update_sharepoint_list_item_field2(item_id=p_id, update_field_name='cronograma',
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
                            f"Falha ao Atualizar item. Erro {status_code}.")
                        connection.logger.error(err)
                        return
        except Exception as err:
            connection.logger.error(err)
            return
        connection.logger.info("Campo atualizado com sucesso.")


def update_status_migracao_value(p_id, p_token, connection):
    if p_id != -1:
        try:
            max_retries = 5
            for retry in range(1, max_retries + 1):
                try:
                    connection.get_sharepoint_digest()
                    update_sharepoint_list_item_field2(item_id=p_id, update_field_name='StatusdaMigra_x00e7__x00e3_o',
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
                            f"Falha ao Atualizar item. Erro {status_code}.")
                        connection.logger.error(err)
                        return
        except Exception as err:
            connection.logger.error(err)
            return
        connection.logger.info("Campo atualizado com sucesso.")


def update_acao_pos_revisao_value(p_id, p_token, connection):
    if p_id != -1:
        try:
            max_retries = 5
            for retry in range(1, max_retries + 1):
                try:
                    connection.get_sharepoint_digest()
                    update_sharepoint_list_item_field2(item_id=p_id,
                                                       update_field_name='A_x00e7__x00e3_oap_x00f3_sRevis_',
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
                            f"Falha ao Atualizar item. Erro {status_code}.")
                        connection.logger.error(err)
                        return
        except Exception as err:
            connection.logger.error(err)
            return
        connection.logger.info("Campo atualizado com sucesso.")


def update_email_ponto_focal(p_id, p_token, connection):
    if p_id == -1:
        try:
            max_retries = 5
            for retry in range(1, max_retries + 1):
                try:
                    connection.get_sharepoint_digest()
                    update_sharepoint_list_item_field2(item_id=p_id,
                                                       update_field_name='PontoFocaldaMigra_x00e7__x00e3_o',
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
                            f"Falha ao Atualizar item. Erro {status_code}.")
                        connection.logger.error(err)
                        return
        except Exception as err:
            connection.logger.error(err)
            return
        connection.logger.info("Campo atualizado com sucesso.")


def update_acao_apos_revisao(p_id, p_token, connection):
    if p_id == -1:
        try:
            max_retries = 5
            for retry in range(1, max_retries + 1):
                try:
                    connection.get_sharepoint_digest()
                    update_sharepoint_list_item_field2(item_id=p_id,
                                                       update_field_name='A_x00e7__x00e3_oap_x00f3_sRevis_',
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
                            f"Falha ao Atualizar item. Erro {status_code}.")
                        connection.logger.error(err)
                        return
        except Exception as err:
            connection.logger.error(err)
            return
        connection.logger.info("Campo atualizado com sucesso.")


def generate_csv_file(p_path, p_list):
    if p_list is not None and len(p_list) > 0:
        v_df = pd.DataFrame(p_list)
        v_df.to_csv(p_path,
                    columns=["ID", "PontoFocaldaMigra_x00e7__x00e3_o", "A_x00e7__x00e3_oap_x00f3_sRevis_", "VALIDADO",
                             "SistemaGitlab",
                             "UrldoGIT", "DiretoriadoOwner",
                             "GerenciaSrdoOwner", "Owner", "Linguagem", "Title", "TipodeBuild",
                             "CIServer", "TipoFerramentaSCM_x0028_Controle",
                             "DatadeAtualiza_x00e7__x00e3_o", "URLPipeline",
                             "StatusdaMigra_x00e7__x00e3_o", "Ignorar",
                             "Sistema_x0028_AF_x0029_Id"])


def generate_integracoes_all_api_gateway_csv_file(p_path, p_list):
    if p_list is not None and len(p_list) > 0:
        v_df = pd.DataFrame(p_list)
        v_df.to_csv(p_path)
        v_df.to_csv(p_path,
                    columns=["ID", "Sistema_x0028_AF_x0029_Id", "SistemaGitlab",
                             "DatadeAtualiza_x00e7__x00e3_o", "UrldoGIT", "A_x00e7__x00e3_oap_x00f3_sRevis_",
                             "StatusdaMigra_x00e7__x00e3_o"])


def check_hub_pagamentos_filter(url_value, item, data_metrics, connection):
    aux_id = item.get('ID')
    validado = item.get('VALIDADO')
    if re.search('https://gitlab.redecorp.br/hubdepagamento', url_value):
        data_metrics.data_hub_pag.all_items.append(item)
        it_acao = item.get(HEADER_SHAREPOINT[FIELD_SANIT_POS])
        if validado:
            data_metrics.data_hub_pag.all_validados.append(item)
            if it_acao is None:
                it_acao = 'Migrar'
            if re.search('Sanitizar', it_acao):
                data_metrics.data_hub_pag.all_sanitizar.append(item)
            elif re.search('Migrar', it_acao):
                data_metrics.data_hub_pag.all_migrar.append(item)
        aux = item.get('aux_filter_data')

        if aux is None:
            aux = ""
        if aux == "":
            update_aux_filter_value(aux_id, 'HUB_PAGAMENTO', connection)
        v_aux_status_migracao = item.get("StatusdaMigra_x00e7__x00e3_o")
        if v_aux_status_migracao is None:
            v_aux_status_migracao = ""
        if v_aux_status_migracao == "Migrado":
            update_cronograma_value(aux_id, "01/11/2023", connection)
        elif v_aux_status_migracao == "Sanitizado":
            update_cronograma_value(aux_id, "02/11/2023", connection)

    #    if v_aux_status_migracao == "" or v_aux_status_migracao == "Pronto para Migrar":
    #        if it_acao == "Migrar":
    #            update_status_migracao_value(aux_id, "Migrado", connection)
    #        elif it_acao == "Sanitizar":
    #            update_status_migracao_value(aux_id, "Sanitizado", connection)


def check_b2b_filter(url_value, item, data_metrics, connection):
    aux_id = item.get('ID')

    v_diretor = item.get('DiretoriadoOwner')

    if isinstance(v_diretor, str):
        if re.search('Gabriel Simoes Goncalves Da Silva', v_diretor):
            v_sistema_gitlab = item.get('SistemaGitlab')
            v_aux = item.get('aux_filter_data')
            if v_aux is None:
                v_aux = ""
            if v_aux != "NAO_RECONHECIDO":
                if v_sistema_gitlab is None:
                    v_sistema_gitlab = ""
                if re.search('LOJAONLINE-B2B', v_sistema_gitlab):
                    data_metrics.data_loja_b2b.all_items.append(item)
                else:
                    data_metrics.data_b2b.all_items.append(item)
                    validado = item.get('VALIDADO')
                    if validado:
                        data_metrics.data_b2b.all_validados.append(item)
                        it_acao = item.get(HEADER_SHAREPOINT[FIELD_SANIT_POS])
                        if it_acao is None:
                            it_acao = 'Migrar'
                        if re.search('Sanitizar', it_acao):
                            data_metrics.data_b2b.all_sanitizar.append(item)
                        elif re.search('Migrar', it_acao):
                            data_metrics.data_b2b.all_migrar.append(item)


def check_loja_online_filter(url_value, item, df_loja, data_metrics, connection):
    aux_id = item.get('ID')
    validado = item.get('VALIDADO')
    it_acao = item.get(HEADER_SHAREPOINT[FIELD_SANIT_POS])
    if url_value in df_loja['URL_GIT'].unique():
        data_metrics.data_loja_b2c.all_items.append(item)
        if validado:
            data_metrics.data_loja_b2c.all_validados.append(item)
            if it_acao is None:
                it_acao = 'Migrar'
            if re.search('Sanitizar', it_acao):
                data_metrics.data_loja_b2c.all_sanitizar.append(item)
            elif re.search('Migrar', it_acao):
                data_metrics.data_loja_b2c.all_migrar.append(item)

        i = df_loja[df_loja['URL_GIT'] == url_value]
        v_acao = i.get('ACAO').to_string()
        if re.search('MIGRAR', v_acao):
            v_acao = 'Migrar'
        elif re.search('SANITIZAR', v_acao):
            v_acao = 'Sanitizar'

        # update_email_ponto_focal(aux_id, 'lucas.pereira@telefonica.com', connection)
        # update_acao_apos_revisao(aux_id, v_acao, connection)
        # update_aux_filter_value(aux_id, 'LOJA_ONLINE_B2C', connection)


def integracao_data_metrics_update(item, data_metrics, connection):
    validado = item.get('VALIDADO')
    data_metrics.data_integra.all_items.append(item)
    if validado:
        it_acao = item.get(HEADER_SHAREPOINT[FIELD_SANIT_POS])
        data_metrics.data_integra.all_validados.append(item)
        if it_acao is None:
            it_acao = 'Migrar'
        if re.search('Sanitizar', it_acao):
            data_metrics.data_integra.all_sanitizar.append(item)
        elif re.search('Migrar', it_acao):
            data_metrics.data_integra.all_migrar.append(item)


def oss_data_metrics_update(item, data_metrics, connection):
    validado = item.get('VALIDADO')
    data_metrics.data_oss.all_items.append(item)
    if validado:
        it_acao = item.get(HEADER_SHAREPOINT[FIELD_SANIT_POS])
        data_metrics.data_oss.all_validados.append(item)
        if it_acao is None:
            it_acao = 'Migrar'
        if re.search('Sanitizar', it_acao):
            data_metrics.data_oss.all_sanitizar.append(item)
        elif re.search('Migrar', it_acao):
            data_metrics.data_oss.all_migrar.append(item)


def check_oss_other_filter(url_value, item, data_metrics, connection):
    aux_id = item.get('ID')
    url_value = url_value.upper()
    aux = item.get('aux_token_3')
    if aux is None:
        aux = ""
    if aux == "":
        v_gitlab = item.get("SistemaGitlab")
        if v_gitlab is None:
            v_gitlab = ""
        if v_gitlab == "SIGRES":
            update_aux_filter_value(aux_id, 'OSS_OTHER', connection)
        elif v_gitlab == "TELEFONICA":
            update_aux_filter_value(aux_id, 'OSS_OTHER', connection)
        elif v_gitlab == "NETCOOL-VIVO":
            update_aux_filter_value(aux_id, 'OSS_OTHER', connection)
        elif v_gitlab == "SCIENCE":
            update_aux_filter_value(aux_id, 'OSS_OTHER', connection)
        elif v_gitlab == "RESOURCE-ORDER":
            update_aux_filter_value(aux_id, 'OSS_OTHER', connection)
        elif v_gitlab == "TROUBLETICKET":
            update_aux_filter_value(aux_id, 'OSS_OTHER', connection)
        elif v_gitlab == "FSW11":
            update_aux_filter_value(aux_id, 'OSS_OTHER', connection)
        elif v_gitlab == "ROM-CAMUNDA":
            update_aux_filter_value(aux_id, 'OSS_OTHER', connection)
        elif v_gitlab == "PRUMA":
             update_aux_filter_value(aux_id, 'OSS_OTHER', connection)
        elif v_gitlab == "RM":
            update_aux_filter_value(aux_id, 'OSS_OTHER', connection)
        elif v_gitlab == "RESOURCE-TEST":
            update_aux_filter_value(aux_id, 'OSS_OTHER', connection)
        elif v_gitlab == "MANOBRA-GPON":
            update_aux_filter_value(aux_id, 'OSS_OTHER', connection)
        elif v_gitlab == "MANOBRA-UNIFICADA":
            update_aux_filter_value(aux_id, 'OSS_OTHER', connection)
        elif v_gitlab == "LM":
            update_aux_filter_value(aux_id, 'OSS_OTHER', connection)
        elif v_gitlab == "RESOURCE-SCHEMAS":
            update_aux_filter_value(aux_id, 'OSS_OTHER', connection)
        elif v_gitlab == "MANOBRA-ÚNICA":
            update_aux_filter_value(aux_id, 'OSS_OTHER', connection)
        elif v_gitlab == "OSSOPENAPIS":
            update_aux_filter_value(aux_id, 'OSS_OTHER', connection)
        elif v_gitlab == "LOCATIONMANAGEMENT":
            update_aux_filter_value(aux_id, 'OSS_OTHER', connection)
        elif v_gitlab == "SIGRES-MASSIVA":
            update_aux_filter_value(aux_id, 'OSS_OTHER', connection)
        elif v_gitlab == "ODP":
            update_aux_filter_value(aux_id, 'OSS_OTHER', connection)
        elif v_gitlab == "MONITOR-OSS":
            update_aux_filter_value(aux_id, 'OSS_OTHER', connection)
        elif v_gitlab == "BLUEPLANETINVENTORY":
            update_aux_filter_value(aux_id, 'OSS_OTHER', connection)
        elif v_gitlab == "SIGITM":
            update_aux_filter_value(aux_id, 'OSS_OTHER', connection)

def check_oss_filter(url_value, item, data_metrics, connection):
    aux_id = item.get('ID')
    update = False
    url_value = url_value.upper()
    aux = item.get('aux_filter_data')
    if aux is None:
        aux = ""
    if aux == "":
        update = True

    update_all = False
    aux_all = item.get('aux_url_scm')
    if aux_all is None:
        aux_all = ""
    if aux_all == "":
        update_all = True
    if re.search('https://gitlab.redecorp.br/accessmanagement'.upper(), url_value.upper()):
        oss_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'OSS_AccessManagement', connection)
        if update_all:
            update_aux_filter2_value(aux_id, 'OSS_ALL', connection)
    elif re.search('https://gitlab.redecorp.br/BluePlanetInventory'.upper(), url_value.upper()):
        oss_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'OSS_BluePlanetInventory', connection)
        if update_all:
            update_aux_filter2_value(aux_id, 'OSS_ALL', connection)
    elif re.search('https://gitlab.redecorp.br/changemanagement'.upper(), url_value.upper()):
        oss_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'OSS_ChangeManagement', connection)
        if update_all:
            update_aux_filter2_value(aux_id, 'OSS_ALL', connection)
    elif re.search('https://gitlab.redecorp.br/diagnoseserviceproblem'.upper(), url_value.upper()):
        oss_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'OSS_DiagnoseServiceProblem', connection)
        if update_all:
            update_aux_filter2_value(aux_id, 'OSS_ALL', connection)
    elif re.search('https://gitlab.redecorp.br/esboss'.upper(), url_value.upper()):
        oss_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'OSS_esboss', connection)
        if update_all:
            update_aux_filter2_value(aux_id, 'OSS_ALL', connection)
    elif re.search('https://gitlab.redecorp.br/geniaus'.upper(), url_value.upper()):
        oss_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'OSS_Geniaus', connection)
        if update_all:
            update_aux_filter2_value(aux_id, 'OSS_ALL', connection)
    elif re.search('https://gitlab.redecorp.br/gsim'.upper(), url_value.upper()):
        oss_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'OSS_Gsim', connection)
        if update_all:
            update_aux_filter2_value(aux_id, 'OSS_ALL', connection)
    elif re.search('https://gitlab.redecorp.br/gvox'.upper(), url_value.upper()):
        oss_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'OSS_Gvox', connection)
        if update_all:
            update_aux_filter2_value(aux_id, 'OSS_ALL', connection)
    elif re.search('https://gitlab.redecorp.br/locationmanagement'.upper(), url_value.upper()):
        oss_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'OSS_LocationManagement', connection)
        if update_all:
            update_aux_filter2_value(aux_id, 'OSS_ALL', connection)
    elif re.search('https://gitlab.redecorp.br/NetworkReallocation'.upper(), url_value.upper()):
        oss_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'OSS_NetworkReallocation', connection)
        if update_all:
            update_aux_filter2_value(aux_id, 'OSS_ALL', connection)
    elif re.search('https://gitlab.redecorp.br/NumberInventory'.upper(), url_value.upper()):
        oss_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'OSS_NumberInventory', connection)
        if update_all:
            update_aux_filter2_value(aux_id, 'OSS_ALL', connection)
    elif re.search('https://gitlab.redecorp.br/odp'.upper(), url_value):
        oss_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'OSS_odp', connection)
        if update_all:
            update_aux_filter2_value(aux_id, 'OSS_ALL', connection)
    elif re.search('https://gitlab.redecorp.br/resourceorder'.upper(), url_value.upper()):
        oss_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'OSS_resourceorder', connection)
        if update_all:
            update_aux_filter2_value(aux_id, 'OSS_ALL', connection)
    elif re.search('https://gitlab.redecorp.br/sigitmoss'.upper(), url_value.upper()):
        oss_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'OSS_SigitmOSS', connection)
        if update_all:
            update_aux_filter2_value(aux_id, 'OSS_ALL', connection)
    elif re.search('https://gitlab.redecorp.br/ossopenapis'.upper(), url_value.upper()):
        oss_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'OSS_ossopenapis', connection)
        if update_all:
            update_aux_filter2_value(aux_id, 'OSS_ALL', connection)
    elif re.search('https://gitlab.redecorp.br/oss-commons'.upper(), url_value.upper()):
        oss_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'OSS_oss-commons', connection)
        if update_all:
            update_aux_filter2_value(aux_id, 'OSS_ALL', connection)
    elif re.search('https://gitlab.redecorp.br/WorkforceManagement'.upper(), url_value.upper()):
        oss_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'OSS_WorkforceManagement', connection)
        if update_all:
            update_aux_filter2_value(aux_id, 'OSS_ALL', connection)
    elif re.search('https://gitlab.redecorp.br/wfm'.upper(), url_value.upper()):
        oss_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'OSS_WFM', connection)
        if update_all:
            update_aux_filter2_value(aux_id, 'OSS_ALL', connection)
    elif re.search('https://gitlab.redecorp.br/ResourceActivation'.upper(), url_value.upper()):
        oss_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'OSS_ResourceActivation', connection)
        if update_all:
            update_aux_filter2_value(aux_id, 'OSS_ALL', connection)
    elif re.search('https://gitlab.redecorp.br/resource-activation'.upper(), url_value.upper()):
        oss_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'OSS_resource-activation', connection)
        if update_all:
            update_aux_filter2_value(aux_id, 'OSS_ALL', connection)
    elif re.search('https://gitlab.redecorp.br/ResourceInventory'.upper(), url_value.upper()):
        oss_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'OSS_ResourceInventory', connection)
        if update_all:
            update_aux_filter2_value(aux_id, 'OSS_ALL', connection)
    elif re.search('https://gitlab.redecorp.br/resourceschemas'.upper(), url_value.upper()):
        oss_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'OSS_ResourceSchemas', connection)
        if update_all:
            update_aux_filter2_value(aux_id, 'OSS_ALL', connection)
    elif re.search('https://gitlab.redecorp.br/ResourceTest'.upper(), url_value.upper()):
        oss_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'OSS_ResourceTest', connection)
        if update_all:
            update_aux_filter2_value(aux_id, 'OSS_ALL', connection)
    elif re.search('https://gitlab.redecorp.br/scm'.upper(), url_value.upper()):
        oss_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'OSS_SCM', connection)
        if update_all:
            update_aux_filter2_value(aux_id, 'OSS_ALL', connection)
    elif re.search('https://gitlab.redecorp.br/scqla'.upper(), url_value):
        oss_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'OSS_SCQLA', connection)
        if update_all:
            update_aux_filter2_value(aux_id, 'OSS_ALL', connection)
    elif re.search('https://gitlab.redecorp.br/serviceproblemmanagement'.upper(), url_value):
        oss_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'OSS_ServiceProblemManagement', connection)
        if update_all:
            update_aux_filter2_value(aux_id, 'OSS_ALL', connection)
    elif re.search('https://gitlab.redecorp.br/servicetestmanagement'.upper(), url_value):
        oss_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'OSS_ServiceTestManagement', connection)
        if update_all:
            update_aux_filter2_value(aux_id, 'OSS_ALL', connection)
    elif re.search('https://gitlab.redecorp.br/sigan'.upper(), url_value):
        oss_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'OSS_Sigan', connection)
        if update_all:
            update_aux_filter2_value(aux_id, 'OSS_ALL', connection)
    elif re.search('https://gitlab.redecorp.br/sigitm-2'.upper(), url_value):
        oss_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'OSS_Sigitm-2', connection)
        if update_all:
            update_aux_filter2_value(aux_id, 'OSS_ALL', connection)
    elif re.search('https://gitlab.redecorp.br/sigitm-3'.upper(), url_value):
        oss_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'OSS_Sigitm-3', connection)
        if update_all:
            update_aux_filter2_value(aux_id, 'OSS_ALL', connection)
    elif re.search('https://gitlab.redecorp.br/sigres-portal'.upper(), url_value):
        oss_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'OSS_SIGRES_PORTAL', connection)
        if update_all:
            update_aux_filter2_value(aux_id, 'OSS_ALL', connection)
    elif re.search('https://gitlab.redecorp.br/sigres-viewer'.upper(), url_value):
        oss_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'OSS_SIGRES_VIEWER', connection)
        if update_all:
            update_aux_filter2_value(aux_id, 'OSS_ALL', connection)
    elif re.search('https://gitlab.redecorp.br/sigres-2'.upper(), url_value):
        oss_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'OSS_sigres-2', connection)
        if update_all:
            update_aux_filter2_value(aux_id, 'OSS_ALL', connection)
    elif re.search('https://gitlab.redecorp.br/sigres-dm'.upper(), url_value):
        oss_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'OSS_sigres-dm', connection)
        if update_all:
            update_aux_filter2_value(aux_id, 'OSS_ALL', connection)
    elif re.search('https://gitlab.redecorp.br/smtx'.upper(), url_value):
        oss_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'OSS_smtx', connection)
        if update_all:
            update_aux_filter2_value(aux_id, 'OSS_ALL', connection)
    elif re.search('https://gitlab.redecorp.br/Star'.upper(), url_value):
        oss_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'OSS_Star', connection)
        if update_all:
            update_aux_filter2_value(aux_id, 'OSS_ALL', connection)
    elif re.search('https://gitlab.redecorp.br/talc'.upper(), url_value):
        oss_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'OSS_Talc', connection)
        if update_all:
            update_aux_filter2_value(aux_id, 'OSS_ALL', connection)
    elif re.search('https://gitlab.redecorp.br/TestSchemas'.upper(), url_value):
        oss_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'OSS_TestSchemas', connection)
        if update_all:
            update_aux_filter2_value(aux_id, 'OSS_ALL', connection)
    elif re.search('https://gitlab.redecorp.br/TopologyInventory'.upper(), url_value):
        oss_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'OSS_TopologyInventory', connection)
        if update_all:
            update_aux_filter2_value(aux_id, 'OSS_ALL', connection)
    elif re.search('https://gitlab.redecorp.br/oss'.upper(), url_value):
        oss_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'OSS_oss', connection)
        if update_all:
            update_aux_filter2_value(aux_id, 'OSS_ALL', connection)
    elif re.search('https://gitlab.redecorp.br/troubleticket'.upper(), url_value):
        oss_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'OSS_oss', connection)
        if update_all:
            update_aux_filter2_value(aux_id, 'OSS_ALL', connection)
    elif re.search('https://gitlab.redecorp.br/troubleticket'.upper(), url_value):
        oss_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'OSS_OUTROD', connection)
        if update_all:
            update_aux_filter2_value(aux_id, 'OSS_ALL', connection)
    elif re.search('https://gitlab.redecorp.br/sigres2'.upper(), url_value):
        oss_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'OSS_OUTROS', connection)
        if update_all:
            update_aux_filter2_value(aux_id, 'OSS_ALL', connection)
    elif re.search('https://gitlab.redecorp.br/sigitm3'.upper(), url_value):
        oss_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'OSS_OUTROS', connection)
        if update_all:
            update_aux_filter2_value(aux_id, 'OSS_ALL', connection)
    elif re.search('https://gitlab.redecorp.br/sigitmoss'.upper(), url_value):
        oss_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'OSS_OUTROS', connection)
        if update_all:
            update_aux_filter2_value(aux_id, 'OSS_ALL', connection)
    elif re.search('https://gitlab.redecorp.br/sigitm'.upper(), url_value):
        oss_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'OSS_OUTROS', connection)
        if update_all:
            update_aux_filter2_value(aux_id, 'OSS_ALL', connection)
    elif re.search('https://gitlab.redecorp.br/service-test-management'.upper(), url_value):
        oss_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'OSS_OUTROS', connection)
        if update_all:
            update_aux_filter2_value(aux_id, 'OSS_ALL', connection)
    elif re.search('https://gitlab.redecorp.br/resource-inventory'.upper(), url_value):
        oss_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'OSS_OUTROS', connection)
        if update_all:
            update_aux_filter2_value(aux_id, 'OSS_ALL', connection)
    elif re.search('https://gitlab.redecorp.br/number-inventory'.upper(), url_value):
        oss_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'OSS_OUTROS', connection)
        if update_all:
            update_aux_filter2_value(aux_id, 'OSS_ALL', connection)

    elif re.search('https://gitlab.redecorp.br/network-reallocation'.upper(), url_value):
        oss_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'OSS_OUTROS', connection)
        if update_all:
            update_aux_filter2_value(aux_id, 'OSS_ALL', connection)
    elif re.search('https://gitlab.redecorp.br/location-management'.upper(), url_value):
        oss_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'OSS_OUTROS', connection)
        if update_all:
            update_aux_filter2_value(aux_id, 'OSS_ALL', connection)
    elif re.search('https://gitlab.redecorp.br/diagnose-service-problem'.upper(), url_value):
        oss_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'OSS_OUTROS', connection)
        if update_all:
            update_aux_filter2_value(aux_id, 'OSS_ALL', connection)
    elif re.search('https://gitlab.redecorp.br/diagnose-service-problem'.upper(), url_value):
        oss_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'OSS_OUTROS', connection)
        if update_all:
            update_aux_filter2_value(aux_id, 'OSS_ALL', connection)
    elif re.search('https://gitlab.redecorp.br/network-reallocation'.upper(), url_value):
        oss_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'OSS_OUTROS', connection)
        if update_all:
            update_aux_filter2_value(aux_id, 'OSS_ALL', connection)


def check_integracao_dip_filter(url_value, item, data_metrics, connection):
    aux_id = item.get('ID')
    update = False
    aux = item.get('aux_filter_data')
    api_ok = False
    if aux is None:
        aux = ""
    if aux == "":
        update = True
    if re.search('https://gitlab.redecorp.br/business-partner-domain', url_value):
        integracao_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'DIP-Business-Partner-Domain', connection)
    elif re.search('https://gitlab.redecorp.br/common-domain', url_value):
        integracao_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'DIP-Common-Domain', connection)
    elif re.search('https://gitlab.redecorp.br/customer-domain', url_value):
        integracao_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'DIP-Customer-Domain', connection)
    elif re.search('https://gitlab.redecorp.br/enterprise-domain', url_value):
        integracao_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'DIP-Enterprise-Domain', connection)
    elif re.search('https://gitlab.redecorp.br/integration-domain', url_value):
        integracao_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'DIP-Integration-Domain', connection)
    elif re.search('https://gitlab.redecorp.br/market-sales-domain', url_value):
        integracao_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'DIP-Market-Sales-Domain', connection)
    elif re.search('https://gitlab.redecorp.br/product-domain', url_value):
        integracao_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'DIP-Product-Domain', connection)
    elif re.search('https://gitlab.redecorp.br/resource-domain', url_value):
        integracao_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'DIP-Resource-Domain', connection)
    elif re.search('https://gitlab.redecorp.br/service-domain', url_value):
        integracao_data_metrics_update(item, data_metrics, connection)
        if update:
            update_aux_filter_value(aux_id, 'DIP-Service-Domain', connection)
    elif re.search('https://gitlab.redecorp.br/osb/', url_value):
        integracao_data_metrics_update(item, data_metrics, connection)
        data_metrics.data_integra_all_osb.all_items.append(item)
        if update:
            update_aux_filter_value(aux_id, 'OSB', connection)
    elif re.search('https://gitlab.redecorp.br/soa/', url_value):
        integracao_data_metrics_update(item, data_metrics, connection)
        data_metrics.data_integra_all_soa.all_items.append(item)
        if update:
            update_aux_filter_value(aux_id, 'SOA', connection)
    elif re.search('https://gitlab.redecorp.br/api-management/', url_value):
        api_ok = True
    elif re.search('https://gitlab.redecorp.br/api-management-cloud/', url_value):
        api_ok = True
    elif re.search('https://gitlab.redecorp.br/api-management-onprem-internal/', url_value):
        api_ok = True
    elif re.search('https://gitlab.redecorp.br/api-management-onprem-files/', url_value):
        api_ok = True
    elif re.search('https://gitlab.redecorp.br/api-management-onprem-external/', url_value):
        api_ok = True
    elif re.search('https://gitlab.redecorp.br/api-management-onprem-files/', url_value):
        api_ok = True

    if api_ok:
        data_metrics.data_integra_all_api_gateway.all_items.append(item)
        integracao_data_metrics_update(item, data_metrics, connection)
        data_metrics.data_integra.all_items.append(item)
        it_acao = item.get(HEADER_SHAREPOINT[FIELD_SANIT_POS])
        data_metrics.data_oss.all_validados.append(item)
        if it_acao is None:
            it_acao = ""
        if it_acao == "":
            update_acao_pos_revisao_value(aux_id, 'Migrar', connection)


def check_qa_filter(url_value, item, data_metrics, connection):
    aux_id = item.get('ID')
    is_qa = False
    if re.search('https://gitlab.redecorp.br/cenarios-qa-automacao/', url_value):
        is_qa = True
    elif re.search('https://gitlab.redecorp.br/framework-qa-automacao/', url_value):
        is_qa = True
    elif re.search('https://gitlab.redecorp.br/portalautomacao/', url_value):
        is_qa = True
    elif re.search('https://gitlab.redecorp.br/servicos-virtuais/', url_value):
        is_qa = True
    elif re.search('/src/src-test-', url_value):
        is_qa = True
    elif re.search('/testes/', url_value):
        is_qa = True

    if is_qa:
        aux = item.get('aux_filter_data')
        if aux is None:
            aux = ""
        if aux == "":
            update_aux_filter_value(aux_id, 'QA', connection)


def check_gps_filter(url_value, item, data_metrics, connection):
    aux_id = item.get('ID')
    is_conv = False
    if re.search('https://gitlab.redecorp.br/conv86/', url_value):
        is_conv = True
    elif re.search('https://gitlab.redecorp.br/novoportalgf/', url_value):
        is_conv = True
    if is_conv:
        aux = item.get('aux_filter_data')
        if aux is None:
            aux = ""
        if aux == "":
            update_aux_filter_value(aux_id, 'CONV86', connection)


def check_conv86_filter(url_value, item, data_metrics, connection):
    aux_id = item.get('ID')
    is_gps = False
    if re.search('https://gitlab.redecorp.br/gps2/', url_value):
        is_gps = True

    if is_gps:
        aux = item.get('aux_filter_data')
        if aux is None:
            aux = ""
        if aux == "":
            update_aux_filter_value(aux_id, 'GPS', connection)


def check_4t_url(url_value, item, data_metrics, connection):
    aux_id = item.get('ID')
    is_4t = False
    if re.search('https://gitlab.redecorp.br/framework-brasil', url_value):
        is_4t = True
    elif re.search('https://gitlab.redecorp.br/fb-app-vivo', url_value):
        is_4t = True
    elif re.search('https://gitlab.redecorp.br/meuvivo-b2c-ecare', url_value):
        is_4t = True
    elif re.search('https://gitlab.redecorp.br/microservicos4p', url_value):
        is_4t = True
    elif re.search('https://gitlab.redecorp.br/MicroServicosMeuVivo', url_value):
        is_4t = True
    if is_4t:
        aux = item.get('aux_filter_data')
        if aux is None:
            aux = ""
        if aux == "":
            update_aux_filter_value(aux_id, '4T', connection)


def check_rpa_filter(url_value, item, data_metrics, connection):
    aux_id = item.get('ID')
    is_rpa = False
    if re.search('https://gitlab.redecorp.br/rpa-blue-prism/deploy/scr-', url_value):
        is_rpa = True
    elif re.search('https://gitlab.redecorp.br/rpa-blue-prism/deploy/deploy-', url_value):
        is_rpa = True
    if is_rpa:
        aux = item.get('aux_filter_data')
        if aux is None:
            aux = ""
        if aux == "":
            update_aux_filter_value(aux_id, 'RPA-BLUE-PRISM-', connection)


def check_4p_filter(url_value, item, data_metrics, connection):
    aux_id = item.get('ID')

    aux_filter = item.get('aux_filter_data')
    if aux_filter is None:
        aux_filter = ""
    if re.search('4T', aux_filter):
        validado = item.get('VALIDADO')
        data_metrics.data_plataforma.all_items.append(item)
        if validado:
            it_acao = item.get(HEADER_SHAREPOINT[FIELD_SANIT_POS])
            data_metrics.data_plataforma.all_validados.append(item)
            if it_acao is None:
                it_acao = 'Migrar'
            if re.search('Sanitizar', it_acao):
                data_metrics.data_plataforma.all_sanitizar.append(item)
            elif re.search('Migrar', it_acao):
                data_metrics.data_plataforma.all_migrar.append(item)
    else:
        check_4t_url(url_value, item, data_metrics, connection)


def check_diretor_filter(url_value, item, data_metrics, connection):
    aux_id = item.get('ID')
    aux_diretor = item.get('DiretoriadoOwner')
    if aux_diretor is None:
        aux_diretor = ""
    aux_diretor = aux_diretor.upper().strip()
    validado = item.get('VALIDADO')
    if re.search('Adriana Lika Shimomura'.upper(), aux_diretor) or re.search('Adriana Lika'.upper(), aux_diretor):
        data_metrics.dir_adriana_lika.all_items.append(item)
        if validado:
            it_acao = item.get(HEADER_SHAREPOINT[FIELD_SANIT_POS])
            data_metrics.dir_adriana_lika.all_validados.append(item)
            if it_acao is None:
                it_acao = 'Migrar'
            if re.search('Sanitizar', it_acao):
                data_metrics.dir_adriana_lika.all_sanitizar.append(item)
            elif re.search('Migrar', it_acao):
                data_metrics.dir_adriana_lika.all_migrar.append(item)
    elif re.search('Ana Lucia Gomes De Sa Drumond Pardo'.upper(), aux_diretor):
        data_metrics.dir_ana_lucia.all_items.append(item)
        if validado:
            it_acao = item.get(HEADER_SHAREPOINT[FIELD_SANIT_POS])
            data_metrics.dir_ana_lucia.all_validados.append(item)
            if it_acao is None:
                it_acao = 'Migrar'
            if re.search('Sanitizar', it_acao):
                data_metrics.dir_ana_lucia.all_sanitizar.append(item)
            elif re.search('Migrar', it_acao):
                data_metrics.dir_ana_lucia.all_migrar.append(item)
    elif re.search('Carla Beltrão'.upper(), aux_diretor):
        data_metrics.dir_carla_beltrao.all_items.append(item)
        if validado:
            it_acao = item.get(HEADER_SHAREPOINT[FIELD_SANIT_POS])
            data_metrics.dir_carla_beltrao.all_validados.append(item)
            if it_acao is None:
                it_acao = 'Migrar'
            if re.search('Sanitizar', it_acao):
                data_metrics.dir_carla_beltrao.all_sanitizar.append(item)
            elif re.search('Migrar', it_acao):
                data_metrics.dir_carla_beltrao.all_migrar.append(item)
    elif re.search('Fabio Shigueo Mori'.upper(), aux_diretor):
        data_metrics.dir_fabio_mori.all_items.append(item)
        if validado:
            it_acao = item.get(HEADER_SHAREPOINT[FIELD_SANIT_POS])
            data_metrics.dir_fabio_mori.all_validados.append(item)
            if it_acao is None:
                it_acao = 'Migrar'
            if re.search('Sanitizar', it_acao):
                data_metrics.dir_fabio_mori.all_sanitizar.append(item)
            elif re.search('Migrar', it_acao):
                data_metrics.dir_fabio_mori.all_migrar.append(item)
    elif re.search('Andre Dias Vitor Santos'.upper(), aux_diretor):
        data_metrics.dir_andre_santos.all_items.append(item)
        if validado:
            it_acao = item.get(HEADER_SHAREPOINT[FIELD_SANIT_POS])
            data_metrics.dir_andre_santos.all_validados.append(item)
            if it_acao is None:
                it_acao = 'Migrar'
            if re.search('Sanitizar', it_acao):
                data_metrics.dir_andre_santos.all_sanitizar.append(item)
            elif re.search('Migrar', it_acao):
                data_metrics.dir_andre_santos.all_migrar.append(item)
    elif re.search('Bruno de Moraes'.upper(), aux_diretor):
        data_metrics.dir_bruno_moraes.all_items.append(item)
        if validado:
            it_acao = item.get(HEADER_SHAREPOINT[FIELD_SANIT_POS])
            data_metrics.dir_bruno_moraes.all_validados.append(item)
            if it_acao is None:
                it_acao = 'Migrar'
            if re.search('Sanitizar', it_acao):
                data_metrics.dir_bruno_moraes.all_sanitizar.append(item)
            elif re.search('Migrar', it_acao):
                data_metrics.dir_bruno_moraes.all_migrar.append(item)
    elif re.search('Daniel Falbi'.upper(), aux_diretor):
        data_metrics.dir_daniel_falbi.all_items.append(item)
        if validado:
            it_acao = item.get(HEADER_SHAREPOINT[FIELD_SANIT_POS])
            data_metrics.dir_daniel_falbi.all_validados.append(item)
            if it_acao is None:
                it_acao = 'Migrar'
            if re.search('Sanitizar', it_acao):
                data_metrics.dir_daniel_falbi.all_sanitizar.append(item)
            elif re.search('Migrar', it_acao):
                data_metrics.dir_daniel_falbi.all_migrar.append(item)
    elif re.search('Fabio Stellato'.upper(), aux_diretor):
        data_metrics.dir_fabio_stellato.all_items.append(item)
        if validado:
            it_acao = item.get(HEADER_SHAREPOINT[FIELD_SANIT_POS])
            data_metrics.dir_fabio_stellato.all_validados.append(item)
            if it_acao is None:
                it_acao = 'Migrar'
            if re.search('Sanitizar', it_acao):
                data_metrics.dir_fabio_stellato.all_sanitizar.append(item)
            elif re.search('Migrar', it_acao):
                data_metrics.dir_fabio_stellato.all_migrar.append(item)
    elif re.search('Fernando Paes Campos'.upper(), aux_diretor):
        data_metrics.dir_fernando_campos.all_items.append(item)
        if validado:
            it_acao = item.get(HEADER_SHAREPOINT[FIELD_SANIT_POS])
            data_metrics.dir_fernando_campos.all_validados.append(item)
            if it_acao is None:
                it_acao = 'Migrar'
            if re.search('Sanitizar', it_acao):
                data_metrics.dir_fernando_campos.all_sanitizar.append(item)
            elif re.search('Migrar', it_acao):
                data_metrics.dir_fernando_campos.all_migrar.append(item)
    elif re.search('Gabriel Simoes Goncalves Da Silva'.upper(), aux_diretor):
        data_metrics.dir_gabriel_simioes.all_items.append(item)
        if validado:
            it_acao = item.get(HEADER_SHAREPOINT[FIELD_SANIT_POS])
            data_metrics.dir_gabriel_simioes.all_validados.append(item)
            if it_acao is None:
                it_acao = 'Migrar'
            if re.search('Sanitizar', it_acao):
                data_metrics.dir_gabriel_simioes.all_sanitizar.append(item)
            elif re.search('Migrar', it_acao):
                data_metrics.dir_gabriel_simioes.all_migrar.append(item)
    elif re.search('Giuliano Rodrigues Recco'.upper(), aux_diretor):
        data_metrics.dir_giuliano_recco.all_items.append(item)
        if validado:
            it_acao = item.get(HEADER_SHAREPOINT[FIELD_SANIT_POS])
            data_metrics.dir_giuliano_recco.all_validados.append(item)
            if it_acao is None:
                it_acao = 'Migrar'
            if re.search('Sanitizar', it_acao):
                data_metrics.dir_giuliano_recco.all_sanitizar.append(item)
            elif re.search('Migrar', it_acao):
                data_metrics.dir_giuliano_recco.all_migrar.append(item)
    elif re.search('Luis  Felipe  Jacobsen'.upper(), aux_diretor) or re.search('LUIS FELIPE JACOBSEN', aux_diretor):
        data_metrics.dir_luis_jacobsen.all_items.append(item)
        if validado:
            it_acao = item.get(HEADER_SHAREPOINT[FIELD_SANIT_POS])
            data_metrics.dir_luis_jacobsen.all_validados.append(item)
            if it_acao is None:
                it_acao = 'Migrar'
            if re.search('Sanitizar', it_acao):
                data_metrics.dir_luis_jacobsen.all_sanitizar.append(item)
            elif re.search('Migrar', it_acao):
                data_metrics.dir_luis_jacobsen.all_migrar.append(item)
    elif re.search('Nilson Franca Junior'.upper(), aux_diretor):
        data_metrics.dir_nilson_franca.all_items.append(item)
        if validado:
            it_acao = item.get(HEADER_SHAREPOINT[FIELD_SANIT_POS])
            data_metrics.dir_nilson_franca.all_validados.append(item)
            if it_acao is None:
                it_acao = 'Migrar'
            if re.search('Sanitizar', it_acao):
                data_metrics.dir_nilson_franca.all_sanitizar.append(item)
            elif re.search('Migrar', it_acao):
                data_metrics.dir_nilson_franca.all_migrar.append(item)
    elif re.search('Patricia Razzolini Orn'.upper(), aux_diretor):
        data_metrics.dir_patricia_orn.all_items.append(item)
        if validado:
            it_acao = item.get(HEADER_SHAREPOINT[FIELD_SANIT_POS])
            data_metrics.dir_patricia_orn.all_validados.append(item)
            if it_acao is None:
                it_acao = 'Migrar'
            if re.search('Sanitizar', it_acao):
                data_metrics.dir_patricia_orn.all_sanitizar.append(item)
            elif re.search('Migrar', it_acao):
                data_metrics.dir_patricia_orn.all_migrar.append(item)
    elif re.search('Tania De Araujo Azevedo'.upper(), aux_diretor):
        data_metrics.dir_tania_azevedo.all_items.append(item)
        if validado:
            it_acao = item.get(HEADER_SHAREPOINT[FIELD_SANIT_POS])
            data_metrics.dir_tania_azevedo.all_validados.append(item)
            if it_acao is None:
                it_acao = 'Migrar'
            if re.search('Sanitizar', it_acao):
                data_metrics.dir_tania_azevedo.all_sanitizar.append(item)
            elif re.search('Migrar', it_acao):
                data_metrics.dir_tania_azevedo.all_migrar.append(item)
    else:
        data_metrics.dir_sem_nome.all_items.append(item)
        if validado:
            it_acao = item.get(HEADER_SHAREPOINT[FIELD_SANIT_POS])
            data_metrics.dir_sem_nome.all_validados.append(item)
            if it_acao is None:
                it_acao = 'Migrar'
            if re.search('Sanitizar', it_acao):
                data_metrics.dir_sem_nome.all_sanitizar.append(item)
            elif re.search('Migrar', it_acao):
                data_metrics.dir_sem_nome.all_migrar.append(item)


def export_csv_revisados(data_metrics, connection):
    data_str = datetime.today().strftime('%d_%m_%Y')
    hora_str = datetime.today().strftime('%H_%M_%S')
    path_vivo = "c:/vivo/"
    file_name = path_vivo + "revisados_integra_" + data_str + "_" + hora_str + ".csv"
    generate_csv_file(file_name, data_metrics.data_integra.all_validados)

    file_name = path_vivo + "revisados_b2b_" + data_str + "_" + hora_str + ".csv"
    generate_csv_file(file_name, data_metrics.data_b2b.all_validados)
    file_name = path_vivo + "revisados_loja_b2c_" + data_str + "_" + hora_str + ".csv"
    generate_csv_file(file_name, data_metrics.data_loja_b2c.all_validados)
    file_name = path_vivo + "revisados_hub_pag_" + data_str + "_" + hora_str + ".csv"
    generate_csv_file(file_name, data_metrics.data_hub_pag.all_validados)
    file_name = path_vivo + "revisados_plataforma_4P_" + data_str + "_" + hora_str + ".csv"
    generate_csv_file(file_name, data_metrics.data_plataforma.all_validados)


def export_csv_integracoes_apis(data_metrics, connection):
    data_str = datetime.today().strftime('%d_%m_%Y')
    hora_str = datetime.today().strftime('%H_%M_%S')
    path_vivo = "c:/vivo/integracoes/"
    file_name = path_vivo + "integracoes_all_api_gateway_" + data_str + "_" + hora_str + ".csv"
    generate_integracoes_all_api_gateway_csv_file(file_name, data_metrics.data_integra_all_api_gateway.all_items)
    file_name = path_vivo + "integracoes_all_osb_" + data_str + "_" + hora_str + ".csv"
    generate_integracoes_all_api_gateway_csv_file(file_name, data_metrics.data_integra_all_osb.all_items)
    file_name = path_vivo + "integracoes_all_soa_" + data_str + "_" + hora_str + ".csv"
    generate_integracoes_all_api_gateway_csv_file(file_name, data_metrics.data_integra_all_soa.all_items)


def import_new_gitlab_itens(connection):
    df_new_itens = pd.read_csv('c:/vivo/import/devops_migração/novos_itens_ate_09_11_2023.csv')
    df_sharepoint = pd.read_csv("c:/Temp/list_all_git_url.csv")
    df_sharepoint.set_index('UrldoGIT', inplace=True)
    items_conflito = []
    sem_conflito = True
    for index, item in df_new_itens.iterrows():
        urldoGIT = item.get("UrldoGIT")
        result = df_sharepoint.query("UrldoGIT == @urldoGIT")
        if len(result.index) > 0:
            print("URL GIT JA existente " + urldoGIT)
            items_conflito.append(item)
            sem_conflito = False
    if sem_conflito:
        import_list_itens(df_new_itens, connection, True)

    print("FIM")


def getUrlGit(itemData, df):
    if itemData is None or itemData == "":
        return False
    else:
        dfx = df.loc[df["URL_DO_SCM"] == itemData]
        return len(dfx) > 0


def is_qa_filter(url_value):
    is_qa = False
    if re.search('https://gitlab.redecorp.br/cenarios-qa-automacao/', url_value):
        is_qa = True
    elif re.search('https://gitlab.redecorp.br/framework-qa-automacao/', url_value):
        is_qa = True
    elif re.search('https://gitlab.redecorp.br/portalautomacao/', url_value):
        is_qa = True
    elif re.search('https://gitlab.redecorp.br/servicos-virtuais/', url_value):
        is_qa = True
    elif re.search('/src/src-test-', url_value):
        is_qa = True
    elif re.search('/testes/', url_value):
        is_qa = True
    return is_qa


def getUrlGitItem(itemData, df):
    dfx = df.loc[df["URL_DO_SCM"] == itemData]
    return dfx


def check_arquivo_massivo_test_qa(connection):
    df_massivo = pd.read_csv("c:/vivo/import/massivo/oss_massivo1.csv", encoding='iso-8859-1')
    df_filtro = pd.read_csv("c:/vivo/import/massivo/oss_massivo1.csv")
    df_sharepoint = pd.read_csv("c:/vivo/import/massivo/relatorio_gerencial_31_10_2023_10_22_28.csv")
    for index, item in df_filtro.iterrows():
        urldoGIT = item.get("URL_DO_SCM")
        print(urldoGIT)
        if urldoGIT is not None and urldoGIT != "":
            itsf = getUrlGitItem(urldoGIT, df_sharepoint)
            for index2, item2 in itsf.iterrows():
                id = str(int(item2.get("ID")))
                update_aux_toke2_value(id, "S_MASSIVA", connection)
                print(id)
    cont_qa = 0
    for index2, item2 in df_massivo.iterrows():
        url = item2.get("URL_DO_SCM")
        is_qa = is_qa_filter(url)
        if is_qa:
            cont_qa = cont_qa + 1
            df_massivo.at[index2, 'IS_QA'] = 'TRUE'
        else:
            df_massivo.at[index2, 'IS_QA'] = 'FALSE'

    df_massivo.to_csv("c:/vivo/import/massivo/massivo1_qa2.csv")
    print("FIM")


def diff_sharepoint_x_meg_itens(connection):
    df_meg = pd.read_csv("c:/vivo/integracoes/todos_os_itens_ate_24_10_2023_meg.csv")
    df_sharepoint = pd.read_csv("c:/vivo/integracoes/relatorio_gerencial_24_10_2023_08_55_47.csv")
    items_conflito = []
    for index, item in df_sharepoint.iterrows():
        urldoGIT = item.get("URL_DO_SCM")
        print(urldoGIT)
        if not getUrlGit(urldoGIT, df_meg):
            items_conflito.append(item)
            id = item.get("ID")

    print("FIM")


def diff_meg_x_sharepoint_itens(connection):
    df_meg = pd.read_csv("c:/vivo/integracoes/todos_os_itens_ate_24_10_2023_meg.csv")
    df_sharepoint = pd.read_csv("c:/vivo/integracoes/relatorio_gerencial_24_10_2023_08_55_47.csv")
    items_conflito = []
    for index, item in df_meg.iterrows():
        urldoGIT = item.get("URL_DO_SCM")
        print(urldoGIT)
        if not getUrlGit(urldoGIT, df_sharepoint):
            items_conflito.append(item)
            id = item.get("ID")

    print("FIM")


def diff_sharepoint_x_gitlab_itens(connection):
    df_gitlab = pd.read_csv("c:/vivo/integracoes/gitlab.csv")
    df_sharepoint = pd.read_csv("c:/vivo/integracoes/relatorio_gerencial_23_10_2023_14_57_34.csv")
    items_conflito = []
    for index, item in df_sharepoint.iterrows():
        urldoGIT = item.get("URL_DO_SCM")
        print(urldoGIT)
        if not getUrlGit(urldoGIT, df_gitlab):
            items_conflito.append(item)
            id = item.get("ID")
            aux_scm = item.get("aux_url_scm")
            if aux_scm is None or aux_scm == "":
                update_aux_filter2_value(id, "NOT_FOUND", connection)
            else:
                print("JA EXISTE aux_url_scm value " + aux_scm)

    print("FIM")


def diff_gitlab_x_sharepoint_itens(connection):
    df_gitlab = pd.read_csv("c:/vivo/integracoes/gitlab.csv")
    df_sharepoint = pd.read_csv("c:/vivo/integracoes/relatorio_gerencial_23_10_2023_14_57_34.csv")
    items_conflito = []
    for index, item in df_gitlab.iterrows():
        urldoGIT = item.get("URL_DO_SCM")
        print(urldoGIT)
        if not getUrlGit(urldoGIT, df_sharepoint):
            items_conflito.append(item)
            id = item.get("ID")

    print("FIM")


def import_update_itens_integracao(connection):
    df_new_itens = pd.read_csv('c:/vivo/integracoes/integracoes_all_soa_24_10_2023_12_18_13.csv', keep_default_na=False)
    cont = 0
    for index, item in df_new_itens.iterrows():
        cont = cont + 1
        p_id = item.get("ID")
        print(str(p_id) + " " + str(cont))
        v_acao = item.get("A_x00e7__x00e3_oap_x00f3_sRevis_")
        if v_acao is None or v_acao == "":
            update_row_integracao(p_id, item, connection)
    print("FIM")


def import_update_diretor(connection):
    df_new_itens = pd.read_csv('c:/vivo/import/devops_migração/atualizacao_de_itens_ate_26_09_2023.csv',
                               keep_default_na=False)
    df_sharepoint = pd.read_csv("c:/vivo/dir_sem_nome_27_09_2023_14_32_21.csv", keep_default_na=False)

    # df_sharepoint.set_index('ID', inplace=True)
    # id_not_found = []
    # for index, item in df_sharepoint.iterrows():
    #     p_id = index
    #     result = df_new_itens.query("ID == @p_id")
    #     if len(result.index) <= 0:
    #         id_not_found.append(item)

    for index, item in df_new_itens.iterrows():
        p_id = item.get("ID")
        diretor = item.get("DiretoriadoOwner")
        update_diretor_value(p_id, diretor, connection)
    print("FIM")


def import_update_status_migracao(connection):
    df_new_itens = pd.read_csv('c:/vivo/import/devops_migração/hub_sanitizados_2.csv',
                               keep_default_na=False, encoding='iso-8859-1')
    for index, item in df_new_itens.iterrows():
        p_id = item.get("ID")
        if p_id is not None:
            v_status = "Sanitizado"
            update_status_migracao_value(p_id, v_status, connection)
    print("FIM")


def getItemValue(itemData):
    if itemData is None:
        return ""
    else:
        return itemData


def getItemAFValue(itemData, df_af):
    if itemData is None or itemData == "":
        return ""
    else:
        dfx = df_af.loc[df_af["ID"] == itemData]
        if len(dfx) > 0:
            return dfx.iloc[0]['NOME']
        else:
            return ""


def getItemAFId(itemData, df_af):
    if itemData is None or itemData == "":
        return ""
    else:
        dfx = df_af.loc[df_af["NOME"] == itemData]
        if len(dfx) > 0:
            return dfx.iloc[0]['ID']
        else:
            return 0


def import_update_gitlab_itens_oss_x(connection):
    df_new_itens = pd.read_csv('c:/vivo/import/OSS/oss_ajustado.csv', encoding='iso-8859-1', keep_default_na=False)
    df_sharepoint = pd.read_csv("c:/Temp/list_all_git_url.csv")
    df_af = pd.read_csv('c:/vivo/report/af_aplicacoes_id_para_nome_do_ic_11_10_2023 1.csv')
    df_sharepoint.set_index('UrldoGIT', inplace=True)
    items_not_found = []
    found_all = True
    for index, item in df_new_itens.iterrows():
        urldoGIT = item.get("UrldoGIT")
        if urldoGIT is not None:
            result = df_sharepoint.query("UrldoGIT == @urldoGIT")
            if len(result.index) <= 0:
                print("URL GIT not found " + urldoGIT)
                items_not_found.append(item)
                found_all = False
            else:
                af_cod = getItemAFId(getItemValue(item.get("Sistema_x0028_AF_x0029_Id")), df_af)
                if af_cod is None or af_cod == '':
                    af_cod = 0
                item["Sistema_x0028_AF_x0029_Id"] = int(af_cod)
                if item["TipodeBuild"] is None:
                    item["TipodeBuild"] = ""
            update_oss_row(item, connection)

    print("FIM")


def getById(itemData, df):
    if itemData is None or itemData == "":
        return False
    else:
        dfx = df.loc[df["ID"] == int(itemData)]
        return len(dfx) > 0


def import_update_gitlab_itens_oss(connection):
    df_new_itens = pd.read_csv('c:/vivo/import/OSS/oss_ajustado.csv', encoding='iso-8859-1', keep_default_na=False)
    df_sharepoint = pd.read_csv("c:/vivo/import/OSS/relatorio_gerencial_16_11_2023_10_42_43.csv")
    df_af = pd.read_csv('c:/vivo/import/OSS/af_aplicacoes_id_para_nome_do_ic_11_10_2023 1.csv')
    items_not_found = []
    found_all = True
    for index, item in df_new_itens.iterrows():
        found_item = True
        urldoGIT = item.get("UrldoGIT")
        s_af = item.get("Sistema_x0028_AF_x0029_Id")
        id_oss = item.get("ID")
        if urldoGIT is not None and len(urldoGIT.strip()) > 0:
            if not getById(id_oss, df_sharepoint):
                print("URL GIT not found " + urldoGIT)
                items_not_found.append(item)
                found_all = False
                found_item = False
            else:
                af_cod_int = -1
                af_cod = getItemAFId(getItemValue(s_af), df_af)
                if af_cod is None or af_cod == '':
                    af_cod_int = -1
                else:
                    try:
                        af_cod_int = int(af_cod)
                    except ValueError:
                        af_cod_int = -1
                item["Sistema_x0028_AF_x0029_Id"] = af_cod_int
                aux_acao = item.get("A_x00e7__x00e3_oap_x00f3_sRevis_")
                if aux_acao is not None:
                    aux_acao = aux_acao.strip()
                item["A_x00e7__x00e3_oap_x00f3_sRevis_"] = aux_acao
                if item["TipodeBuild"] is None:
                    item["TipodeBuild"] = ""

            try:
                max_retries = 5
                for retry in range(1, max_retries + 1):
                    try:
                        connection.get_sharepoint_digest()
                        if found_item:
                            update_oss_row(item, connection)
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
                                f"Falha ao Atualizar {urldoGIT}. Erro {status_code}.")
                            connection.logger.error(err)
                            return
            except Exception as err:
                connection.logger.error(err)
                return
            connection.logger.info(urldoGIT + "atualizada   com sucesso.")

    print("FIM")


def new_rel_Ger_item(item, df_af):
    new_item = {}
    new_item["ID"] = item.get("Id")
    new_item["NOME_DO_COMPONENTE"] = getItemValue(item.get("Title"))
    new_item["DESCRICAO_COMPONENTE"] = getItemValue(item.get("NomedoComponente"))
    new_item["SIGLA_ARQ_FUTURO"] = getItemAFValue(getItemValue(item.get("Sistema_x0028_AF_x0029_Id")), df_af)
    new_item["TIPO_DO_SCM"] = getItemValue(item.get("TipoFerramentaSCM_x0028_Controle"))
    new_item["URL_DO_SCM"] = getItemValue(item.get("UrldoGIT"))
    new_item["CRIACAO"] = getItemValue(item.get("Created"))
    new_item["ATUALIZACAO"] = getItemValue(item.get("DatadeAtualiza_x00e7__x00e3_o"))
    new_item["REPOSITORIO_DE_CI"] = getItemValue(item.get("CIServer"))
    new_item["TECNOLOGIA"] = getItemValue(item.get("Linguagem"))
    new_item["TIPO_DO_BUILD"] = getItemValue(item.get("TipodeBuild"))
    new_item["URL_DO_PIPELINE"] = getItemValue(item.get("URLPipeline"))
    new_item["ACAO_APOS_REVISAO"] = getItemValue(item.get("A_x00e7__x00e3_oap_x00f3_sRevis_"))
    new_item["EMAIL_PONTO_FOCAL_MIGRACAO"] = getItemValue(item.get("PontoFocaldaMigra_x00e7__x00e3_o"))
    new_item["OWNER"] = getItemValue(item.get("Owner"))
    new_item["GERENCIA_SR_DO_OWNER"] = getItemValue(item.get("GerenciaSrdoOwner"))
    new_item["DIRETORIA_DO_OWNER"] = getItemValue(item.get("DiretoriadoOwner"))
    new_item["OBSERVACOES"] = getItemValue(item.get("OBSERVA_x00c7__x00d5_ES"))
    new_item["VALIDADO"] = getItemValue(item.get("VALIDADO"))
    new_item["SISTEMA_GITLAB"] = getItemValue(item.get("SistemaGitlab"))
    new_item["STATUS_MIGRACAO"] = getItemValue(item.get("StatusdaMigra_x00e7__x00e3_o"))
    new_item["TOKEN_FILTRO"] = getItemValue(item.get("aux_filter_data"))
    new_item["CRONOGRAMA"] = getItemValue(item.get("cronograma"))
    status_gitlab = getItemValue(item.get("aux_url_scm"))
    if status_gitlab is None or status_gitlab == "":
        status_gitlab = "OK"
    new_item["STATUS_GITLAB"] = status_gitlab
    return new_item


def get_all_sharepoint_list_items(data_metrics, connection):
    api_url = f"{connection.site_url}/_api/web/lists/getbytitle('{connection.list_name}')/items"

    headers = {
        "accept": "application/json;odata=verbose",
        "X-RequestDigest": connection.digest_header
    }
    df_report_gerencial = pd.read_csv('c:/vivo/import/devops_migração/relatorio_consolidado_dados_migracao_format.csv')
    df_af = pd.read_csv('c:/vivo/report/af_aplicacoes_id_para_nome_do_ic_11_10_2023 1.csv')
    count = 0
    all_items = []
    while True:
        response1 = requests.get(api_url, cookies=connection.cookies, headers=headers)
        url_repo_internal_field = HEADER_SHAREPOINT[FIELD_URL_REPO]

        if response1.status_code == 200:
            data = response1.json()["d"]["results"]
            count += len(data)
            for item in data:
                id = item['__metadata']['id']
                url_value = item.get(url_repo_internal_field)
                if url_value is not None:
                    url_value = url_value["Url"]
                    item[url_repo_internal_field] = url_value
                    all_items.append(item)
                data_metrics.all_items.append(item)
                check_oss_filter(url_value, item, data_metrics, connection)
                check_oss_other_filter(url_value, item, data_metrics, connection)
                check_integracao_dip_filter(url_value, item, data_metrics, connection)
                # check_hub_pagamentos_filter(url_value, item, data_metrics, connection)
                check_b2b_filter(url_value, item, data_metrics, connection)
                # check_loja_online_filter(url_value, item, df_lojaonline, data_metrics, connection)
                check_4p_filter(url_value, item, data_metrics, connection)
                # check_rpa_filter(url_value, item, data_metrics, connection)
                check_qa_filter(url_value, item, data_metrics, connection)
                check_gps_filter(url_value, item, data_metrics, connection)
                check_conv86_filter(url_value, item, data_metrics, connection)
                # check_diretor_filter(url_value, item, data_metrics, connection)
                new_item = new_rel_Ger_item(item, df_af)
                df2 = pd.DataFrame.from_dict([new_item])
                df_report_gerencial = pd.concat([df_report_gerencial, df2])
                connection.logger.info(f"{count} itens processados .")
            next_link = response1.json()["d"].get("__next")
            if next_link:
                api_url = next_link
            else:
                break
        else:
            raise requests.exceptions.HTTPError(f"Falha ao recuperar itens. {response.status_code}", response=response)
    data_now = datetime.today()
    data_str = data_now.strftime('%d_%m_%Y')
    hora_str = data_now.strftime('%H_%M_%S')
    name_file = "relatorio_gerencial_" + data_str + "_" + hora_str + ".csv"
    cols = ["ID", "SISTEMA_GITLAB", "NOME_DO_COMPONENTE", "DESCRICAO_COMPONENTE", "SIGLA_ARQ_FUTURO", "TIPO_DO_SCM",
            "URL_DO_SCM", "CRIACAO", "ATUALIZACAO", "REPOSITORIO_DE_CI", "TECNOLOGIA", "TIPO_DO_BUILD",
            "URL_DO_PIPELINE", "ACAO_APOS_REVISAO", "EMAIL_PONTO_FOCAL_MIGRACAO", "OWNER", "GERENCIA_SR_DO_OWNER",
            "DIRETORIA_DO_OWNER", "OBSERVACOES", "VALIDADO", "STATUS_MIGRACAO", "STATUS_GITLAB",
            "TOKEN_FILTRO", "CRONOGRAMA"]
    df_report_gerencial.to_csv("c:/vivo/report/" + name_file, columns=cols)
    export_csv_revisados(data_metrics, connection)
    export_csv_integracoes_apis(data_metrics, connection)
    # generate_csv_file('c:/Temp/diego_lima.csv', df_acao_not_valided)
    # generate_csv_file('c:/Temp/validados.csv', data_metrics.all_items_validados)
    # generate_csv_file('c:/Temp/not_validados.csv', data_metrics.all_items_not_validados)
    # df_all_l = pd.DataFrame(data_metrics.all_loja_online)
    list_not_in = []
    # list_all = df_lojaonline['URL_GIT'].tolist()
    # for item in list_all:
    #    if item not in df_all_l['UrldoGIT'].unique():
    #        list_not_in.append(item)
    # aux_df = pd.DataFrame(list_not_in)
    # aux_df.to_csv('c:/Temp/loja_not_found.csv')
    return data_metrics.all_items


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


def update_sharepoint_list_row(row, connection):
    item_id = row.get("ID")
    if not str(item_id).isdigit():
        raise Exception(
            f"Falha ao atualizar o row {item_id}. Id do item inválido.")

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

    # "UrldoGIT": row.get("UrldoGIT"),

    data.update({"SistemaGitlab": row.get("SistemaGitlab"),
                 "Linguagem": row.get('Linguagem'),
                 "TipodeBuild": row.get("TipodeBuild"),
                 "URLPipeline": row.get("URLPipeline"),
                 "A_x00e7__x00e3_oap_x00f3_sRevis_": row.get("A_x00e7__x00e3_oap_x00f3_sRevis_"),
                 "PontoFocaldaMigra_x00e7__x00e3_o": row.get("PontoFocaldaMigra_x00e7__x00e3_o"),
                 "Owner": row.get("Owner"),
                 "GerenciaSrdoOwner": row.get("GerenciaSrdoOwner"),
                 "DiretoriadoOwner": row.get("DiretoriadoOwner"),
                 "ORDEMPRIORIDADE": row.get("ORDEMPRIORIDADE"),
                 "VALIDADO": "False",
                 "OBSERVA_x00c7__x00d5_ES": row.get("OBSERVA_x00c7__x00d5_ES")})

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


def update_oss_row(row, connection):
    item_id = int(row.get("ID"))

    if not str(item_id).isdigit():
        raise Exception(
            f"Falha ao atualizar o row {item_id}. Id do item inválido.")

    api_url = f"{connection.site_url}/_api/web/lists/getbytitle('{connection.list_name}')/items({item_id})"

    # Cabeçalhos para a requisição POST
    headers = {
        "Accept": "application/json;odata=verbose",
        "Content-Type": "application/json;odata=verbose",
        "X-HTTP-Method": "MERGE",
        "IF-MATCH": "*",
        "charset": "'iso-8859-1'",
        "X-RequestDigest": connection.digest_header,
    }

    # Dados para atualizar um novo item
    data = {
        "__metadata": {
            "type": f"SP.Data.{connection.list_name_for_create}ListItem"
        },
    }

    # "UrldoGIT": row.get("UrldoGIT"),

    v_af = row.get("Sistema_x0028_AF_x0029_Id")
    has_af = False
    if v_af is not None:
        v_af_cod = int(v_af)
        if v_af_cod > 0:
            has_af = True
    if has_af:
        xdata = {"SistemaGitlab": row.get("SistemaGitlab"),
                 "Linguagem": row.get('Linguagem'),
                 "TipodeBuild": row.get("TipodeBuild"),
                 "URLPipeline": row.get("URLPipeline"),
                 "CIServer": row.get("CIServer"),
                 "A_x00e7__x00e3_oap_x00f3_sRevis_": row.get("A_x00e7__x00e3_oap_x00f3_sRevis_"),
                 "PontoFocaldaMigra_x00e7__x00e3_o": row.get("PontoFocaldaMigra_x00e7__x00e3_o"),
                 "ICdoCMDB": row.get("ICdoCMDB"),
                 "Sistema_x0028_AF_x0029_Id": int(row.get("Sistema_x0028_AF_x0029_Id")),
                 "Owner": row.get("Owner"),
                 "GerenciaSrdoOwner": row.get("GerenciaSrdoOwner"),
                 "DiretoriadoOwner": row.get("DiretoriadoOwner"),
                 "VALIDADO": "True",
                 "OBSERVA_x00c7__x00d5_ES": row.get("OBSERVA_x00c7__x00d5_ES")}
    else:
        xdata = {"SistemaGitlab": row.get("SistemaGitlab"),
                 "Linguagem": row.get('Linguagem'),
                 "TipodeBuild": row.get("TipodeBuild"),
                 "URLPipeline": row.get("URLPipeline"),
                 "CIServer": row.get("CIServer"),
                 "A_x00e7__x00e3_oap_x00f3_sRevis_": row.get("A_x00e7__x00e3_oap_x00f3_sRevis_"),
                 "PontoFocaldaMigra_x00e7__x00e3_o": row.get("PontoFocaldaMigra_x00e7__x00e3_o"),
                 "ICdoCMDB": row.get("ICdoCMDB"),
                 "Owner": row.get("Owner"),
                 "GerenciaSrdoOwner": row.get("GerenciaSrdoOwner"),
                 "DiretoriadoOwner": row.get("DiretoriadoOwner"),
                 "VALIDADO": "True",
                 "OBSERVA_x00c7__x00d5_ES": row.get("OBSERVA_x00c7__x00d5_ES")}

    data.update(xdata)

    response = requests.post(
        api_url, headers=headers, json=data, cookies=connection.cookies)

    if response.status_code == 204:
        connection.logger.info(
            f"Campo {item_id} do item atualizado com sucesso. <{response.status_code}.>")
    else:
        connection.logger.error(response.text)
        raise requests.exceptions.HTTPError(
            f"Falha ao atualizar o   id {item_id}. Erro {response.status_code}.",
            response=response)


def update_integracao_row(row, connection):
    item_id = row.get("ID")
    connection.logger.info("item {item_id}")
    if not str(item_id).isdigit():
        raise Exception(
            f"Falha ao atualizar o row {item_id}. Id do item inválido.")

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

    obs = "Atualizado com autorização da gestão do time de integrações (por e-mail) "
    data.update({"A_x00e7__x00e3_oap_x00f3_sRevis_": "Migrar",
                 "VALIDADO": "False",
                 "StatusdaMigra_x00e7__x00e3_o": "",
                 "OBSERVA_x00c7__x00d5_ES": obs})

    response = requests.post(api_url, headers=headers, json=data, cookies=connection.cookies)

    if response.status_code == 204:
        connection.logger.info("item atualizado com sucesso. <{response.status_code}.>")
    else:
        connection.logger.error(response.text)
        raise requests.exceptions.HTTPError(
            f"Falha ao atualizar o campo {item_id}. Erro {response.status_code}.",
            response=response)


def update_row_integracao(p_id, item, connection):
    if p_id != -1:
        try:
            max_retries = 5
            for retry in range(1, max_retries + 1):
                try:
                    connection.get_sharepoint_digest()

                    update_integracao_row(item, connection)
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
                            f"Falha ao Atualizar item. Erro {status_code}.")
                        connection.logger.error(err)
                        return
        except Exception as err:
            connection.logger.error(err)
            return
        connection.logger.info("Campo atualizado com sucesso.")


def update_row(p_id, vDiretor, connection):
    if p_id != -1:
        try:
            max_retries = 5
            for retry in range(1, max_retries + 1):
                try:
                    connection.get_sharepoint_digest()
                    update_sharepoint_list_row(item, connection)
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
                            f"Falha ao Atualizar item. Erro {status_code}.")
                        connection.logger.error(err)
                        return
        except Exception as err:
            connection.logger.error(err)
            return
        connection.logger.info("Campo atualizado com sucesso.")


def teste_resumo_csv(csv_file_path, connection):
    dataMetrics = DataMetrics(connection.logger)
    dataMetrics.log_resume(False)


def generate_sharepoint_list_all_items_csv(csv_file_path, connection):
    dataMetrics = DataMetrics(connection.logger)

    df_sharepoint = pd.DataFrame(get_all_sharepoint_list_items(dataMetrics, connection))
    connection.logger.debug(f'{len(df_sharepoint.index)} itens recuperados do sharepoint.')
    connection.logger.debug(df_sharepoint.columns)
    cols = ["Sistema_x0028_AF_x0029_Id", "SistemaGitlab",
            "DiretoriadoOwner", "GerenciaSrdoOwner",
            "Owner", "Linguagem", "Title", "TipodeBuild",
            "CIServer", "TipoFerramentaSCM_x0028_Controle",
            "DatadeAtualiza_x00e7__x00e3_o", "UrldoGIT",
            "URLPipeline", "StatusdaMigra_x00e7__x00e3_o"]

    df_sharepoint.to_csv(csv_file_path, columns=cols)
    df_sharepoint.to_csv("c:/Temp/sharepoint_full_bck.csv")

    connection.logger.debug(csv_file_path + " gerado")
    dataMetrics.log_resume(True)
