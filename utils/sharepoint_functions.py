import requests
import pandas as pd
import re
from .parameters import *
from .data_metrics import *


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
    if p_id == -1:
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


def check_hub_pagamentos_filter(url_value, item, data_metrics, connection):
    aux_id = item.get('ID')
    validado = item.get('VALIDADO')
    if re.search('https://gitlab.redecorp.br/hubdepagamento', url_value):
        data_metrics.data_hub_pag.all_items.append(item)
        if validado:
            data_metrics.data_hub_pag.all_validados.append(item)
            it_acao = item.get(HEADER_SHAREPOINT[FIELD_SANIT_POS])
            if it_acao is None:
                it_acao = 'Migrar'
            if re.search('Sanitizar', it_acao):
                data_metrics.data_hub_pag.all_sanitizar.append(item)
            elif re.search('Migrar', it_acao):
                data_metrics.data_hub_pag.all_migrar.append(item)
        # update_aux_filter_value(aux_id, 'HUB_PAGAMENTO', connection)


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

            # update_aux_filter_value(aux_id, 'HUB_PAGAMENTO', connection)


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


def check_integracao_dip_filter(url_value, item, data_metrics, connection):
    aux_id = item.get('ID')
    validado = item.get('VALIDADO')
    if re.search('https://gitlab.redecorp.br/business-partner-domain', url_value):
        integracao_data_metrics_update(item, data_metrics, connection)
        # update_aux_filter_value(aux_id, 'DIP-Business-Partner-Domain', connection)
    elif re.search('https://gitlab.redecorp.br/common-domain', url_value):
        integracao_data_metrics_update(item, data_metrics, connection)
        # update_aux_filter_value(aux_id, 'DIP-Common-Domain', connection)
    elif re.search('https://gitlab.redecorp.br/customer-domain', url_value):
        integracao_data_metrics_update(item, data_metrics, connection)
        # update_aux_filter_value(aux_id, 'DIP-Customer-Domain', connection)
    elif re.search('https://gitlab.redecorp.br/enterprise-domain', url_value):
        integracao_data_metrics_update(item, data_metrics, connection)
        # update_aux_filter_value(aux_id, 'DIP-Enterprise-Domain', connection)
    elif re.search('https://gitlab.redecorp.br/integration-domain', url_value):
        integracao_data_metrics_update(item, data_metrics, connection)
        # update_aux_filter_value(aux_id, 'DIP-Integration-Domain', connection)
    elif re.search('https://gitlab.redecorp.br/market-sales-domain', url_value):
        integracao_data_metrics_update(item, data_metrics, connection)
        # update_aux_filter_value(aux_id, 'DIP-Market-Sales-Domain', connection)
    elif re.search('https://gitlab.redecorp.br/product-domain', url_value):
        integracao_data_metrics_update(item, data_metrics, connection)
        # update_aux_filter_value(aux_id, 'DIP-Product-Domain', connection)
    elif re.search('https://gitlab.redecorp.br/resource-domain', url_value):
        integracao_data_metrics_update(item, data_metrics, connection)
        # update_aux_filter_value(aux_id, 'DIP-Resource-Domain', connection)
    elif re.search('https://gitlab.redecorp.br/service-domain', url_value):
        integracao_data_metrics_update(item, data_metrics, connection)
        # update_aux_filter_value(aux_id, 'DIP-Service-Domain', connection)
    # elif re.search('https://gitlab.redecorp.br/osb', url_value):
    #    integracao_data_metrics_update(item, data_metrics, connection)
    # update_aux_filter_value(aux_id, 'OSB', connection)


# elif re.search('https://gitlab.redecorp.br/soa', url_value):
#     integracao_data_metrics_update(item, data_metrics, connection)
# update_aux_filter_value(aux_id, 'SOA', connection)
# elif re.search('https://gitlab.redecorp.br/api-management/src-77', url_value):
#     integracao_data_metrics_update(item, data_metrics, connection)

# update_aux_filter_value(aux_id, 'API-Gateway', connection)


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
        update_aux_filter_value(aux_id, '4T', connection)


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
    # check_4t_url(url_value, item, data_metrics, connection)

def check_diretor_filter(url_value, item, data_metrics, connection):
    aux_id = item.get('ID')
    aux_diretor  = item.get('DiretoriadoOwner')
    if aux_diretor is None:
        aux_diretor = ""
    aux_diretor =  aux_diretor.upper()
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
    elif re.search('LUIS FELIPE JACOBSEN'.upper(), aux_diretor):
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


def import_new_gitlab_itens(connection):
    df_new_itens = pd.read_csv('c:/Temp/novos_itens_ate_14_09_2023.csv')
    df_sharepoint = pd.read_csv("c:/Temp/list_all_git_url2.csv")

    df_sharepoint.set_index('url_git', inplace=True)

    # search for rows with index value 'Bob'

    for index, item in df_new_itens.iterrows():
        urldoGIT = item.get("UrldoGIT")
        result = df_sharepoint.query("url_git == @urldoGIT")
        if len(result.index) > 0:
            print("URL GIT JA existente " + urldoGIT)
        else:
            print("incluir  " + urldoGIT)
    print("FIM")


def get_all_sharepoint_list_items(data_metrics, connection):
    api_url = f"{connection.site_url}/_api/web/lists/getbytitle('{connection.list_name}')/items"

    headers = {
        "accept": "application/json;odata=verbose",
        "X-RequestDigest": connection.digest_header
    }

    df_lojaonline = pd.read_csv('c:/Temp/repositorios_loja_online_b2c.CSV')
    col_found = []
    tam = len(df_lojaonline)
    for i in range(tam):
        col_found.append('False')
    df_lojaonline['FOUND'] = col_found
    df_lojaonline.to_csv('c:/Temp/loja_log1.csv')
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
                data_metrics.all_items.append(item)

                validado = item.get('VALIDADO')
                if validado:
                    data_metrics.all_items_validados.append(item)
                    it_acao = item.get(HEADER_SHAREPOINT[FIELD_SANIT_POS])
                    if it_acao is None:
                        it_acao = 'Migrar'
                    if re.search('Sanitizar', it_acao):
                        data_metrics.all_items_sanitizar.append(item)
                    elif re.search('Migrar', it_acao):
                        data_metrics.all_items_migrar.append(item)
                else:
                    data_metrics.all_items_not_validados.append(item)
                check_integracao_dip_filter(url_value, item, data_metrics, connection)
                check_hub_pagamentos_filter(url_value, item, data_metrics, connection)
                check_b2b_filter(url_value, item, data_metrics, connection)
                check_loja_online_filter(url_value, item, df_lojaonline, data_metrics, connection)
                check_4p_filter(url_value, item, data_metrics, connection)
                check_diretor_filter(url_value, item, data_metrics, connection)
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

    export_csv_revisados(data_metrics, connection)
    generate_csv_file('c:/Temp/validados.csv', data_metrics.all_items_validados)
    generate_csv_file('c:/Temp/not_validados.csv', data_metrics.all_items_not_validados)
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


def teste_resumo_csv(csv_file_path, connection):
    dataMetrics = DataMetrics(connection.logger)
    dataMetrics.log_resume(False)


def generate_sharepoint_list_all_items_csv(csv_file_path, connection):
    dataMetrics = DataMetrics(connection.logger)

    df_sharepoint = pd.DataFrame(get_all_sharepoint_list_items(dataMetrics, connection))
    connection.logger.debug(f'{len(df_sharepoint.index)} itens recuperados do sharepoint.')
    connection.logger.debug(df_sharepoint.columns)

    df_sharepoint.to_csv(csv_file_path)
    df_sharepoint.to_csv("c:/Temp/list_all_git_url.csv", columns=['UrldoGIT'])

    connection.logger.debug(csv_file_path + " gerado")
    dataMetrics.log_resume(True)
