import logging
import pandas as pd
from datetime import datetime
from utils import data_area as da
from utils import data_diretor as dir


class DataMetrics:

    def __init__(self, logger: object) -> object:
        self.FOLDER_EXPORT_DATA = "c:/vivo/metric/"
        self.logger = logger

        self.dir_adriana_lika = dir.DataDiretor('Adriana Lika Shimomura', 'sharepoint_list_dir_adriana_lika.csv', self.logger)
        self.dir_ana_lucia = dir.DataDiretor('Ana Lucia Gomes De Sa Drumond Pardo', 'sharepoint_list_dir_ana_lucia.csv', self.logger)
        self.dir_andre_santos = dir.DataDiretor('Andre Dias Vitor Santos', 'sharepoint_list_dir_andre_santos.csv', self.logger)
        self.dir_bruno_moraes = dir.DataDiretor('Bruno de Moraes', 'sharepoint_list_dir_bruno_moraes.csv', self.logger)
        self.dir_daniel_falbi = dir.DataDiretor('Daniel Falbi', 'sharepoint_list_dir_bruno_moraes.csv', self.logger)
        self.dir_fabio_stellato = dir.DataDiretor('Fabio Stellato', 'sharepoint_list_dir_fabio_stellato.csv', self.logger)
        self.dir_fernando_campos = dir.DataDiretor('Fernando Paes Campos', 'sharepoint_list_dir_fernando_campos.csv', self.logger)
        self.dir_gabriel_simioes = dir.DataDiretor('Gabriel Simoes Goncalves Da Silva', 'sharepoint_list_dir_gabriel_simioes.csv', self.logger)
        self.dir_giuliano_recco = dir.DataDiretor('Giuliano Rodrigues Recco', 'sharepoint_list_dir_giuliano_recco.csv', self.logger)
        self.dir_luis_jacobsen = dir.DataDiretor('LUIS FELIPE JACOBSEN', 'sharepoint_list_dir_luis_jacobsen.csv', self.logger)
        self.dir_nilson_franca = dir.DataDiretor('Nilson Franca Junior', 'sharepoint_list_dir_nilson_franca.csv', self.logger)
        self.dir_patricia_orn = dir.DataDiretor('Patricia Razzolini Orn', 'sharepoint_list_dir_patricia_orn.csv', self.logger)
        self.dir_tania_azevedo = dir.DataDiretor('Tania De Araujo Azevedo', 'sharepoint_list_dir_tania_azevedo.csv', self.logger)
        self.dir_carla_beltrao = dir.DataDiretor('Carla Beltrão', 'sharepoint_list_dir_carla_beltrao.csv', self.logger)
        self.dir_fabio_mori = dir.DataDiretor('Fabio Mori', 'sharepoint_list_dir_fabio_mori.csv', self.logger)
        self.dir_sem_nome = dir.DataDiretor('sem Nome', 'sharepoint_list_dir_sem_nome.csv',self.logger)


        self.data_loja_b2c = da.DataArea('Loja Online (B2C)', 'sharepoint_list_loja_online_b2c.csv', self.logger)
        self.data_b2b = da.DataArea('B2B', 'sharepoint_list_b2b.csv', self.logger)
        self.data_integra = da.DataArea('Integracoes', 'sharepoint_list_integra.csv', self.logger)
        self.data_integra_all_api_gateway = da.DataArea('Integracoes all Api Gateway','sharepoint_list_integra_all_api.csv', self.logger)
        self.data_integra_all_soa = da.DataArea('Integracoes all SOA', 'sharepoint_list_integra_all_soa.csv', self.logger)
        self.data_integra_all_osb = da.DataArea('Integracoes all OSB', 'sharepoint_list_integra_all_osb.csv', self.logger)



        self.data_hub_pag = da.DataArea('Hub Pagamentos', 'sharepoint_list_hub_pag.csv', self.logger)
        self.data_plataforma = da.DataArea('Plataforma Digitais', 'sharepoint_list_4p.csv', self.logger)
        self.data_loja_b2b = da.DataArea('Loja Online (B2B)', 'sharepoint_list_loja_online_b2b.csv', self.logger)
        self.data_oss = da.DataArea('OSS', 'sharepoint_list_oss.csv', self.logger)




        self.all_items = []  # List to store all items
        self.all_items_validados = []
        self.all_items_not_validados = []
        self.all_items_build_pack = []
        self.all_items_legados = []
        self.all_items_validados = []
        self.all_items_sanitizar = []
        self.all_items_migrar = []




    def generate_resumo_dir_sem_nome_csv(self, save_accumulated, data_now):
        data_str = data_now.strftime('%d/%m/%Y')
        hora_str = data_now.strftime('%H:%M:%S')
        data_str_file = data_now.strftime('%d_%m_%Y')
        hora_str_file = data_now.strftime('%H_%M_%S')

        v_total = len(self.dir_sem_nome.all_items)
        v_validados = len(self.dir_sem_nome.all_validados)
        v_sanitizar = len(self.dir_sem_nome.all_sanitizar)
        v_migrar = len(self.dir_sem_nome.all_migrar)
        v_pendentes = v_total - v_validados
        if v_total == 0:
            v_percent_validados = 0
            v_percent_pendentes = 0
        else:
            v_percent_validados = (v_validados * 100) / v_total
            v_percent_pendentes = (v_pendentes * 100) / v_total
        new_row = {'data': data_str, 'hora': hora_str, 'escopo': v_total, 'validados': v_validados,'% Validado':  v_percent_validados ,
                   'pendentes': v_pendentes, '% pendentes' : v_percent_pendentes,  'migrar': v_migrar, 'sanitizar': v_sanitizar}
        new_df = pd.DataFrame([new_row])
        name_file = self.FOLDER_EXPORT_DATA + "diario/diario_dir_adriana_lika_" + data_str_file + "_" + hora_str_file + ".csv"
        new_df.to_csv(name_file, columns=['data', 'hora', 'escopo', 'validados', '% Validado', 'pendentes', '% pendentes', 'migrar', 'sanitizar' ])

        return new_df


    def generate_resumo_dir_adriana_lika_csv(self, save_accumulated, data_now):
        data_str = data_now.strftime('%d/%m/%Y')
        hora_str = data_now.strftime('%H:%M:%S')
        data_str_file = data_now.strftime('%d_%m_%Y')
        hora_str_file = data_now.strftime('%H_%M_%S')

        v_total = len(self.dir_adriana_lika.all_items)
        v_validados = len(self.dir_adriana_lika.all_validados)
        v_sanitizar = len(self.dir_adriana_lika.all_sanitizar)
        v_migrar = len(self.dir_adriana_lika.all_migrar)
        v_pendentes = v_total - v_validados
        if v_total == 0:
            v_percent_validados = 0
            v_percent_pendentes = 0
        else:
            v_percent_validados = (v_validados * 100) / v_total
            v_percent_pendentes = (v_pendentes * 100) / v_total
        new_row = {'data': data_str, 'hora': hora_str, 'escopo': v_total, 'validados': v_validados,
                   '% Validado': v_percent_validados,
                   'pendentes': v_pendentes, '% pendentes': v_percent_pendentes, 'migrar': v_migrar,
                   'sanitizar': v_sanitizar}
        new_df = pd.DataFrame([new_row])
        name_file = self.FOLDER_EXPORT_DATA + "diario/diario_dir_adriana_lika_" + data_str_file + "_" + hora_str_file + ".csv"
        new_df.to_csv(name_file, columns=['data', 'hora', 'escopo', 'validados', '% Validado', 'pendentes', '% pendentes', 'migrar', 'sanitizar' ])

        return new_df

    def generate_resumo_dir_gabriel_simioes_csv(self, save_accumulated, data_now):
        data_str = data_now.strftime('%d/%m/%Y')
        hora_str = data_now.strftime('%H:%M:%S')
        data_str_file = data_now.strftime('%d_%m_%Y')
        hora_str_file = data_now.strftime('%H_%M_%S')

        v_total = len(self.dir_gabriel_simioes.all_items)
        v_validados = len(self.dir_gabriel_simioes.all_validados)
        v_sanitizar = len(self.dir_gabriel_simioes.all_sanitizar)
        v_migrar = len(self.dir_gabriel_simioes.all_migrar)
        v_pendentes = v_total - v_validados

        if v_total == 0:
            v_percent_validados = 0
            v_percent_pendentes = 0
        else:
            v_percent_validados = (v_validados * 100) / v_total
            v_percent_pendentes = (v_pendentes * 100) / v_total
        new_row = {'data': data_str, 'hora': hora_str, 'escopo': v_total, 'validados': v_validados,
                   '% Validado': v_percent_validados,
                   'pendentes': v_pendentes, '% pendentes': v_percent_pendentes, 'migrar': v_migrar,
                   'sanitizar': v_sanitizar}


        new_df = pd.DataFrame([new_row])
        name_file = self.FOLDER_EXPORT_DATA + "diario/diario_dir_gabriel_simioes_" + data_str_file + "_" + hora_str_file + ".csv"
        new_df.to_csv(name_file,
                      columns=['data', 'hora', 'escopo', 'validados', '% Validado', 'pendentes', '% pendentes',
                               'migrar', 'sanitizar'])

        return new_df
    def generate_resumo_dir_patricia_orn_csv(self, save_accumulated, data_now):
        data_str = data_now.strftime('%d/%m/%Y')
        hora_str = data_now.strftime('%H:%M:%S')
        data_str_file = data_now.strftime('%d_%m_%Y')
        hora_str_file = data_now.strftime('%H_%M_%S')

        v_total = len(self.dir_patricia_orn.all_items)
        v_validados = len(self.dir_patricia_orn.all_validados)
        v_sanitizar = len(self.dir_patricia_orn.all_sanitizar)
        v_migrar = len(self.dir_patricia_orn.all_migrar)
        v_pendentes = v_total - v_validados

        if v_total == 0:
            v_percent_validados = 0
            v_percent_pendentes = 0
        else:
            v_percent_validados = (v_validados * 100) / v_total
            v_percent_pendentes = (v_pendentes * 100) / v_total
        new_row = {'data': data_str, 'hora': hora_str, 'escopo': v_total, 'validados': v_validados,
                   '% Validado': v_percent_validados,
                   'pendentes': v_pendentes, '% pendentes': v_percent_pendentes, 'migrar': v_migrar,
                   'sanitizar': v_sanitizar}

        new_df = pd.DataFrame([new_row])
        name_file = self.FOLDER_EXPORT_DATA + "diario/diario_dir_patricia_orn_" + data_str_file + "_" + hora_str_file + ".csv"
        new_df.to_csv(name_file,
                      columns=['data', 'hora', 'escopo', 'validados', '% Validado', 'pendentes', '% pendentes',
                               'migrar', 'sanitizar'])

        return new_df

    def generate_resumo_dir_tania_azevedo_csv(self, save_accumulated, data_now):

        data_str = data_now.strftime('%d/%m/%Y')
        hora_str = data_now.strftime('%H:%M:%S')
        data_str_file = data_now.strftime('%d_%m_%Y')
        hora_str_file = data_now.strftime('%H_%M_%S')

        v_total = len(self.dir_tania_azevedo.all_items)
        v_validados = len(self.dir_tania_azevedo.all_validados)
        v_sanitizar = len(self.dir_tania_azevedo.all_sanitizar)
        v_migrar = len(self.dir_tania_azevedo.all_migrar)
        v_pendentes = v_total - v_validados

        if v_total == 0:
            v_percent_validados = 0
            v_percent_pendentes = 0
        else:
            v_percent_validados = (v_validados * 100) / v_total
            v_percent_pendentes = (v_pendentes * 100) / v_total
        new_row = {'data': data_str, 'hora': hora_str, 'escopo': v_total, 'validados': v_validados,
                   '% Validado': v_percent_validados,
                   'pendentes': v_pendentes, '% pendentes': v_percent_pendentes, 'migrar': v_migrar,
                   'sanitizar': v_sanitizar}

        new_df = pd.DataFrame([new_row])
        name_file = self.FOLDER_EXPORT_DATA + "diario/diario_dir_tania_azevedo_" + data_str_file + "_" + hora_str_file + ".csv"
        new_df.to_csv(name_file,
                      columns=['data', 'hora', 'escopo', 'validados', '% Validado', 'pendentes', '% pendentes',
                               'migrar', 'sanitizar'])

        return new_df

    def generate_resumo_dir_fabio_mori_csv(self, save_accumulated, data_now):

        data_str = data_now.strftime('%d/%m/%Y')
        hora_str = data_now.strftime('%H:%M:%S')
        data_str_file = data_now.strftime('%d_%m_%Y')
        hora_str_file = data_now.strftime('%H_%M_%S')

        v_total = len(self.dir_fabio_mori.all_items)
        v_validados = len(self.dir_fabio_mori.all_validados)
        v_sanitizar = len(self.dir_fabio_mori.all_sanitizar)
        v_migrar = len(self.dir_fabio_mori.all_migrar)
        v_pendentes = v_total - v_validados

        if v_total == 0:
            v_percent_validados = 0
            v_percent_pendentes = 0
        else:
            v_percent_validados = (v_validados * 100) / v_total
            v_percent_pendentes = (v_pendentes * 100) / v_total
        new_row = {'data': data_str, 'hora': hora_str, 'escopo': v_total, 'validados': v_validados,
                   '% Validado': v_percent_validados,
                   'pendentes': v_pendentes, '% pendentes': v_percent_pendentes, 'migrar': v_migrar,
                   'sanitizar': v_sanitizar}

        new_df = pd.DataFrame([new_row])
        name_file = self.FOLDER_EXPORT_DATA + "diario/diario_dir_fabio_mori_" + data_str_file + "_" + hora_str_file + ".csv"
        new_df.to_csv(name_file,
                      columns=['data', 'hora', 'escopo', 'validados', '% Validado', 'pendentes', '% pendentes',
                               'migrar', 'sanitizar'])

        return new_df
    def generate_resumo_dir_carla_beltrao_csv(self, save_accumulated, data_now):

        data_str = data_now.strftime('%d/%m/%Y')
        hora_str = data_now.strftime('%H:%M:%S')
        data_str_file = data_now.strftime('%d_%m_%Y')
        hora_str_file = data_now.strftime('%H_%M_%S')

        v_total = len(self.dir_carla_beltrao.all_items)
        v_validados = len(self.dir_carla_beltrao.all_validados)
        v_sanitizar = len(self.dir_carla_beltrao.all_sanitizar)
        v_migrar = len(self.dir_carla_beltrao.all_migrar)
        v_pendentes = v_total - v_validados

        if v_total == 0:
            v_percent_validados = 0
            v_percent_pendentes = 0
        else:
            v_percent_validados = (v_validados * 100) / v_total
            v_percent_pendentes = (v_pendentes * 100) / v_total
        new_row = {'data': data_str, 'hora': hora_str, 'escopo': v_total, 'validados': v_validados,
                   '% Validado': v_percent_validados,
                   'pendentes': v_pendentes, '% pendentes': v_percent_pendentes, 'migrar': v_migrar,
                   'sanitizar': v_sanitizar}

        new_df = pd.DataFrame([new_row])
        name_file = self.FOLDER_EXPORT_DATA + "diario/diario_dir_carla_beltrao_" + data_str_file + "_" + hora_str_file + ".csv"
        new_df.to_csv(name_file,
                      columns=['data', 'hora', 'escopo', 'validados', '% Validado', 'pendentes', '% pendentes',
                               'migrar', 'sanitizar'])

        return new_df

    def generate_resumo_dir_nilson_franca_csv(self, save_accumulated, data_now):

        data_str = data_now.strftime('%d/%m/%Y')
        hora_str = data_now.strftime('%H:%M:%S')
        data_str_file = data_now.strftime('%d_%m_%Y')
        hora_str_file = data_now.strftime('%H_%M_%S')

        v_total = len(self.dir_nilson_franca.all_items)
        v_validados = len(self.dir_nilson_franca.all_validados)
        v_sanitizar = len(self.dir_nilson_franca.all_sanitizar)
        v_migrar = len(self.dir_nilson_franca.all_migrar)
        v_pendentes = v_total - v_validados

        if v_total == 0:
            v_percent_validados = 0
            v_percent_pendentes = 0
        else:
            v_percent_validados = (v_validados * 100) / v_total
            v_percent_pendentes = (v_pendentes * 100) / v_total
        new_row = {'data': data_str, 'hora': hora_str, 'escopo': v_total, 'validados': v_validados,
                   '% Validado': v_percent_validados,
                   'pendentes': v_pendentes, '% pendentes': v_percent_pendentes, 'migrar': v_migrar,
                   'sanitizar': v_sanitizar}

        new_df = pd.DataFrame([new_row])
        name_file = self.FOLDER_EXPORT_DATA + "diario/diario_dir_nilson_franca_" + data_str_file + "_" + hora_str_file + ".csv"
        new_df.to_csv(name_file,
                      columns=['data', 'hora', 'escopo', 'validados', '% Validado', 'pendentes', '% pendentes',
                               'migrar', 'sanitizar'])

        return new_df

    def generate_resumo_dir_luis_jacobsen_csv(self, save_accumulated, data_now):
        data_str = data_now.strftime('%d/%m/%Y')
        hora_str = data_now.strftime('%H:%M:%S')
        data_str_file = data_now.strftime('%d_%m_%Y')
        hora_str_file = data_now.strftime('%H_%M_%S')

        v_total = len(self.dir_luis_jacobsen.all_items)
        v_validados = len(self.dir_luis_jacobsen.all_validados)
        v_sanitizar = len(self.dir_luis_jacobsen.all_sanitizar)
        v_migrar = len(self.dir_luis_jacobsen.all_migrar)
        v_pendentes = v_total - v_validados

        if v_total == 0:
            v_percent_validados = 0
            v_percent_pendentes = 0
        else:
            v_percent_validados = (v_validados * 100) / v_total
            v_percent_pendentes = (v_pendentes * 100) / v_total
        new_row = {'data': data_str, 'hora': hora_str, 'escopo': v_total, 'validados': v_validados,
                   '% Validado': v_percent_validados,
                   'pendentes': v_pendentes, '% pendentes': v_percent_pendentes, 'migrar': v_migrar,
                   'sanitizar': v_sanitizar}

        new_df = pd.DataFrame([new_row])
        name_file = self.FOLDER_EXPORT_DATA + "diario/diario_dir_luis_jacobsen_" + data_str_file + "_" + hora_str_file + ".csv"
        new_df.to_csv(name_file,
                      columns=['data', 'hora', 'escopo', 'validados', '% Validado', 'pendentes', '% pendentes',
                               'migrar', 'sanitizar'])

        return new_df

    def generate_resumo_dir_giuliano_recco_csv(self, save_accumulated, data_now):
        data_str = data_now.strftime('%d/%m/%Y')
        hora_str = data_now.strftime('%H:%M:%S')
        data_str_file = data_now.strftime('%d_%m_%Y')
        hora_str_file = data_now.strftime('%H_%M_%S')

        v_total = len(self.dir_giuliano_recco.all_items)
        v_validados = len(self.dir_giuliano_recco.all_validados)
        v_sanitizar = len(self.dir_giuliano_recco.all_sanitizar)
        v_migrar = len(self.dir_giuliano_recco.all_migrar)
        v_pendentes = v_total - v_validados

        if v_total == 0:
            v_percent_validados = 0
            v_percent_pendentes = 0
        else:
            v_percent_validados = (v_validados * 100) / v_total
            v_percent_pendentes = (v_pendentes * 100) / v_total
        new_row = {'data': data_str, 'hora': hora_str, 'escopo': v_total, 'validados': v_validados,
                   '% Validado': v_percent_validados,
                   'pendentes': v_pendentes, '% pendentes': v_percent_pendentes, 'migrar': v_migrar,
                   'sanitizar': v_sanitizar}

        new_df = pd.DataFrame([new_row])
        name_file = self.FOLDER_EXPORT_DATA + "diario/diario_dir_giuliano_recco_" + data_str_file + "_" + hora_str_file + ".csv"
        new_df.to_csv(name_file,
                      columns=['data', 'hora', 'escopo', 'validados', '% Validado', 'pendentes', '% pendentes',
                               'migrar', 'sanitizar'])

        return new_df

    def generate_resumo_dir_ana_lucia_csv(self, save_accumulated, data_now):
        data_str = data_now.strftime('%d/%m/%Y')
        hora_str = data_now.strftime('%H:%M:%S')
        data_str_file = data_now.strftime('%d_%m_%Y')
        hora_str_file = data_now.strftime('%H_%M_%S')

        v_total = len(self.dir_ana_lucia.all_items)
        v_validados = len(self.dir_ana_lucia.all_validados)
        v_sanitizar = len(self.dir_ana_lucia.all_sanitizar)
        v_migrar = len(self.dir_ana_lucia.all_migrar)
        v_pendentes = v_total - v_validados

        if v_total == 0:
            v_percent_validados = 0
            v_percent_pendentes = 0
        else:
            v_percent_validados = (v_validados * 100) / v_total
            v_percent_pendentes = (v_pendentes * 100) / v_total
        new_row = {'data': data_str, 'hora': hora_str, 'escopo': v_total, 'validados': v_validados,
                   '% Validado': v_percent_validados,
                   'pendentes': v_pendentes, '% pendentes': v_percent_pendentes, 'migrar': v_migrar,
                   'sanitizar': v_sanitizar}

        new_df = pd.DataFrame([new_row])
        name_file = self.FOLDER_EXPORT_DATA + "diario/diario_dir_ana_lucia_" + data_str_file + "_" + hora_str_file + ".csv"
        new_df.to_csv(name_file,
                      columns=['data', 'hora', 'escopo', 'validados', '% Validado', 'pendentes', '% pendentes',
                               'migrar', 'sanitizar'])

        return new_df

    def generate_resumo_dir_andre_santos_csv(self, save_accumulated, data_now):
        data_str = data_now.strftime('%d/%m/%Y')
        hora_str = data_now.strftime('%H:%M:%S')
        data_str_file = data_now.strftime('%d_%m_%Y')
        hora_str_file = data_now.strftime('%H_%M_%S')

        v_total = len(self.dir_andre_santos.all_items)
        v_validados = len(self.dir_andre_santos.all_validados)
        v_sanitizar = len(self.dir_andre_santos.all_sanitizar)
        v_migrar = len(self.dir_andre_santos.all_migrar)
        v_pendentes = v_total - v_validados

        if v_total == 0:
            v_percent_validados = 0
            v_percent_pendentes = 0
        else:
            v_percent_validados = (v_validados * 100) / v_total
            v_percent_pendentes = (v_pendentes * 100) / v_total
        new_row = {'data': data_str, 'hora': hora_str, 'escopo': v_total, 'validados': v_validados,
                   '% Validado': v_percent_validados,
                   'pendentes': v_pendentes, '% pendentes': v_percent_pendentes, 'migrar': v_migrar,
                   'sanitizar': v_sanitizar}

        new_df = pd.DataFrame([new_row])
        name_file = self.FOLDER_EXPORT_DATA + "diario/diario_dir_andre_santos_" + data_str_file + "_" + hora_str_file + ".csv"
        new_df.to_csv(name_file,
                      columns=['data', 'hora', 'escopo', 'validados', '% Validado', 'pendentes', '% pendentes',
                               'migrar', 'sanitizar'])

        return new_df

    def generate_resumo_dir_bruno_moraes_csv(self, save_accumulated, data_now):
        data_str = data_now.strftime('%d/%m/%Y')
        hora_str = data_now.strftime('%H:%M:%S')
        data_str_file = data_now.strftime('%d_%m_%Y')
        hora_str_file = data_now.strftime('%H_%M_%S')

        v_total = len(self.dir_bruno_moraes.all_items)
        v_validados = len(self.dir_bruno_moraes.all_validados)
        v_sanitizar = len(self.dir_bruno_moraes.all_sanitizar)
        v_migrar = len(self.dir_bruno_moraes.all_migrar)
        v_pendentes = v_total - v_validados

        if v_total == 0:
            v_percent_validados = 0
            v_percent_pendentes = 0
        else:
            v_percent_validados = (v_validados * 100) / v_total
            v_percent_pendentes = (v_pendentes * 100) / v_total
        new_row = {'data': data_str, 'hora': hora_str, 'escopo': v_total, 'validados': v_validados,
                   '% Validado': v_percent_validados,
                   'pendentes': v_pendentes, '% pendentes': v_percent_pendentes, 'migrar': v_migrar,
                   'sanitizar': v_sanitizar}

        new_df = pd.DataFrame([new_row])
        name_file = self.FOLDER_EXPORT_DATA + "diario/diario_dir_bruno_moraes_" + data_str_file + "_" + hora_str_file + ".csv"
        new_df.to_csv(name_file,
                      columns=['data', 'hora', 'escopo', 'validados', '% Validado', 'pendentes', '% pendentes',
                               'migrar', 'sanitizar'])

        if save_accumulated:
            name_file_save_accumulated = self.FOLDER_EXPORT_DATA + "acumulado/resumo_dir_bruno_moraes.csv"
            df = pd.read_csv(name_file_save_accumulated)
            df = pd.concat([df, new_df], ignore_index=True)
            df.to_csv(name_file_save_accumulated,
                      columns=['data', 'hora', 'escopo', 'validados', 'pendentes', 'migrar', 'sanitizar', 'percent'])
        return new_df

    def generate_resumo_dir_daniel_falbi_csv(self, save_accumulated, data_now):
        data_str = data_now.strftime('%d/%m/%Y')
        hora_str = data_now.strftime('%H:%M:%S')
        data_str_file = data_now.strftime('%d_%m_%Y')
        hora_str_file = data_now.strftime('%H_%M_%S')

        v_total = len(self.dir_daniel_falbi.all_items)
        v_validados = len(self.dir_daniel_falbi.all_validados)
        v_sanitizar = len(self.dir_daniel_falbi.all_sanitizar)
        v_migrar = len(self.dir_daniel_falbi.all_migrar)
        v_pendentes = v_total - v_validados

        if v_total == 0:
            v_percent_validados = 0
            v_percent_pendentes = 0
        else:
            v_percent_validados = (v_validados * 100) / v_total
            v_percent_pendentes = (v_pendentes * 100) / v_total
        new_row = {'data': data_str, 'hora': hora_str, 'escopo': v_total, 'validados': v_validados,
                   '% Validado': v_percent_validados,
                   'pendentes': v_pendentes, '% pendentes': v_percent_pendentes, 'migrar': v_migrar,
                   'sanitizar': v_sanitizar}

        new_df = pd.DataFrame([new_row])
        name_file = self.FOLDER_EXPORT_DATA + "diario/diario_dir_daniel_falbi_" + data_str_file + "_" + hora_str_file + ".csv"
        new_df.to_csv(name_file,
                      columns=['data', 'hora', 'escopo', 'validados', '% Validado', 'pendentes', '% pendentes',
                               'migrar', 'sanitizar'])

        if save_accumulated:
            name_file_save_accumulated = self.FOLDER_EXPORT_DATA + "acumulado/resumo_dir_daniel_falbi.csv"
            df = pd.read_csv(name_file_save_accumulated)
            df = pd.concat([df, new_df], ignore_index=True)
            df.to_csv(name_file_save_accumulated,
                      columns=['data', 'hora', 'escopo', 'validados', 'pendentes', 'migrar', 'sanitizar', 'percent'])
        return new_df
    def generate_resumo_dir_fabio_stellato_csv(self, save_accumulated, data_now):
        data_str = data_now.strftime('%d/%m/%Y')
        hora_str = data_now.strftime('%H:%M:%S')
        data_str_file = data_now.strftime('%d_%m_%Y')
        hora_str_file = data_now.strftime('%H_%M_%S')

        v_total = len(self.dir_fabio_stellato.all_items)
        v_validados = len(self.dir_fabio_stellato.all_validados)
        v_sanitizar = len(self.dir_fabio_stellato.all_sanitizar)
        v_migrar = len(self.dir_fabio_stellato.all_migrar)
        v_pendentes = v_total - v_validados

        if v_total == 0:
            v_percent_validados = 0
            v_percent_pendentes = 0
        else:
            v_percent_validados = (v_validados * 100) / v_total
            v_percent_pendentes = (v_pendentes * 100) / v_total
        new_row = {'data': data_str, 'hora': hora_str, 'escopo': v_total, 'validados': v_validados,
                   '% Validado': v_percent_validados,
                   'pendentes': v_pendentes, '% pendentes': v_percent_pendentes, 'migrar': v_migrar,
                   'sanitizar': v_sanitizar}

        new_df = pd.DataFrame([new_row])
        name_file = self.FOLDER_EXPORT_DATA + "diario/diario_dir_fabio_stellato_" + data_str_file + "_" + hora_str_file + ".csv"
        new_df.to_csv(name_file,
                      columns=['data', 'hora', 'escopo', 'validados', '% Validado', 'pendentes', '% pendentes',
                               'migrar', 'sanitizar'])

        if save_accumulated:
            name_file_save_accumulated = self.FOLDER_EXPORT_DATA + "acumulado/resumo_dir_fabio_stellato.csv"
            df = pd.read_csv(name_file_save_accumulated)
            df = pd.concat([df, new_df], ignore_index=True)
            df.to_csv(name_file_save_accumulated,
                      columns=['data', 'hora', 'escopo', 'validados', 'pendentes', 'migrar', 'sanitizar', 'percent'])
        return new_df

    def generate_resumo_dir_fernando_campos_csv(self, save_accumulated, data_now):
        data_str = data_now.strftime('%d/%m/%Y')
        hora_str = data_now.strftime('%H:%M:%S')
        data_str_file = data_now.strftime('%d_%m_%Y')
        hora_str_file = data_now.strftime('%H_%M_%S')

        v_total = len(self.dir_fernando_campos.all_items)
        v_validados = len(self.dir_fernando_campos.all_validados)
        v_sanitizar = len(self.dir_fernando_campos.all_sanitizar)
        v_migrar = len(self.dir_fernando_campos.all_migrar)
        v_pendentes = v_total - v_validados

        if v_total == 0:
            v_percent_validados = 0
            v_percent_pendentes = 0
        else:
            v_percent_validados = (v_validados * 100) / v_total
            v_percent_pendentes = (v_pendentes * 100) / v_total
        new_row = {'data': data_str, 'hora': hora_str, 'escopo': v_total, 'validados': v_validados,
                   '% Validado': v_percent_validados,
                   'pendentes': v_pendentes, '% pendentes': v_percent_pendentes, 'migrar': v_migrar,
                   'sanitizar': v_sanitizar}

        new_df = pd.DataFrame([new_row])
        name_file = self.FOLDER_EXPORT_DATA + "diario/diario_dir_fernando_campos_" + data_str_file + "_" + hora_str_file + ".csv"
        new_df.to_csv(name_file,
                      columns=['data', 'hora', 'escopo', 'validados', '% Validado', 'pendentes', '% pendentes',
                               'migrar', 'sanitizar'])

        if save_accumulated:
            name_file_save_accumulated = self.FOLDER_EXPORT_DATA + "acumulado/resumo_dir_fernando_campos.csv"
            df = pd.read_csv(name_file_save_accumulated)
            df = pd.concat([df, new_df], ignore_index=True)
            df.to_csv(name_file_save_accumulated,
                      columns=['data', 'hora', 'escopo', 'validados', 'pendentes', 'migrar', 'sanitizar', 'percent'])
        return new_df
    def generate_resumo_integracao_csv(self, save_accumulated, data_now):
        data_str = data_now.strftime('%d/%m/%Y')
        hora_str = data_now.strftime('%H:%M:%S')
        data_str_file = data_now.strftime('%d_%m_%Y')
        hora_str_file = data_now.strftime('%H_%M_%S')

        v_total = len(self.data_integra.all_items)
        v_validados = len(self.data_integra.all_validados)
        v_sanitizar = len(self.data_integra.all_sanitizar)
        v_migrar = len(self.data_integra.all_migrar)
        v_pendentes = v_total - v_validados

        if v_total == 0:
            v_percent = 0
        else:
            v_percent = (v_validados * 100) / v_total
        new_row = {'data': data_str, 'hora': hora_str, 'escopo': v_total, 'validados': v_validados,
                   'pendentes': v_pendentes, 'migrar': v_migrar, 'sanitizar': v_sanitizar, 'percent': v_percent}
        new_df = pd.DataFrame([new_row])
        name_file = self.FOLDER_EXPORT_DATA + "diario/diario_integra_" + data_str_file + "_" + hora_str_file + ".csv"
        new_df.to_csv(name_file,
                      columns=['data', 'hora', 'escopo', 'validados', 'pendentes', 'migrar', 'sanitizar', 'percent'])

        if save_accumulated:
            name_file_save_accumulated = self.FOLDER_EXPORT_DATA + "acumulado/resumo_integra.csv"
            df = pd.read_csv(name_file_save_accumulated)
            df = pd.concat([df, new_df], ignore_index=True)
            df.to_csv(name_file_save_accumulated,
                      columns=['data', 'hora', 'escopo', 'validados', 'pendentes', 'migrar', 'sanitizar', 'percent'])
        return new_df

    def generate_resumo_b2b_csv(self, save_accumulated, data_now):
        data_str = data_now.strftime('%d/%m/%Y')
        hora_str = data_now.strftime('%H:%M:%S')
        data_str_file = data_now.strftime('%d_%m_%Y')
        hora_str_file = data_now.strftime('%H_%M_%S')

        v_total = len(self.data_b2b.all_items)
        v_validados = len(self.data_b2b.all_validados)
        v_sanitizar = len(self.data_b2b.all_sanitizar)
        v_migrar = len(self.data_b2b.all_migrar)
        v_pendentes = v_total - v_validados

        if v_total == 0:
            v_percent = 0
        else:
            v_percent = (v_validados * 100) / v_total
        new_row = {'data': data_str, 'hora': hora_str, 'escopo': v_total, 'validados': v_validados,
                   'pendentes': v_pendentes, 'migrar': v_migrar, 'sanitizar': v_sanitizar, 'percent': v_percent}
        new_df = pd.DataFrame([new_row])
        name_file = self.FOLDER_EXPORT_DATA + "diario/diario_b2b_" + data_str_file + "_" + hora_str_file + ".csv"
        new_df.to_csv(name_file,
                      columns=['data', 'hora', 'escopo', 'validados', 'pendentes', 'migrar', 'sanitizar', 'percent'])

        if save_accumulated:
            name_file_save_accumulated = self.FOLDER_EXPORT_DATA + "acumulado/resumo_b2b.csv"
            df = pd.read_csv(name_file_save_accumulated)
            df = pd.concat([df, new_df], ignore_index=True)
            df.to_csv(name_file_save_accumulated,
                      columns=['data', 'hora', 'escopo', 'validados', 'pendentes', 'migrar', 'sanitizar', 'percent'])
        return new_df

    def generate_resumo_loja_b2c_csv(self, save_accumulated, data_now):
        data_str = data_now.strftime('%d/%m/%Y')
        hora_str = data_now.strftime('%H:%M:%S')
        data_str_file = data_now.strftime('%d_%m_%Y')
        hora_str_file = data_now.strftime('%H_%M_%S')

        v_total = len(self.data_loja_b2c.all_items)
        v_validados = len(self.data_loja_b2c.all_validados)
        v_sanitizar = len(self.data_loja_b2c.all_sanitizar)
        v_migrar = len(self.data_loja_b2c.all_migrar)
        v_pendentes = v_total - v_validados
        if v_total == 0:
            v_percent = 0
        else:
            v_percent = (v_validados * 100) / v_total
        new_row = {'data': data_str, 'hora': hora_str, 'escopo': v_total, 'validados': v_validados,
                   'pendentes': v_pendentes, 'migrar': v_migrar, 'sanitizar': v_sanitizar, 'percent': v_percent}
        new_df = pd.DataFrame([new_row])
        name_file = self.FOLDER_EXPORT_DATA + "diario/diario_loja_b2c_" + data_str_file + "_" + hora_str_file + ".csv"
        new_df.to_csv(name_file,
                      columns=['data', 'hora', 'escopo', 'validados', 'pendentes', 'migrar', 'sanitizar', 'percent'])

        if save_accumulated:
            name_file_save_accumulated = self.FOLDER_EXPORT_DATA + "acumulado/resumo_loja_b2c.csv"
            df = pd.read_csv(name_file_save_accumulated)
            df = pd.concat([df, new_df], ignore_index=True)
            df.to_csv(name_file_save_accumulated,
                      columns=['data', 'hora', 'escopo', 'validados', 'pendentes', 'migrar', 'sanitizar', 'percent'])
        return new_df

    def generate_resumo_plataforma_csv(self, save_accumulated, data_now):
        data_str = data_now.strftime('%d/%m/%Y')
        hora_str = data_now.strftime('%H:%M:%S')
        data_str_file = data_now.strftime('%d_%m_%Y')
        hora_str_file = data_now.strftime('%H_%M_%S')

        v_total = len(self.data_plataforma.all_items)
        v_validados = len(self.data_plataforma.all_validados)
        v_sanitizar = len(self.data_plataforma.all_sanitizar)
        v_migrar = len(self.data_plataforma.all_migrar)
        v_pendentes = v_total - v_validados
        if v_total == 0:
            v_percent = 0
        else:
            v_percent = (v_validados * 100) / v_total
        new_row = {'data': data_str, 'hora': hora_str, 'escopo': v_total, 'validados': v_validados,
                   'pendentes': v_pendentes, 'migrar': v_migrar, 'sanitizar': v_sanitizar, 'percent': v_percent}
        new_df = pd.DataFrame([new_row])
        name_file = self.FOLDER_EXPORT_DATA + "diario/diario_plataforma_4p_" + data_str_file + "_" + hora_str_file + ".csv"
        new_df.to_csv(name_file,
                      columns=['data', 'hora', 'escopo', 'validados', 'pendentes', 'migrar', 'sanitizar', 'percent'])
        if save_accumulated:
            name_file_save_accumulated = self.FOLDER_EXPORT_DATA + "acumulado/resumo_plataforma.csv"
            df = pd.read_csv(name_file_save_accumulated)
            df = pd.concat([df, new_df], ignore_index=True)
            df.to_csv(name_file_save_accumulated,
                      columns=['data', 'hora', 'escopo', 'validados', 'pendentes', 'migrar', 'sanitizar', 'percent'])
        return new_df

    def generate_resumo_hub_pag_csv(self, save_accumulated, data_now):
        data_str = data_now.strftime('%d/%m/%Y')
        hora_str = data_now.strftime('%H:%M:%S')
        data_str_file = data_now.strftime('%d_%m_%Y')
        hora_str_file = data_now.strftime('%H_%M_%S')

        v_total = len(self.data_hub_pag.all_items)
        v_validados = len(self.data_hub_pag.all_validados)
        v_sanitizar = len(self.data_hub_pag.all_sanitizar)
        v_migrar = len(self.data_hub_pag.all_migrar)
        v_pendentes = v_total - v_validados
        if v_total == 0:
            v_percent = 0
        else:
            v_percent = (v_validados * 100) / v_total
        new_row = {'data': data_str, 'hora': hora_str, 'escopo': v_total, 'validados': v_validados,
                   'pendentes': v_pendentes, 'migrar': v_migrar, 'sanitizar': v_sanitizar, 'percent': v_percent}
        new_df = pd.DataFrame([new_row])
        name_file = self.FOLDER_EXPORT_DATA + "diario/diario_hub_pag_" + data_str_file + "_" + hora_str_file + ".csv"
        new_df.to_csv(name_file,
                      columns=['data', 'hora', 'escopo', 'validados', 'pendentes', 'migrar', 'sanitizar', 'percent'])

        if save_accumulated:
            name_file_save_accumulated = self.FOLDER_EXPORT_DATA + "acumulado/resumo_hub_pag.csv"
            df = pd.read_csv(name_file_save_accumulated)
            df = pd.concat([df, new_df], ignore_index=True)
            df.to_csv(name_file_save_accumulated,
                      columns=['data', 'hora', 'escopo', 'validados', 'pendentes', 'migrar', 'sanitizar', 'percent'])
        return new_df



    def generate_resumo_geral_csv(self, save_accumulated, data_now):
        data_str = data_now.strftime('%d/%m/%Y')
        hora_str = data_now.strftime('%H:%M:%S')
        data_str_file = data_now.strftime('%d_%m_%Y')
        hora_str_file = data_now.strftime('%H_%M_%S')
        v_total = len(self.all_items)
        v_validados = len(self.all_items_validados)
        v_sanitizar = len(self.all_items_sanitizar)
        v_migrar = len(self.all_items_migrar)
        v_pendentes = v_total - v_validados

        if v_total == 0:
            v_percent_validados = 0
            v_percent_pendentes = 0
        else:
            v_percent_validados = (v_validados * 100) / v_total
            v_percent_pendentes = (v_pendentes * 100) / v_total
        new_row = {'data': data_str, 'hora': hora_str, 'escopo': v_total, 'validados': v_validados,
                   '% Validado': v_percent_validados,
                   'pendentes': v_pendentes, '% pendentes': v_percent_pendentes, 'migrar': v_migrar,
                   'sanitizar': v_sanitizar}

        new_df = pd.DataFrame([new_row])
        name_file = self.FOLDER_EXPORT_DATA + "diario/diario_geral_" + data_str_file + "_" + hora_str_file + ".csv"
        new_row = {'data': data_str, 'hora': hora_str, 'escopo': v_total, 'validados': v_validados,
                   '% Validado': v_percent_validados,
                   'pendentes': v_pendentes, '% pendentes': v_percent_pendentes, 'migrar': v_migrar,
                   'sanitizar': v_sanitizar}

        if save_accumulated:
            name_file_save_accumulated = self.FOLDER_EXPORT_DATA + "acumulado/resumo_geral.csv"
            df = pd.read_csv(name_file_save_accumulated)
            df = pd.concat([df, new_df], ignore_index=True)
            df.to_csv(name_file_save_accumulated,
                      columns=['data', 'hora', 'escopo', 'validados', 'pendentes', 'migrar', 'sanitizar', 'percent'])
        return new_df

    def generate_resumo_geral_workshop_csv(self, save_accumulated, data_now):
        data_str = data_now.strftime('%d/%m/%Y')
        hora_str = data_now.strftime('%H:%M:%S')
        data_str_file = data_now.strftime('%d_%m_%Y')
        hora_str_file = data_now.strftime('%H_%M_%S')
        v_total = len(self.data_integra.all_items) + len(self.data_plataforma.all_items) + len(
            self.data_hub_pag.all_items) + len(self.data_loja_b2c.all_items) + len(self.data_b2b.all_items)
        v_validados = len(self.data_integra.all_validados) + len(self.data_plataforma.all_validados) + len(
            self.data_hub_pag.all_validados) + len(self.data_loja_b2c.all_validados) + len(self.data_b2b.all_validados)
        v_sanitizar = len(self.data_integra.all_sanitizar) + len(self.data_plataforma.all_sanitizar) + len(
            self.data_hub_pag.all_sanitizar) + len(self.data_loja_b2c.all_sanitizar) + len(self.data_b2b.all_sanitizar)
        v_migrar = len(self.data_integra.all_migrar) + len(self.data_plataforma.all_migrar) + len(
            self.data_hub_pag.all_migrar) + len(self.data_loja_b2c.all_migrar) + len(self.data_b2b.all_migrar)
        v_pendentes = v_total - v_validados
        if v_total == 0:
            v_percent = 0
        else:
            v_percent = (v_validados * 100) / v_total
        new_row = {'data': data_str, 'hora': hora_str, 'escopo': v_total, 'validados': v_validados,
                   'pendentes': v_pendentes, 'migrar': v_migrar, 'sanitizar': v_sanitizar, 'percent': v_percent}
        new_df = pd.DataFrame([new_row])
        name_file = self.FOLDER_EXPORT_DATA + "diario/diario_geral_workshop" + data_str_file + "_" + hora_str_file + ".csv"
        new_df.to_csv(name_file,
                      columns=['data', 'hora', 'escopo', 'validados', 'pendentes', 'migrar', 'sanitizar', 'percent'])
        if save_accumulated:
            name_file_save_accumulated = self.FOLDER_EXPORT_DATA + "acumulado/resumo_geral_workshop.csv"
            df = pd.read_csv(name_file_save_accumulated)
            df = pd.concat([df, new_df], ignore_index=True)
            df.to_csv(name_file_save_accumulated,
                      columns=['data', 'hora', 'escopo', 'validados', 'pendentes', 'migrar', 'sanitizar', 'percent'])
        return new_df

    def log_resume(self, save_accumulated):
        data_now = datetime.today()

        df_integra = self.generate_resumo_integracao_csv(save_accumulated, data_now)
        df_integra["AREA"] = "DIP/Integrações"
        df_integra["E-MAIL"] = "jose.guerreiro@telefonica.com"
        df_integra["Arquivo"] = "resumo_integra.csv"
        df_integra["Workshop"] = "10/8/2023"

        df_b2b = self.generate_resumo_b2b_csv(save_accumulated, data_now)
        df_b2b["AREA"] = "B2B"
        df_b2b["E-MAIL"] = "diego.paula@telefonica.com"
        df_b2b["Arquivo"] = "resumo_b2b.csv"
        df_b2b["Workshop"] = "15/08/2023"

        df_loja_b2c = self.generate_resumo_loja_b2c_csv(save_accumulated, data_now)
        df_loja_b2c["AREA"] = "Loja Online B2C"
        df_loja_b2c["E-MAIL"] = "lucas.pereira@telefonica"
        df_loja_b2c["Arquivo"] = "resumo_loja_b2c.csv"
        df_loja_b2c["Workshop"] = "21/08/2023"

        df_plataforma_4p = self.generate_resumo_plataforma_csv(save_accumulated, data_now)
        df_plataforma_4p["AREA"] = "Plataforma Digitais e APIs (4p)"
        df_plataforma_4p["E-MAIL"] = "pedro.bsoares@telefonica.com"
        df_plataforma_4p["Arquivo"] = "resumo_plataforma.csv"
        df_plataforma_4p["Workshop"] = "25/08/2023"

        df_hub_pag = self.generate_resumo_hub_pag_csv(save_accumulated, data_now)
        df_hub_pag["AREA"] = "Hub Pagamentos"
        df_hub_pag["E-MAIL"] = "katia.rsilva@telefonica.com"
        df_hub_pag["Arquivo"] = "resumo_hub_pag.csv"
        df_hub_pag["Workshop"] = "23/08/2023"

        df_geral = self.generate_resumo_geral_workshop_csv(save_accumulated, data_now)
        df_geral["AREA"] = "Geral"
        df_geral["E-MAIL"] = ""
        df_geral["Arquivo"] = "resumo_geral.csv"
        df_geral["Workshop"] = ""

        df_geral = pd.concat([df_geral, df_integra], ignore_index=True)
        df_geral = pd.concat([df_geral, df_b2b], ignore_index=True)
        df_geral = pd.concat([df_geral, df_hub_pag], ignore_index=True)
        df_geral = pd.concat([df_geral, df_loja_b2c], ignore_index=True)
        df_geral = pd.concat([df_geral, df_plataforma_4p], ignore_index=True)


        data_str = data_now.strftime('%d_%m_%Y')
        hora_str = data_now.strftime('%H_%M_%S')
        name_file = self.FOLDER_EXPORT_DATA + "agrupado_" + data_str + "_" + hora_str + ".csv"
        df_geral.to_csv(name_file, columns=['AREA', 'escopo','validados','pendentes','migrar','sanitizar','percent','Workshop','Arquivo'])

        save_accumulated = False
        df_diretor = self.generate_resumo_geral_csv(save_accumulated, data_now)
        df_diretor["Nome"] = "Todos"

        df_adriana_lika  = self.generate_resumo_dir_adriana_lika_csv(save_accumulated, data_now)
        df_adriana_lika["Nome"] = "Adriana Lika Shimomura"

        df_ana_lucia  = self.generate_resumo_dir_ana_lucia_csv(save_accumulated, data_now)
        df_ana_lucia["Nome"] = "Ana Lucia Gomes De Sa Drumond Pardo"

        df_andre_santos = self.generate_resumo_dir_andre_santos_csv(save_accumulated, data_now)
        df_andre_santos["Nome"] = "Andre Dias Vitor Santos"

        df_bruno_moraes = self.generate_resumo_dir_bruno_moraes_csv(save_accumulated, data_now)
        df_bruno_moraes["Nome"] = "Bruno de Moraes"

        df_daniel_falbi = self.generate_resumo_dir_daniel_falbi_csv(save_accumulated, data_now)
        df_daniel_falbi["Nome"] = "Daniel Falbi"

        df_fabio_stellato = self.generate_resumo_dir_fabio_stellato_csv(save_accumulated, data_now)
        df_fabio_stellato["Nome"] = "Fabio Stellato"

        df_fernando_campos = self.generate_resumo_dir_fernando_campos_csv(save_accumulated, data_now)
        df_fernando_campos["Nome"] = "Fernando Paes Campos"

        df_gabriel_simioes = self.generate_resumo_dir_gabriel_simioes_csv(save_accumulated, data_now)
        df_gabriel_simioes["Nome"] = "Gabriel Simoes Goncalves Da Silva"

        df_giuliano_recco = self.generate_resumo_dir_giuliano_recco_csv(save_accumulated, data_now)
        df_giuliano_recco["Nome"] = "Giuliano Rodrigues Recco"

        df_luis_jacobsen = self.generate_resumo_dir_luis_jacobsen_csv(save_accumulated, data_now)
        df_luis_jacobsen["Nome"] = "LUIS FELIPE JACOBSEN"

        df_nilson_franca = self.generate_resumo_dir_nilson_franca_csv(save_accumulated, data_now)
        df_nilson_franca["Nome"] = "Nilson Franca Junior"

        df_patricia_orn = self.generate_resumo_dir_patricia_orn_csv(save_accumulated, data_now)
        df_patricia_orn["Nome"] = "Patricia Razzolini Orn"

        df_tania_azevedo = self.generate_resumo_dir_tania_azevedo_csv(save_accumulated, data_now)
        df_tania_azevedo["Nome"] = "Tania De Araujo Azevedo"

        df_carla_beltrao = self.generate_resumo_dir_carla_beltrao_csv(save_accumulated, data_now)
        df_carla_beltrao["Nome"] = "Carla Beltrão"

        df_fabio_mori = self.generate_resumo_dir_fabio_mori_csv(save_accumulated, data_now)
        df_fabio_mori["Nome"] = "Fabio Mori"

        df_sem_nome = self.generate_resumo_dir_sem_nome_csv(save_accumulated, data_now)
        df_sem_nome["Nome"] = "Sem Nome"

        df_diretor = pd.concat([df_diretor, df_adriana_lika], ignore_index=True)
        df_diretor = pd.concat([df_diretor, df_ana_lucia], ignore_index=True)
        df_diretor = pd.concat([df_diretor, df_andre_santos], ignore_index=True)
        df_diretor = pd.concat([df_diretor, df_bruno_moraes], ignore_index=True)
        df_diretor = pd.concat([df_diretor, df_daniel_falbi], ignore_index=True)
        df_diretor = pd.concat([df_diretor, df_fabio_stellato], ignore_index=True)
        df_diretor = pd.concat([df_diretor, df_fernando_campos], ignore_index=True)
        df_diretor = pd.concat([df_diretor, df_gabriel_simioes], ignore_index=True)
        df_diretor = pd.concat([df_diretor, df_giuliano_recco], ignore_index=True)
        df_diretor = pd.concat([df_diretor, df_luis_jacobsen], ignore_index=True)
        df_diretor = pd.concat([df_diretor, df_nilson_franca], ignore_index=True)
        df_diretor = pd.concat([df_diretor, df_patricia_orn], ignore_index=True)
        df_diretor = pd.concat([df_diretor, df_tania_azevedo], ignore_index=True)
        df_diretor = pd.concat([df_diretor, df_carla_beltrao], ignore_index=True)
        df_diretor = pd.concat([df_diretor, df_fabio_mori], ignore_index=True)
        df_diretor = pd.concat([df_diretor, df_sem_nome], ignore_index=True)


        data_str = data_now.strftime('%d_%m_%Y')
        hora_str = data_now.strftime('%H_%M_%S')
        name_file = self.FOLDER_EXPORT_DATA + "diretor_agrupado_" + data_str + "_" + hora_str + ".csv"
        df_diretor.to_csv(name_file,
                        columns=['Nome', 'escopo', 'validados', '% Validado', 'pendentes', '% pendentes',
                               'migrar', 'sanitizar'])

        self.logger.info('')
        self.logger.info('----------------------------------------------------------------')

        self.logger.info('----------------------------------------------------------------')
