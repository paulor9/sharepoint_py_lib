import logging
import pandas as pd
from datetime import datetime
from utils import data_area as da


class DataMetrics:

    def __init__(self, logger: object) -> object:
        self.FOLDER_EXPORT_DATA = "c:/vivo/metric/"
        self.logger = logger

        self.data_loja_b2c = da.DataArea('Loja Online (B2C)', 'sharepoint_list_loja_online_b2c.csv', self.logger)
        self.data_b2b = da.DataArea('B2B', 'sharepoint_list_b2b.csv', self.logger)
        self.data_integra = da.DataArea('Integracoes', 'sharepoint_list_integra.csv', self.logger)
        self.data_hub_pag = da.DataArea('Hub Pagamentos', 'sharepoint_list_hub_pag.csv', self.logger)
        self.data_plataforma = da.DataArea('Plataforma Digitais', 'sharepoint_list_4p.csv', self.logger)
        self.data_loja_b2b = da.DataArea('Loja Online (B2B)', 'sharepoint_list_loja_online_b2b.csv', self.logger)

        self.all_items = []  # List to store all items
        self.all_items_validados = [];
        self.all_items_not_validados = [];
        self.all_items_build_pack = [];
        self.all_items_legados = [];

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
        new_row = {'data': data_str, 'hora': hora_str, 'escopo': v_total, 'validados': v_validados, 'pendentes': v_pendentes, 'migrar': v_migrar, 'sanitizar': v_sanitizar, 'percent': v_percent}
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
        name_file = self.FOLDER_EXPORT_DATA + "diario/diario_geral_" + data_str_file + "_" + hora_str_file + ".csv"
        new_df.to_csv(name_file,
                      columns=['data', 'hora', 'escopo', 'validados', 'pendentes', 'migrar', 'sanitizar', 'percent'])
        if save_accumulated:
            name_file_save_accumulated = self.FOLDER_EXPORT_DATA + "acumulado/resumo_geral.csv"
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

        df_geral = self.generate_resumo_geral_csv(save_accumulated, data_now)
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

        self.logger.info('')
        self.logger.info('----------------------------------------------------------------')

        self.logger.info('----------------------------------------------------------------')
