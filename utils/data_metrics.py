import logging
import pandas as pd
from datetime import datetime
from utils import data_area as da


class DataMetrics:

    def __init__(self, logger: object) -> object:
        self.FOLDER_EXPORT_DATA = "c:/Temp/vivo"
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



    def generate_resumo_integracao_csv(self):
        data_str = datetime.today().strftime('%d/%m/%Y')
        hora_str = datetime.today().strftime('%H:%M:%S')
        df  = pd.read_csv("c:/Temp/vivo/resumo_integra.csv")
        v_total = len(self.data_integra.all_items)
        v_validados = len(self.data_integra.all_validados)
        v_sanitizar = len(self.data_integra.all_sanitizar)
        v_migrar = len(self.data_integra.all_migrar)
        v_pendentes = v_total - v_validados
        v_percent = (v_validados * 100) / v_total
        new_row = {'data': data_str, 'hora': hora_str, 'escopo': v_total, 'validados': v_validados,
                   'pendentes': v_pendentes, 'migrar': v_migrar, 'sanitizar': v_sanitizar, 'percent': v_percent}
        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
        df.to_csv("c:/Temp/vivo/resumo_integra.csv",
                  columns=['data', 'hora', 'escopo', 'validados', 'pendentes', 'migrar', 'sanitizar', 'percent'])

    def generate_resumo_b2b_csv(self):
        data_str = datetime.today().strftime('%d/%m/%Y')
        hora_str = datetime.today().strftime('%H:%M:%S')
        df = pd.read_csv("c:/Temp/vivo/resumo_b2b.csv")
        v_total = len(self.data_b2b.all_items)
        v_validados = len(self.data_b2b.all_validados)
        v_sanitizar = len(self.data_b2b.all_sanitizar)
        v_migrar = len(self.data_b2b.all_migrar)
        v_pendentes = v_total - v_validados
        v_percent = (v_validados * 100) / v_total
        new_row = {'data': data_str, 'hora': hora_str, 'escopo': v_total, 'validados': v_validados,
                   'pendentes': v_pendentes, 'migrar': v_migrar, 'sanitizar': v_sanitizar, 'percent': v_percent}
        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
        df.to_csv("c:/Temp/vivo/resumo_b2b.csv",
                  columns=['data', 'hora', 'escopo', 'validados', 'pendentes', 'migrar', 'sanitizar', 'percent'])

    def generate_resumo_loja_b2c_csv(self):
        data_str = datetime.today().strftime('%d/%m/%Y')
        hora_str = datetime.today().strftime('%H:%M:%S')
        df = pd.read_csv("c:/Temp/vivo/resumo_loja_b2c.csv")
        v_total = len(self.data_loja_b2c.all_items)
        v_validados = len(self.data_loja_b2c.all_validados)
        v_sanitizar = len(self.data_loja_b2c.all_sanitizar)
        v_migrar = len(self.data_loja_b2c.all_migrar)
        v_pendentes = v_total - v_validados
        v_percent = (v_validados * 100) / v_total
        new_row = {'data': data_str, 'hora': hora_str, 'escopo': v_total, 'validados': v_validados,
                   'pendentes': v_pendentes, 'migrar': v_migrar, 'sanitizar': v_sanitizar, 'percent': v_percent}
        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
        df.to_csv("c:/Temp/vivo/resumo_loja_b2c.csv",
                  columns=['data', 'hora', 'escopo', 'validados', 'pendentes', 'migrar', 'sanitizar', 'percent'])

    def generate_resumo_plataforma_csv(self):
        data_str = datetime.today().strftime('%d/%m/%Y')
        hora_str = datetime.today().strftime('%H:%M:%S')
        df = pd.read_csv("c:/Temp/vivo/resumo_plataforma.csv")
        v_total = len(self.data_plataforma.all_items)
        v_validados = len(self.data_plataforma.all_validados)
        v_sanitizar = len(self.data_plataforma.all_sanitizar)
        v_migrar = len(self.data_plataforma.all_migrar)
        v_pendentes = v_total - v_validados
        v_percent = (v_validados * 100) / v_total
        new_row = {'data': data_str, 'hora': hora_str, 'escopo': v_total, 'validados': v_validados,
                   'pendentes': v_pendentes, 'migrar': v_migrar, 'sanitizar': v_sanitizar, 'percent': v_percent}
        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
        df.to_csv("c:/Temp/vivo/resumo_plataforma.csv",
                  columns=['data', 'hora', 'escopo', 'validados', 'pendentes', 'migrar', 'sanitizar', 'percent'])
    def generate_resumo_hub_pag_csv(self):
        data_str = datetime.today().strftime('%d/%m/%Y')
        hora_str = datetime.today().strftime('%H:%M:%S')
        df = pd.read_csv("c:/Temp/vivo/resumo_hub_pag.csv")
        v_total = len(self.data_hub_pag.all_items)
        v_validados = len(self.data_hub_pag.all_validados)
        v_sanitizar = len(self.data_hub_pag.all_sanitizar)
        v_migrar = len(self.data_hub_pag.all_migrar)
        v_pendentes = v_total - v_validados
        v_percent = (v_validados * 100) / v_total
        new_row = {'data': data_str, 'hora': hora_str, 'escopo': v_total, 'validados': v_validados,
                   'pendentes': v_pendentes, 'migrar': v_migrar, 'sanitizar': v_sanitizar, 'percent': v_percent}
        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
        df.to_csv("c:/Temp/vivo/resumo_hub_pag.csv",
                  columns=['data', 'hora', 'escopo', 'validados', 'pendentes', 'migrar', 'sanitizar', 'percent'])



    def generate_resumo_geral_csv(self):
        data_str = datetime.today().strftime('%d/%m/%Y')
        hora_str = datetime.today().strftime('%H:%M:%S')
        df = pd.read_csv("c:/Temp/vivo/resumo_geral.csv")
        v_total     = len(self.data_integra.all_items) +     len(self.data_plataforma.all_items)      + len(self.data_hub_pag.all_items)     +  len(self.data_loja_b2c.all_items)     + len(self.data_b2b.all_items)
        v_validados = len(self.data_integra.all_validados) + len(self.data_plataforma.all_validados)  + len(self.data_hub_pag.all_validados) +  len(self.data_loja_b2c.all_validados) + len(self.data_b2b.all_validados)
        v_sanitizar = len(self.data_integra.all_sanitizar) + len(self.data_plataforma.all_sanitizar)  + len(self.data_hub_pag.all_sanitizar) +  len(self.data_loja_b2c.all_sanitizar) + len(self.data_b2b.all_sanitizar)
        v_migrar    = len(self.data_integra.all_migrar) +    len(self.data_plataforma.all_migrar)     + len(self.data_hub_pag.all_migrar)    +  len(self.data_loja_b2c.all_migrar)    + len(self.data_b2b.all_migrar)
        v_pendentes = v_total - v_validados
        v_percent = (v_validados * 100) / v_total
        new_row = {'data': data_str, 'hora': hora_str, 'escopo': v_total, 'validados': v_validados,
                   'pendentes': v_pendentes, 'migrar': v_migrar, 'sanitizar': v_sanitizar, 'percent' : v_percent}
        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
        df.to_csv("c:/Temp/vivo/resumo_geral.csv",  columns= ['data','hora','escopo','validados', 'pendentes', 'migrar','sanitizar', 'percent'])
    def log_resume(self):

        self.generate_resumo_integracao_csv()
        self.generate_resumo_b2b_csv()
        self.generate_resumo_loja_b2c_csv()
        self.generate_resumo_plataforma_csv()
        self.generate_resumo_hub_pag_csv()
        self.generate_resumo_geral_csv()

        self.logger.info('')
        self.logger.info('----------------------------------------------------------------')


        self.logger.info('----------------------------------------------------------------')
