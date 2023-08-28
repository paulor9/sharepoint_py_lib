import logging


class DataMetrics:
    logger = None
    FOLDER_EXPORT_DATA = "c:/Temp/"
    all_items = []  # List to store all items
    all_items_validados = [];
    all_items_not_validados = [];
    dip_Business_Partner_Domain = []
    dip_Common_Domain = []
    dip_Customer_Domain = []
    dip_Enterprise_Domain = []
    dip_Integration_Domain = []
    dip_Market_Sales_Domain = []
    dip_Product_Domain = []
    dip_Resource_Domain = []
    dip_Service_Domain = []
    l_osb = []
    l_soa = []
    l_api_gateway = []

    dip_Business_Partner_Domain_done = []
    dip_Common_Domain_done = []
    dip_Customer_Domain_done = []
    dip_Enterprise_Domain_done = []
    dip_Integration_Domain_done = []
    dip_Market_Sales_Domain_done = []
    dip_Product_Domain_done = []
    dip_Resource_Domain_done = []
    dip_Service_Domain_done = []
    l_osb_done = []
    l_soa_done = []
    l_api_gateway_done = []

    all_loja_online = []
    all_loja_online_done = []

    all_b2b = []
    all_b2b_done = []

    all_hub_pag = []
    all_hub_pag_done = []


    def log_resume(self):
        self.logger.info('')
        self.logger.info('----------------------------------------------------------------')

        total_b2b = len(self.all_b2b)
        total_b2b_done = len( self.all_b2b_done)
        total_b2b_pendentes = total_b2b - total_b2b_done

        total_loja_online = len(self.all_loja_online)
        total_loja_online_done = len(self.all_loja_online_done)
        total_loja_online_pendentes = total_loja_online - total_loja_online_done

        total_hub_pag = len(self.all_hub_pag)
        total_hub_pag_done = len(self.all_hub_pag_done)
        total_hub_pag_pendentes = total_hub_pag - total_hub_pag_done

        total_dip_done = len(self.dip_Business_Partner_Domain_done) + len(self.dip_Common_Domain_done) + \
                         len(self.dip_Customer_Domain_done) + len(self.dip_Enterprise_Domain_done) + \
                         len(self.dip_Integration_Domain_done) + len(self.dip_Market_Sales_Domain_done) + \
                         len(self.dip_Product_Domain_done) + len(self.dip_Resource_Domain_done) + \
                         len(self.dip_Service_Domain_done) + len(self.l_osb_done) + len(self.l_soa_done) + \
                         len(self.l_api_gateway_done)
        total_dip = len(self.dip_Business_Partner_Domain) + len(self.dip_Common_Domain) + \
                    len(self.dip_Customer_Domain) + len(self.dip_Enterprise_Domain) + \
                    len(self.dip_Integration_Domain) + len(self.dip_Market_Sales_Domain) + \
                    len(self.dip_Product_Domain) + len(self.dip_Resource_Domain) + \
                    len(self.dip_Service_Domain) + len(self.l_osb) + len(self.l_soa) + \
                    len(self.l_api_gateway)

        total_dip_pendentes = total_dip - total_dip_done

        self.logger.info(f'B2B Total  {total_b2b}.')
        self.logger.info(f'B2b Feito  {total_b2b_done}.')
        self.logger.info(f'B2B Pendentes  {total_b2b_pendentes}')

        self.logger.info(f'LojaOnline Total  {total_loja_online}.')
        self.logger.info(f'LojaOnline Feito  {total_loja_online_done}.')
        self.logger.info(f'LojaOnline Pendentes  {total_loja_online_pendentes}')

        self.logger.info(f'DIP Total  {total_dip}.')
        self.logger.info(f'DIP Feito  {total_dip_done}.')
        self.logger.info(f'DIP Pendentes  {total_dip_pendentes}')

        self.logger.info(f'Hub Pag. Total  {total_hub_pag}.')
        self.logger.info(f'Hub Pag. Feito  {total_hub_pag_done}.')
        self.logger.info(f'Hub Pag. Pendentes  {total_hub_pag_pendentes}')

        self.logger.info('----------------------------------------------------------------')



