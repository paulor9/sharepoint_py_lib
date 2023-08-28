import os
# from utils.parameters import SRC_PATH, FIELD_CMDB, FIELD_AF, FIELD_SYS, FIELD_DIR, FIELD_GER, FIELD_FOC, FIELD_EMAIL_FOC, FIELD_EMAIL_GER, FIELD_GER_OPS, FIELD_EMAIL_GER_OPS, FIELD_AREA_OPS, FIELD_OWNER, FIELD_ONWER_RA, FIELD_TECH, FIELD_NAME_COMP, FIELD_CLASSIF, FIELD_SERVER, FIELD_SCM, FIELD_CREATED, FIELD_UPDATED, FIELD_URL_REPO, FIELD_URL_SERVER, FIELD_SANIT, FIELD_SANIT_POS, FIELD_OBS, FIELD_VALID, FIELD_REF_AF, FIELD_DESCRIPTION, HEADER_SHAREPOINT

SRC_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR_PATH = os.path.join(SRC_PATH, 'data')
CREDENTIALS_PATH = os.path.join(SRC_PATH, 'credentials.json')

SHAREPOINT_SITE_URL = 'https://telefonicacorp.sharepoint.com/sites/Arquitetura-RFPs.TMBNL'
SHAREPOINT_LIST_NAME = 'Inventario-DevOps'
SHAREPOINT_LIST_NAME_FOR_CREATE = 'InventarioDevOps'

# Campos do relatorio consolidado
FIELD_CMDB = "IC_ID_CMDB"
FIELD_AF = "SIGLA_ARQ_FUTURO"
FIELD_STATUS = "STATUS_MIGRACAO"
FIELD_SYS = "SISTEMA_GITLAB"
FIELD_DIR = "DIRETORIA_DO_OWNER"
FIELD_GER = "GERENCIA_SR_DO_OWNER"
FIELD_FOC = "PONTO_FOCAL_MIGRACAO_GERENCIA_SR"
FIELD_EMAIL_FOC = "EMAIL_PONTO_FOCAL_MIGRACAO"
FIELD_EMAIL_GER = "EMAIL_GERENCIA_SR"
FIELD_GER_OPS = "GERENCIA_SR_OPERACOES"
FIELD_EMAIL_GER_OPS = "EMAIL_OPERACOES"
FIELD_AREA_OPS = "AREA_OPERACOES"
FIELD_OWNER = "OWNER"
FIELD_ONWER_RA = "OWNER_RA"
FIELD_TECH = "TECNOLOGIA"
FIELD_NAME_COMP = "NOME_DO_COMPONENTE"
FIELD_CLASSIF = "TIPO_DO_BUILD"
FIELD_SERVER = "REPOSITORIO_DE_CI"
FIELD_SCM = "TIPO_DO_SCM"
FIELD_CREATED = "CRIACAO"
FIELD_UPDATED = "ATUALIZACAO"
FIELD_URL_REPO = "URL_DO_SCM"
FIELD_URL_SERVER = "URL_DO_PIPELINE"
FIELD_SANIT = "ACAO_SUGERIDA"
FIELD_SANIT_POS = "ACAO_APOS_REVISAO"
FIELD_OBS = "OBSERVACOES"
FIELD_VALID = "VALIDADO"
FIELD_REF_AF = "REFERENCIA_INTERNA"
FIELD_DESCRIPTION = "DESCRICAO_COMPONENTE"
FIELD_AUX_FILTER_DATA = "AUX_FILTER_DATA"
HEADER_SHAREPOINT = {
    # FIELD_CMDB : "",
    FIELD_AF: "Sistema_x0028_AF_x0029_Id",
    FIELD_SYS: "SistemaGitlab",
    FIELD_DIR: "DiretoriadoOwner",
    FIELD_GER: "GerenciaSrdoOwner",
    # FIELD_FOC : "",
    # FIELD_EMAIL_FOC : "",
    # FIELD_EMAIL_GER : "",
    # FIELD_GER_OPS : "",
    # FIELD_EMAIL_GER_OPS : "",
    # FIELD_AREA_OPS : "",
    FIELD_OWNER: "Owner",
    # FIELD_ONWER_RA : "",
    FIELD_TECH: "Linguagem",
    FIELD_NAME_COMP: "Title",
    FIELD_CLASSIF: "TipodeBuild",
    FIELD_SERVER: "CIServer",
    FIELD_SCM: "TipoFerramentaSCM_x0028_Controle",
    # FIELD_CREATED : "",
    FIELD_UPDATED: "DatadeAtualiza_x00e7__x00e3_o",
    FIELD_URL_REPO: "UrldoGIT",
    FIELD_URL_SERVER: "URLPipeline",
    FIELD_STATUS: "StatusdaMigra_x00e7__x00e3_o",
    FIELD_SANIT : "Ignorar",
    FIELD_SANIT_POS: "A_x00e7__x00e3_oap_x00f3_sRevis_",
    # FIELD_OBS : "",
    FIELD_VALID : "VALIDADO",
    # FIELD_REF_AF : "",
    # FIELD_DESCRIPTION : "",
    FIELD_AUX_FILTER_DATA: "aux_filter_data"
}
