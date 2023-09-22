import coloredlogs
import logging
from utils import parameters as param
from utils.sharepoint_functions import *
from utils import functions
from utils import connection as conn

config = functions.get_config(param.CREDENTIALS_PATH)

# sharepoint_items_file_path = os.path.join(param.DATA_DIR_PATH, 'inventario_devops_itens.csv')



def read_csv_items(file_path, logger):
    # Ler o arquivo para um DataFrame
    df = pd.read_csv(file_path)

    logger.debug(f'{len(df.index)} linhas lidas do arquivo {file_path}.')
    logger.debug(df)

    # Retornar os itens
    return df


def check_field_duplicated_values(output_file_path, df_items, field, logger):
    df_duplicated = df_items[df_items.duplicated(subset=[field], keep=False)]
    has_unique_values = df_duplicated.empty
    if not has_unique_values:
        logger.warning(
            f'{len(df_duplicated)} valores duplicados para o campo {field}.')
        logger.debug(f'Valores duplicados: {df_duplicated[field].to_list()}')

        df_duplicated.rename(columns=param.HEADER_SHAREPOINT, inplace=True)
        columns_to_drop = [col for col in df_duplicated.columns if col not in param.HEADER_SHAREPOINT.values()]
        df_duplicated.drop(columns=columns_to_drop, inplace=True)
        df_duplicated.to_csv(output_file_path)
    else:
        logger.info(f'Não há valores duplicados para o campo {field}.')
    return has_unique_values


def check_field_blank_values(output_file_path, df_items, field, logger):
    df_blank = df_items[df_items[field].str.strip() == '']
    has_blank_values = not df_blank.empty
    if has_blank_values:
        logger.warning(
            f'{len(df_blank)} valores em branco para o campo {field}.')
        logger.debug(f'Valores em branco: {df_blank[field].to_list()}')

        df_blank.rename(columns=param.HEADER_SHAREPOINT, inplace=True)
        columns_to_drop = [col for col in df_blank.columns if col not in param.HEADER_SHAREPOINT.values()]
        df_blank.drop(columns=columns_to_drop, inplace=True)
        df_blank.to_csv(output_file_path)
    else:
        logger.info(f'Não há valores em branco para o campo {field}.')
    return has_blank_values


def main():
    data_str = datetime.today().strftime('%d_%m_%Y')
    hora_str = datetime.today().strftime('%H_%M_%S')
    sharepoint_items_file_path = "c:/vivo/inventario_devops_itens_" + data_str + "_" + hora_str + ".csv"
    logger = logging.getLogger(__name__)
    coloredlogs.install(level='DEBUG', logger=logger)
    connection = conn.Connection(logger)
    generate_sharepoint_list_all_items_csv(sharepoint_items_file_path, connection)
   # import_new_gitlab_itens(connection)
 #  teste_resumo_csv(sharepoint_items_file_path, connection)


if __name__ == "__main__":
    main()
