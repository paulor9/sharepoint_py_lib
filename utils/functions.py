import json
import os
import pandas as pd
import zipfile


def get_config(credentials_path):
    with open(credentials_path) as f:
        config = json.load(f)
    return config

def get_database(version, download_file_dir, username, password):
    name = 'dados_migracao'
    download_file_path = os.path.join(download_file_dir, f'{name}.zip')
    url = f'https://nexus.telefonica.com.br/repository/migracao/devops/vivo/telefonica/{name}/{version}/{name}-{version}.zip'
    cmd = f'curl {url} --output {download_file_path} -u {username}:{password}'
    os.system(cmd)

def unzip_file_and_remove(zip_file_path, extract_path):
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(extract_path)
    
    os.remove(zip_file_path)  # Remove the zip file

def read_csv_items(file_path):
    # Ler o arquivo CSV para um DataFrame
    df = pd.read_csv(file_path)

    # Preencher campos nulos
    df = df.fillna('')

    # Retornar os itens como uma lista de dicionários
    return df.to_dict('records')


def add_line_to_file(file_name, new_line, logger):
    try:
        with open(file_name, 'a') as file:
            file.write(new_line + '\n')
        logger.debug(f"Arquivo {file_name} atualizado.")
    except IOError:
        raise Exception("Erro na escrita/abertura do arquivo.")