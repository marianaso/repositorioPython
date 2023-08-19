import boto3
import os

s3_client = boto3.client('s3')

diretorio_origem_empresas = "C:\Users\mariana.oliveira\Documents\CimentoForte\dadosEmpresas\"

diretorio_origem_estabelecimentos = "C:\Users\mariana.oliveira\Documents\CimentoForte\dadosEstabelecimentos\"

diretorio_origem_cidades = "C:\Users\mariana.oliveira\Documents\CimentoForte\dadosCidades\"

##Definir dados a serem importados para a nuvem

diretorio_origem = diretorio_origem_empresas
categoria = 'empresas'

for arquivo_parquet in os.listdir(diretorio_origem):
    
    print(f"Arquivo: {arquivo_parquet}")
    s3_client.upload_file(f"{diretorio_origem}{arquivo_parquet}", 
                   "datalake-cimento_forte", 
                   f"bronze/{categoria}/{arquivo_parquet}")