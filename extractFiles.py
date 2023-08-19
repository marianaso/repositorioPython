import pyspark
import os
import zipfile
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Pipeline Cimento Forte") \
    .getOrCreate()

##Diretorios de origem e destino dos dados

diretorio_origem_empresas = "C:\Users\mariana.oliveira\Documents\CimentoForte\dadosEmpresasOrigem\"
diretorio_destino_empresas = "C:\Users\mariana.oliveira\Documents\CimentoForte\dadosEmpresas\"

diretorio_origem_estabelecimentos = "C:\Users\mariana.oliveira\Documents\CimentoForte\dadosEstabelecimentosOrigem\"
diretorio_destino_estabelecimentos = "C:\Users\mariana.oliveira\Documents\CimentoForte\dadosEstabelecimentos\"

diretorio_origem_cidades = "C:\Users\mariana.oliveira\Documents\CimentoForte\dadosCidadesOrigem\"
diretorio_destino_cidades = "C:\Users\mariana.oliveira\Documents\CimentoForte\dadosCidades\"

##Definicao das funcoes

def extrair_arquivos_zip(arquivo_zip, diretorio_destino):
    with zipfile.ZipFile(arquivo_zip, 'r') as zip_ref:
        zip_ref.extractall(diretorio_destino)

def csv_to_parquet(arquivo_csv, diretorio_destino_parquet):
    dados = spark.read.csv(arquivo_csv, sep=';')
    dados.write.parquet(diretorio_destino_parquet,mode='overwrite')

##Extracao dos arquivos zip e transformacao em parquet

##Definir o diretorio origem e destino da execucao
diretorio_origem = diretorio_origem_empresas
diretorio_destino = diretorio_destino_empresas
destino_extraido = os.path.join(diretorio_origem, "extract\")
                                
for arquivo_zip in os.listdir(diretorio_origem):

    arquivo_origem = os.path.join(diretorio_origem, arquivo_zip)                                  
    extrair_arquivos_zip(arquivo_origem, destino_extraido)
    print(f'{arquivo_zip} extraido.')


for arquivo_csv in os.listdir(destino_extraido):

    arquivo_origem = os.path.join(diretorio_origem, arquivo_csv)                               
    csv_to_parquet(arquivo_origem, diretorio_destino)
    print(f'{arquivo_csv} parquet salvo.')

spark.stop()

