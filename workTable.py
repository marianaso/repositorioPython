from pyspark.sql import functions as f
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Pipeline Cimento Forte") \
    .getOrCreate()

##Leitura dos arquivos

estabelecimentos = (
    spark
    .read
    .format("parquet")
    .option("inferSchema", True)
    .option("header", True)
    .option("delimiter", ";")
    .load("s3://datalake-cimento_forte/silver/estabelecimentos/")
)

empresas = (
    spark
    .read
    .format("parquet")
    .option("inferSchema", True)
    .option("header", True)
    .option("delimiter", ";")
    .load("s3://datalake-cimento_forte/silver/empresas/")
)

cidades = (
    spark
    .read
    .format("parquet")
    .option("inferSchema", True)
    .option("header", True)
    .option("delimiter", ";")
    .load("s3://datalake-cimento_forte/silver/cidades/")
)

##Juncoes das tabelas

dados = estabelecimentos.join(cidades, 'municipio', how='inner')
dados = dados.join(empresas, 'cnpj_basico', how='inner')

##Juncao do cnpj em 1 coluna unica
dados = dados.withColumn('cnpj', f.concat(f.col('cnpj_basico'), f.col('cnpj_ordem'), f.col('cnpj_dv') ))

dados
    .write
    .mode("overwrite")
    .format("parquet")
    .save("s3://datalake-cimento_forte/gold/dados-totais")

dados_RS_SC = dados.filter((dados.uf == 'RS') | (dados.uf == 'SC'))

dados_RS_SC
    .write
    .mode("overwrite")
    .format("parquet")
    .save("s3://datalake-cimento_forte/gold/dados-RS_SC")