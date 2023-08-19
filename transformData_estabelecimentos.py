from pyspark.sql.functions import mean, max, min, col, count
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, ArrayType, DoubleType, BooleanType
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Pipeline Cimento Forte") \
    .getOrCreate()

##Leitura dos arquivos estabelecimentos

schema = StructType() \
      .add("_c0",StringType(),True) \
      .add("_c1",StringType(),True) \
      .add("_c2",StringType(),True) \
      .add("_c3",IntegerType(),True) \
      .add("_c4",StringType(),True) \
      .add("_c5",StringType(),True) \
      .add("_c6",StringType(),True) \
      .add("_c7",StringType(),True) \
      .add("_c8",StringType(),True) \
      .add("_c9",StringType(),True) \
      .add("_c10",StringType(),True) \
      .add("_c11",StringType(),True) \
      .add("_c12",StringType(),True) \
      .add("_c13",StringType(),True) \
      .add("_c14",StringType(),True) \
      .add("_c15",StringType(),True) \
      .add("_c16",StringType(),True) \
      .add("_c17",StringType(),True) \
      .add("_c18",StringType(),True) \
      .add("_c19",StringType(),True) \
      .add("_c20",StringType(),True) \
      .add("_c21",StringType(),True) \
      .add("_c22",StringType(),True) \
      .add("_c23",StringType(),True) \
      .add("_c24",StringType(),True) \
      .add("_c25",StringType(),True) \
      .add("_c26",StringType(),True) \
      .add("_c27",StringType(),True) \
      .add("_c28",StringType(),True) \
      .add("_c29",StringType(),True) 


estabelecimentos = (
    spark
    .read
    .format("parquet")
    .schema(schema)
    .option("header", True)
    .option("delimiter", ";")
    .load("s3://datalake-cimento_forte/bronze/estabelecimentos/")
)

estabsColNames = ['cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'identificador_matriz_filial', 'nome_fantasia', 'situacao_cadastral', 'data_situacao_cadastral', 'motivo_situacao_cadastral', 'nome_da_cidade_no_exterior', 'pais', 'data_de_inicio_atividade', 'cnae_fiscal_principal', 'cnae_fiscal_secundaria', 'tipo_de_logradouro', 'logradouro', 'numero', 'complemento', 'bairro', 'cep', 'uf', 'municipio', 'ddd_1', 'telefone_1', 'ddd_2', 'telefone_2', 'ddd_do_fax', 'fax', 'correio_eletronico', 'situacao_especial', 'data_da_situacao_especial']

for index, colName in enumerate(estabsColNames):
    estabelecimentos = estabelecimentos.withColumnRenamed(f"_c{index}", colName)

estabelecimentos
    .write
    .mode("overwrite")
    .format("parquet")
    .save("s3://datalake-cimento_forte/silver/estabelecimentos/")