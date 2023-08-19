from pyspark.sql.functions import mean, max, min, col, count
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, ArrayType, DoubleType, BooleanType
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Pipeline Cimento Forte") \
    .getOrCreate()

##Leitura dos arquivos empresas

schema = StructType() \
      .add("_c0",StringType(),True) \
      .add("_c1",StringType(),True) \
      .add("_c2",StringType(),True) \
      .add("_c3",StringType(),True) \
      .add("_c4",IntegerType(),True) \
      .add("_c5",StringType(),True) \
      .add("_c6",StringType(),True)

empresas = (
    spark
    .read
    .format("parquet")
    .schema(schema)
    .option("header", True)
    .option("delimiter", ";")
    .load("s3://datalake-cimento_forte/bronze/empresas/")
)

empresasColNames = ['cnpj_basico', 'razao_social_nome_empresarial', 'natureza_juridica', 'qualificacao_do_responsavel', 'capital_social_da_empresa', 'porte_da_empresa', 'ente_federativo_responsavel']

for index, colName in enumerate(empresasColNames):
    empresas = empresas.withColumnRenamed(f"_c{index}", colName)

empresas
    .write
    .mode("overwrite")
    .format("parquet")
    .save("s3://datalake-cimento_forte/silver/empresas/")