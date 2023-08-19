from pyspark.sql.functions import mean, max, min, col, count
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, ArrayType, DoubleType, BooleanType
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Pipeline Cimento Forte") \
    .getOrCreate()

##Leitura dos arquivos cidades

cidades = (
    spark
    .read
    .format("parquet")
    .option("inferSchema", True)
    .option("header", True)
    .option("delimiter", ";")
    .load("s3://datalake-cimento_forte/bronze/cidades/")
)

cidadesColNames = ['municipio', 'cidade']

for index, colName in enumerate(cidadesColNames):
    cidades = cidades.withColumnRenamed(f"_c{index}", colName)

cidades
    .write
    .mode("overwrite")
    .format("parquet")
    .save("s3://datalake-cimento_forte/silver/cidades/")