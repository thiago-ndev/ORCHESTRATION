from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, when, col, struct
from pyspark.sql.types import StringType
import logging
import json

def create_struct(columns):
    logging.info("Processing dataframe")
    struct_fields = []
    for column in columns:
        struct_fields.append(
            when(
                col(column).isNotNull(),
                struct(col(column).cast("string").alias("S"))
            ).alias(column)
        )
    return struct(*struct_fields)

def transform_dynamo(df, columns):
    df = df.select(columns)
    return df.select(create_struct(df.columns).alias("Item"))

spark = SparkSession.builder.appName("TransformaParquet").getOrCreate()

# Leitura do arquivo Parquet do S3
df = spark.read.option("header", "true").csv("s3://dynamodb-poc-bucket70/teste_generico.csv")


# Exemplo de transformação (você pode mudar)
df_filtrado = df.filter("cidade IS NOT NULL")

df_filtrado = transform_dynamo(df_filtrado, ["nome", "idade", "cidade"])

# Escrita em gzip
df_filtrado.write \
    .option("compression", "gzip") \
    .mode("append") \
    .json("s3://dynamodb-poc-bucket70/gzip/")

spark.stop()
