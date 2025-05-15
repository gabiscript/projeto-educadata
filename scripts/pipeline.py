from pyspark.sql.functions import isnull, when, count, col, lit, countDistinct
from pyspark.sql import SparkSession

from scripts.ingestion import connect_blob, read_csv
from scripts.transform import filter_active, value_transform

def process_df(file_path, spark, dbutils):
    connect_blob(spark, dbutils)
    df_raw = read_csv(file_path, spark)
    df_active = filter_active(df_raw)
    df_filtered = value_transform(df_active)
    return df_filtered

def process_year(year, spark, dbutils):
    path = f"microdados_ed_basica_{year}.csv"
    df = process_df(path, spark, dbutils)
    df = df.withColumn("NU_ANO_CENSO", lit(year))
    return df