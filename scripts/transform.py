from pyspark.sql.functions import isnull, when, count, col, lit, countDistinct

from scripts.constants import filtered_columns, num_cols

def filter_active(df):
    return df.filter(col('TP_SITUACAO_FUNCIONAMENTO') == 1)\
             .select(*filtered_columns)\
             .dropDuplicates()
             
def value_transform(df):
    df = df.withColumn(
    "TP_LOCALIZACAO",
    when(col("TP_LOCALIZACAO")==1, "Urbana")
    .when(col("TP_LOCALIZACAO")==2, "Rural")
    .otherwise("Outro")
    ).withColumn(
    "TP_DEPENDENCIA",
    when(col("TP_DEPENDENCIA")==1, "Federal")
    .when(col("TP_DEPENDENCIA")==2, "Estadual")
    .when(col("TP_DEPENDENCIA")==3, "Municipal")
    .when(col("TP_DEPENDENCIA")==4, "Privada")
    .otherwise("Outro")
    )
    for col_name in num_cols:
        df = df.withColumn(col_name, col(col_name).cast("int"))
    return df