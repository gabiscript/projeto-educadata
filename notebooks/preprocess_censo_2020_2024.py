# Databricks notebook source
# MAGIC %md
# MAGIC # Configuração Inicial
# MAGIC Imports Necessários 

# COMMAND ----------

import sys
import os
sys.path.append('/Workspace/Users/santos.gabriela04@edu.pucrs.br/projeto-educadata/config')
from pyspark.sql.functions import isnull, when, count, col, lit, countDistinct
from myconfig import STORAGE_ACCOUNT, CONTAINER, FINAL_CONTAINER


# COMMAND ----------

# MAGIC %md
# MAGIC # Listas para Configuração do Dataset

# COMMAND ----------

filtered_columns = [
    # General Data
    "NU_ANO_CENSO",         # Census year
    "CO_ENTIDADE",          # Unique school ID
    "NO_ENTIDADE",          # School name
    "CO_UF",                # State code
    "SG_UF",                # State abbreviation
    "CO_MUNICIPIO",         # Municipality code
    "NO_MUNICIPIO",         # Municipality name
    "TP_DEPENDENCIA",       # Type of administrative dependency
    "TP_LOCALIZACAO",       # Indicates if school is located in urban or rural area
    "TP_SITUACAO_FUNCIONAMENTO",  # Indicates whether the school is operating
    
     # School infrastructure
    "IN_AGUA_POTAVEL",                 # Drinking water
    "IN_ENERGIA_REDE_PUBLICA",        # Public electric power
    "IN_ESGOTO_REDE_PUBLICA",         # Public sanitary sewer system
    "IN_LIXO_SERVICO_COLETA",         # Garbage collection service
    "IN_TRATAMENTO_LIXO_SEPARACAO",   # Waste separation
    "IN_TRATAMENTO_LIXO_REUTILIZA",   # Waste reuse
    "IN_TRATAMENTO_LIXO_RECICLAGEM",  # Waste recycling

    "IN_BANHEIRO",                    # School has restrooms
    "IN_BANHEIRO_PNE",                # Accessible restroom for people with disabilities
    "IN_REFEITORIO",                  # School has cafeteria
    "IN_ALIMENTACAO",                 # School provides meals
    "IN_QUADRA_ESPORTES",            # School has sports court
    "IN_QUADRA_ESPORTES_COBERTA",    # Covered sports court
    "IN_BIBLIOTECA",                 # Library available
    "IN_LABORATORIO_CIENCIAS",       # Science lab available
    "IN_LABORATORIO_INFORMATICA",    # Computer lab available

    # Pandemic-related and remote learning resources
    "IN_MEDIACAO_EAD",               # Remote learning
    "IN_MEDIACAO_SEMIPRESENCIAL",    # Hybrid learning
    "IN_MEDIACAO_PRESENCIAL",        # In-person learning

    # Technology and connectivity
    "IN_INTERNET",                   # Internet access available
    "IN_COMPUTADOR",                 # Computer access available
    "IN_DESKTOP_ALUNO",              # Desktop computers available for students
    "IN_COMP_PORTATIL_ALUNO",        # Laptops available for students
    "QT_DESKTOP_ALUNO",              # Number of desktop computers for students
    "QT_COMP_PORTATIL_ALUNO",        # Number of laptops for students
    "IN_EQUIP_MULTIMIDIA",           # Multimedia equipment available
    "IN_EQUIP_TV",                   # TV available

    # Support professionals
    "IN_PROF_PSICOLOGO",             # Psychologist available
    "QT_PROF_PSICOLOGO",             # Number of psychologists
    "IN_PROF_ASSIST_SOCIAL",         # Social worker available
    "QT_PROF_ASSIST_SOCIAL",         # Number of social workers
    "IN_PROF_SAUDE",                 # Health professional available
    "QT_PROF_SAUDE",                 # Number of health professionals
    "IN_PROF_COORDENADOR",           # Pedagogical coordinator available
    "QT_PROF_COORDENADOR",           # Number of pedagogical coordinators

    # Enrollment by education level
    "QT_MAT_INF",                    # Number of students in early childhood education
    "QT_MAT_FUND",                   # Number of students in elementary education
    "QT_MAT_MED"                     # Number of students in high school
]

num_cols = ["QT_MAT_INF", "QT_MAT_FUND", "QT_MAT_MED", "QT_PROF_COORDENADOR", "QT_PROF_SAUDE", "QT_PROF_ASSIST_SOCIAL", "QT_PROF_PSICOLOGO", "QT_COMP_PORTATIL_ALUNO", "QT_DESKTOP_ALUNO"]

years = [2020, 2021, 2022, 2023, 2024]


# COMMAND ----------

# MAGIC %md
# MAGIC # Funções de Definição

# COMMAND ----------

def connect_blob():
    try:
        spark.conf.set(
        f"fs.azure.account.key.{STORAGE_ACCOUNT}.blob.core.windows.net", 
        dbutils.secrets.get(scope="azure-storage", key="storage-account-key")
    )
    except Exception as e:
        raise Exception("Erro ao acessar Azure Storage Key. Verifique o Secret Scope: " + str(e))

    if not all([STORAGE_ACCOUNT, CONTAINER, FINAL_CONTAINER]):
        raise ValueError("Variáveis de ambiente não configuradas corretamente")

def read_csv(file_path):

    path = f"wasbs://{CONTAINER}@{STORAGE_ACCOUNT}.blob.core.windows.net/{file_path}"
    return spark.read.csv(path, sep=';', header= True, encoding='ISO-8859-1')

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

def process_df(file_path):
    connect_blob()
    df_raw = read_csv(file_path)
    df_active = filter_active(df_raw)
    df_filtered = value_transform(df_active)
    return df_filtered

def process_year(year):
    path = f"microdados_ed_basica_{year}.csv"
    df = process_df(path)
    df = df.withColumn("NU_ANO_CENSO", lit(year))
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC # Dataset Final

# COMMAND ----------

dfs = [process_year(year) for year in years]

df_unified = dfs[0]
for df in dfs[1:]:
    df_unified = df_unified.unionByName(df)

df_codes = df_unified.select("CO_ENTIDADE", "NU_ANO_CENSO").dropDuplicates()
select_all_years = df_codes.groupBy("CO_ENTIDADE")\
                    .agg(countDistinct('NU_ANO_CENSO').alias("anos_distintos"))\
                    .filter("anos_distintos = 5")
df_final = df_unified.join(select_all_years, on="CO_ENTIDADE", how="inner")

# COMMAND ----------

# MAGIC %md
# MAGIC # Envio dos Dados para o Blob

# COMMAND ----------


output_path = f"wasbs://{FINAL_CONTAINER}@{STORAGE_ACCOUNT}.blob.core.windows.net/censo_por_ano"
df_final.write.mode("overwrite").parquet(output_path)