# Databricks notebook source
# MAGIC %md
# MAGIC ## Configuração Inicial

# COMMAND ----------

import sys
import os
sys.path.append('/Workspace/Users/santos.gabriela04@edu.pucrs.br/projeto-educadata/config')
from pyspark.sql.functions import isnull, when, count, col
from myconfig import STORAGE_ACCOUNT, CONTAINER, FINAL_CONTAINER

try:
    spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.blob.core.windows.net", 
    dbutils.secrets.get(scope="azure-storage", key="storage-account-key")
    )
except Exception as e:
    raise Exception("Erro ao acessar Azure Storage Key. Verifique o Secret Scope: " + str(e))

if not all([STORAGE_ACCOUNT, CONTAINER, FINAL_CONTAINER]):
    raise ValueError("Variáveis de ambiente não configuradas corretamente")

path = f"wasbs://{CONTAINER}@{STORAGE_ACCOUNT}.blob.core.windows.net/microdados_ed_basica_2024.csv"

df = spark.read.csv(path, sep=';', header= True, encoding='ISO-8859-1')


# COMMAND ----------

# MAGIC %md
# MAGIC ## Definição das Colunas do DF

# COMMAND ----------

filtered_columns = [
    # Dados Gerais
    "NU_ANO_CENSO",         # Ano do Censo
    "CO_ENTIDADE",          # Código da escola (único)
    "NO_ENTIDADE",          # Nome da escola
    "CO_UF",                # Código da UF
    "SG_UF",                # Sigla da UF
    "CO_MUNICIPIO",         # Código do município
    "NO_MUNICIPIO",         # Nome do município
    "TP_DEPENDENCIA",       # Tipo de dependência administrativa
    "TP_LOCALIZACAO",       # Localização (urbana/rural)
    "TP_SITUACAO_FUNCIONAMENTO",  # Situação da escola (em funcionamento ou não)
    
    # Infraestrutura escolar
    "IN_AGUA_POTAVEL",  # Água potável
    "IN_ENERGIA_REDE_PUBLICA",  # Energia elétrica da rede pública
    "IN_ESGOTO_REDE_PUBLICA",  # Esgoto sanitário da rede pública
    "IN_LIXO_SERVICO_COLETA",  # Coleta de lixo
    "IN_TRATAMENTO_LIXO_SEPARACAO",  # Separação do lixo
    "IN_TRATAMENTO_LIXO_REUTILIZA",  # Reutilização do lixo
    "IN_TRATAMENTO_LIXO_RECICLAGEM",  # Reciclagem do lixo 
    
    "IN_BANHEIRO",  # Se a escola tem banheiro
    "IN_BANHEIRO_PNE",  # Banheiro acessível a pessoas com deficiência
    "IN_REFEITORIO", # Se a escola tem refeitório
    "IN_ALIMENTACAO",
    "IN_QUADRA_ESPORTES",  # Se a escola tem quadra de esportes
    "IN_QUADRA_ESPORTES_COBERTA",  # Quadra de esportes coberta
    "IN_BIBLIOTECA",  # Biblioteca
    "IN_LABORATORIO_CIENCIAS",  # Laboratório de ciências
    "IN_LABORATORIO_INFORMATICA",  # Laboratório de informática
    
    # Recursos Tech
    "IN_INTERNET", # Acesso a internet na escola
    "IN_INTERNET_ALUNOS", # Acesso a internet para alunos
    "IN_EQUIP_LOUSA_DIGITAL", # Lousa digital

    # Acessibilidade
    "IN_ACESSIBILIDADE_INEXISTENTE", # Situação da acessibilidade
    
]


# COMMAND ----------

# MAGIC %md
# MAGIC ## Organização das Colunas

# COMMAND ----------

df_ativas = df.filter((col("TP_SITUACAO_FUNCIONAMENTO")==1)).select(*filtered_columns) # Selecionando apenas as escolas ativas
df_ativas = df_ativas.dropDuplicates()
df_ativas = df_ativas.dropna(subset=["TP_LOCALIZACAO", "TP_DEPENDENCIA"])

# Transformação de valores

df_ativas = df_ativas.withColumn(
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


# COMMAND ----------

# MAGIC %md
# MAGIC ## Filtros

# COMMAND ----------

# Por tipo de escola 
escolas_federal = df_ativas.filter(col("TP_DEPENDENCIA")=="Federal")
escolas_estadual = df_ativas.filter(col("TP_DEPENDENCIA")=="Estadual")
escolas_municipal = df_ativas.filter(col("TP_DEPENDENCIA")=="Municipal")
escolas_privada = df_ativas.filter(col("TP_DEPENDENCIA")=="Privada")

# Por localização
escolas_urbana = df_ativas.filter(col("TP_LOCALIZACAO")=="Urbana")
escolas_rural = df_ativas.filter(col("TP_LOCALIZACAO")=="Rural")

# Por Região
escolas_por_estado = df_ativas.groupBy("SG_UF").count().orderBy("SG_UF")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Envio para Destino Final no Blob

# COMMAND ----------

output_path = f"wasbs://{FINAL_CONTAINER}@{STORAGE_ACCOUNT}.blob.core.windows.net/silver/escolas_ativas"
df_ativas.write.mode("overwrite").parquet(output_path)