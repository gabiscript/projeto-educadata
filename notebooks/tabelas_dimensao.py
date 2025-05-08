# Databricks notebook source
# MAGIC %md
# MAGIC ## Configuração Inicial

# COMMAND ----------

import sys
import os
sys.path.append('/Workspace/Users/santos.gabriela04@edu.pucrs.br/projeto-educadata/config')
from myconfig import STORAGE_ACCOUNT, CONTAINER, FINAL_CONTAINER

spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.blob.core.windows.net", 
    dbutils.secrets.get(scope="azure-storage", key="storage-account-key")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Definição das Dimensões

# COMMAND ----------

#Dimensão Escola
colunas = [
  "CO_ENTIDADE", 
  "NO_ENTIDADE", 
  "SG_UF", 
  "CO_UF", 
  "CO_MUNICIPIO", 
  "NO_MUNICIPIO", 
  "TP_DEPENDENCIA", 
  "TP_LOCALIZACAO"]

input_path = f"wasbs://{FINAL_CONTAINER}@{STORAGE_ACCOUNT}.blob.core.windows.net/silver/escolas_ativas"
df_ativas = spark.read.parquet(input_path)

dimensao_escolas = df_ativas.select(*colunas).dropDuplicates()

#Dimensão Tempo
coluna_tempo = ["CO_ENTIDADE", "NU_ANO_CENSO"]

dimensao_tempo = df_ativas.select(*coluna_tempo).dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Envio para Destino Final no Blob

# COMMAND ----------

output_path_escola = f"wasbs://{FINAL_CONTAINER}@{STORAGE_ACCOUNT}.blob.core.windows.net/dimensions/dim_escola"
output_path_tempo = f"wasbs://{FINAL_CONTAINER}@{STORAGE_ACCOUNT}.blob.core.windows.net/dimensions/dim_tempo"

dimensao_escolas.write.mode("overwrite").parquet(output_path_escola)
dimensao_tempo.write.mode("overwrite").parquet(output_path_tempo)