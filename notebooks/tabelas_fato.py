# Databricks notebook source
# MAGIC %md
# MAGIC ## Configuração Inicial

# COMMAND ----------

import sys
import os
sys.path.append('/Workspace/Users/santos.gabriela04@edu.pucrs.br/projeto-educadata/config')
<<<<<<< HEAD
=======
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType
>>>>>>> origin/main
from myconfig import STORAGE_ACCOUNT, CONTAINER, FINAL_CONTAINER

spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.blob.core.windows.net", 
    dbutils.secrets.get(scope="azure-storage", key="storage-account-key")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Definição da Tabela Fato

# COMMAND ----------


# Fato 
colunas = ["CO_ENTIDADE",
           "IN_AGUA_POTAVEL", 
           "IN_ENERGIA_REDE_PUBLICA", 
           "IN_ESGOTO_REDE_PUBLICA",  
           "IN_LIXO_SERVICO_COLETA",  
           "IN_TRATAMENTO_LIXO_SEPARACAO", 
           "IN_TRATAMENTO_LIXO_REUTILIZA", 
           "IN_TRATAMENTO_LIXO_RECICLAGEM", 
           "IN_BANHEIRO", "IN_BANHEIRO_PNE", 
           "IN_REFEITORIO","IN_ALIMENTACAO",
           "IN_QUADRA_ESPORTES", 
           "IN_QUADRA_ESPORTES_COBERTA", 
           "IN_BIBLIOTECA", 
           "IN_LABORATORIO_CIENCIAS",
           "IN_LABORATORIO_INFORMATICA", 
           "IN_INTERNET", 
           "IN_INTERNET_ALUNOS", 
           "IN_EQUIP_LOUSA_DIGITAL"]

input_path = f"wasbs://{FINAL_CONTAINER}@{STORAGE_ACCOUNT}.blob.core.windows.net/silver/escolas_ativas"
df_ativas = spark.read.parquet(input_path)
<<<<<<< HEAD

fato_escolas = df_ativas.select(*colunas).dropDuplicates()

=======
fato_escolas = df_ativas.select(*colunas).dropDuplicates()
for column in colunas[1:]:
    fato_escolas = fato_escolas.withColumn(column, col(column).cast(IntegerType()))
>>>>>>> origin/main


# COMMAND ----------

# MAGIC %md
# MAGIC ## Envio para Destino Final

# COMMAND ----------

output_path_fato = f"wasbs://{FINAL_CONTAINER}@{STORAGE_ACCOUNT}.blob.core.windows.net/fact/fato_escola"

fato_escolas.write.mode("overwrite").parquet(output_path_fato)