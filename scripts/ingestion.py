import sys
import os

sys.path.append('/Workspace/Users/santos.gabriela04@edu.pucrs.br/projeto-educadata/config')

from pyspark.sql import SparkSession
from pyspark.sql.functions import isnull, when, count, col, lit, countDistinct
from myconfig import STORAGE_ACCOUNT, CONTAINER, FINAL_CONTAINER

def connect_blob(spark, dbutils):
    try:
        spark.conf.set(
        f"fs.azure.account.key.{STORAGE_ACCOUNT}.blob.core.windows.net", 
        dbutils.secrets.get(scope="azure-storage", key="storage-account-key")
    )
    except Exception as e:
        raise Exception("Erro ao acessar Azure Storage Key. Verifique o Secret Scope: " + str(e))

    if not all([STORAGE_ACCOUNT, CONTAINER, FINAL_CONTAINER]):
        raise ValueError("Variáveis de ambiente não configuradas corretamente")

def read_csv(file_path, spark):

    path = f"wasbs://{CONTAINER}@{STORAGE_ACCOUNT}.blob.core.windows.net/{file_path}"
    return spark.read.csv(path, sep=';', header= True, encoding='ISO-8859-1')
