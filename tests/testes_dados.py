# Databricks notebook source
# MAGIC %md
# MAGIC ## Configuração Inicial

# COMMAND ----------

import pytest
from pyspark.sql import SparkSession
import sys
sys.path.append('/Workspace/Users/santos.gabriela04@edu.pucrs.br/projeto-educadata/config')
from pyspark.sql.functions import *
from myconfig import STORAGE_ACCOUNT, CONTAINER, FINAL_CONTAINER

@pytest.fixture(scope="module")
def spark_session():
    return SparkSession.builder.appName("pytest").getOrCreate()

def read_parquet(spark_session, file_path):
    input_path = f"wasbs://{FINAL_CONTAINER}@{STORAGE_ACCOUNT}.blob.core.windows.net/{file_path}"
    return spark_session.read.parquet(input_path)

@pytest.fixture(scope="module")
def fato_escolas(spark_session):
    return read_parquet(spark_session, "fact/fato_escolas")

@pytest.fixture(scope="module")
def dim_escolas(spark_session):
    return read_parquet(spark_session, "dimensions/dim_escolas")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Dicionário de Schema

# COMMAND ----------

expected_schema = {
    "fato_escolas": {
        "CO_ENTIDADE" : "string",
        "IN_AGUA_POTAVEL" : "int", 
        "IN_ENERGIA_REDE_PUBLICA" : "int", 
        "IN_ESGOTO_REDE_PUBLICA": "int",  
        "IN_LIXO_SERVICO_COLETA": "int",  
        "IN_TRATAMENTO_LIXO_SEPARACAO": "int", 
        "IN_TRATAMENTO_LIXO_REUTILIZA": "int", 
        "IN_TRATAMENTO_LIXO_RECICLAGEM": "int", 
        "IN_BANHEIRO": "int", 
        "IN_BANHEIRO_PNE": "int", 
        "IN_REFEITORIO": "int",
        "IN_ALIMENTACAO": "int",
        "IN_QUADRA_ESPORTES": "int", 
        "IN_QUADRA_ESPORTES_COBERTA": "int", 
        "IN_BIBLIOTECA": "int", 
        "IN_LABORATORIO_CIENCIAS": "int",
        "IN_LABORATORIO_INFORMATICA": "int", 
        "IN_INTERNET": "int", 
        "IN_INTERNET_ALUNOS": "int", 
        "IN_EQUIP_LOUSA_DIGITAL": "int"},
    "dim_escolas":{
        "CO_ENTIDADE" : "string", 
        "NO_ENTIDADE" : "string", 
        "SG_UF": "string", 
        "CO_UF": "string", 
        "CO_MUNICIPIO" : "string", 
        "NO_MUNICIPIO" : "string", 
        "TP_DEPENDENCIA": "string", 
        "TP_LOCALIZACAO" : "string"
    }
}   

# COMMAND ----------

# MAGIC %md
# MAGIC ## Testes

# COMMAND ----------

def validar_schema(df, schema_esperado):
    for column, expected_type in schema_esperado.items():
        actual_type = df.schema[column].dataType.simpleString()
        assert actual_type == expected_type, f"A coluna {column} tem o tipo errado. Valor esperado: {expected_type}, Valor atual: {actual_type}"

def test_schema_fato_escolas(fato_escolas):
    validar_schema(fato_escolas, expected_schema["fato_escolas"])

def test_schema_dim_escolas(dim_escolas):
    validar_schema(dim_escolas, expected_schema["dim_escolas"])

def test_no_nulls(df):
    for column in df.columns:
        null_count = df.filter(col(column).isNull()).count()
        assert null_count == 0, f"A coluna {column} tem valores nulos"

def test_nulos_fato(fato_escolas):
    test_no_nulls(fato_escolas)

def test_nulos_dim(dim_escolas):
    test_no_nulls(dim_escolas)