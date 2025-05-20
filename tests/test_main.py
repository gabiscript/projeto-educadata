import pytest

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.functions import *

from config.myconfig import STORAGE_ACCOUNT, CONTAINER, FINAL_CONTAINER
from scripts.ingestion import read_csv, connect_blob

@pytest.fixture(scope="module")
def spark_session():
    return SparkSession.builder.appName("pytest").getOrCreate()

def read_parquet(spark_session, file_path):
    input_path = f"wasbs://{FINAL_CONTAINER}@{STORAGE_ACCOUNT}.blob.core.windows.net/{file_path}/"
    return spark_session.read.parquet(input_path)

@pytest.fixture(scope="module")
def fato_escolas(spark_session):
    return read_parquet(spark_session, "fact/fato")

@pytest.fixture(scope="module")
def dim_escolas(spark_session):
    return read_parquet(spark_session, "dimensions/dimensao_escolas")

@pytest.fixture(scope="module")
def dim_infra(spark_session):
    return read_parquet(spark_session, "dimensions/dimensao_infra")

expected_schema = {
    "fato": {
        "CO_ENTIDADE": StringType(),
        "NU_ANO_CENSO": IntegerType(),
        "QT_MAT_INF": IntegerType(),
        "QT_MAT_FUND": IntegerType(),
        "QT_MAT_MED": IntegerType(),
        "QT_PROF_COORDENADOR": IntegerType(),
        "QT_PROF_SAUDE": IntegerType(),
        "QT_PROF_ASSIST_SOCIAL": IntegerType(),
        "QT_PROF_PSICOLOGO": IntegerType(),
        "QT_COMP_PORTATIL_ALUNO": IntegerType(),
        "QT_DESKTOP_ALUNO": IntegerType()
    },
    "dim_escolas":{
        "CO_ENTIDADE": StringType(),
        "NU_ANO_CENSO": IntegerType(),
        "NO_ENTIDADE": StringType(),
        "SG_UF": StringType(),
        "CO_UF": StringType(),
        "CO_MUNICIPIO": StringType(),
        "NO_MUNICIPIO": StringType(),
        "TP_DEPENDENCIA": StringType(),
        "TP_LOCALIZACAO": StringType()
    },
    "dim_infra": {
        "CO_ENTIDADE": StringType(),
        "NU_ANO_CENSO": IntegerType(),
        "IN_AGUA_POTAVEL": StringType(),
        "IN_ENERGIA_REDE_PUBLICA": StringType(),
        "IN_ESGOTO_REDE_PUBLICA": StringType(),
        "IN_LIXO_SERVICO_COLETA": StringType(),
        "IN_TRATAMENTO_LIXO_SEPARACAO": StringType(),
        "IN_TRATAMENTO_LIXO_REUTILIZA": StringType(),
        "IN_TRATAMENTO_LIXO_RECICLAGEM": StringType(),
        "IN_BANHEIRO": StringType(),
        "IN_BANHEIRO_PNE": StringType(),
        "IN_REFEITORIO": StringType(),
        "IN_ALIMENTACAO": StringType(),
        "IN_QUADRA_ESPORTES": StringType(),
        "IN_QUADRA_ESPORTES_COBERTA": StringType(),
        "IN_BIBLIOTECA": StringType(),
        "IN_LABORATORIO_CIENCIAS": StringType(),
        "IN_LABORATORIO_INFORMATICA": StringType(),
        "IN_MEDIACAO_EAD": StringType(),
        "IN_MEDIACAO_SEMIPRESENCIAL": StringType(),
        "IN_MEDIACAO_PRESENCIAL": StringType(),
        "IN_INTERNET": StringType(),
        "IN_COMPUTADOR": StringType(),
        "IN_EQUIP_MULTIMIDIA": StringType(),
        "IN_EQUIP_TV": StringType(),
        "IN_PROF_PSICOLOGO": StringType(),
        "IN_PROF_ASSIST_SOCIAL": StringType(),
        "IN_PROF_SAUDE": StringType(),
        "IN_PROF_COORDENADOR": StringType(),
    }
}   

def validate_schema(df, schema):
    df_columns = set(df.columns)
    expected_columns = set(schema.keys())

    missing_columns = expected_columns - df_columns
    extra_columns = df_columns - expected_columns

    assert not missing_columns, f'Colunas faltando: {missing_columns}'
    assert not extra_columns, f'Colunas extras: {extra_columns}'

    for column, expected_type in schema.items():
        actual_type = df.schema[column].dataType
        assert actual_type == expected_type, f"A coluna {column} tem o tipo errado. Valor esperado: {expected_type}, Valor atual: {actual_type}"

def validate_no_nulls(df):
    for column in df.columns:
        null_count = df.filter(col(column).isNull()).count()
        assert null_count == 0, f"A coluna {column} tem valores nulos"

@pytest.mark.parametrize("df_fixture, schema_key", [
   ("fato_escolas", "fato"),
   ("dim_escolas", "dim_escolas"),
   ("dim_infra", "dim_infra")
])

def test_df_schema(df_fixture, schema_key, request):
  df = request.getfixturevalue(df_fixture)
  schema = expected_schema[schema_key]
  validate_schema(df, schema)

@pytest.mark.parametrize("df_fixture", [
   "fato_escolas",
   "dim_escolas",
   "dim_infra"
])

def test_null_values(df_fixture, request):
    df = request.getfixturevalue(df_fixture)
    validate_no_nulls(df)

