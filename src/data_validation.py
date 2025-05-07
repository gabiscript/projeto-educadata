from pyspark.sql.functions import *
from pyspark.sql.types import *

def limpar_dados(df):
    return df

def test_limpeza_funciona():
    sujo = spark.read.csv("dados.csv", header=True)
    limpo = limpar_dados(sujo)
    assert not limpo.columns[0].startswith(" "), "Tem espaço em branco ainda!"