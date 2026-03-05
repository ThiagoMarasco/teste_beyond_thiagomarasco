import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.regras_negocio.transform import calcular_idade

from pyspark.sql import Row, SparkSession
from datetime import date
from src.regras_negocio.transform import calcular_idade
from src.regras_negocio.transform import cliente_ativo


def test_calcular_idade(spark: SparkSession):

    data = [
        Row(id_cliente=1, data_nascimento="2000-01-01"),
    ]

    df = spark.createDataFrame(data)

    df_result = calcular_idade(df, "data_nascimento")

    resultado = df_result.collect()[0]
    assert "idade" in df_result.columns
    assert resultado.idade >= 20


def test_cliente_ativo(spark: SparkSession):

    data = [
        Row(id_cliente=1, status="ativo"),
        Row(id_cliente=2, status="inativo"),
    ]

    df = spark.createDataFrame(data)

    df_result = cliente_ativo(df)

    resultados = [row.id_cliente for row in df_result.collect()]

    assert 1 in resultados
    assert 2 not in resultados