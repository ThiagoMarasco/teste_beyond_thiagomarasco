#Bibliotecas
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql import DataFrame
from src.utils.logging_handler import logger

# Função para filtrar por chave única mantendo o registro mais recente
def filtro_chave_unica(df: DataFrame, coluna_id: str) -> DataFrame:
    
    window = Window.partitionBy(coluna_id).orderBy(desc("data_evento"))
    
    df = (
        df.withColumn("rn", row_number().over(window))
          .filter(col("rn") == 1)
          .drop("rn")
    )
    
    return df

def cliente_ativo(df: DataFrame) -> DataFrame:
    return df.filter(col("status") == "ativo")

def join_table(df_1: DataFrame, df_2: DataFrame, coluna_chave: str) -> DataFrame:
    
    # Colunas do primeiro DataFrame
    cols_df1 = set(df_1.columns)
    
    # Seleciona apenas colunas do df_2 que não existem no df_1 (exceto chave)
    cols_df2 = [c for c in df_2.columns if c not in cols_df1 or c == coluna_chave]
    
    df_2_filtered = df_2.select(*cols_df2)

    return df_1.join(df_2_filtered, on=coluna_chave, how="left")

def calcular_idade(df: DataFrame, coluna_nascimento: str) -> DataFrame:
    df = df.withColumn(
        "idade",
        floor(months_between(current_date(), col(coluna_nascimento)) / 12)
    )
    return df

def analytics_optimization(df: DataFrame, coluna_chave: str) -> DataFrame:
    df = df.orderBy(coluna_chave)
    df = df.coalesce(4)
    return df