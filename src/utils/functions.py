from datetime import datetime
import os
from pyspark.sql.functions import *
from delta.tables import DeltaTable
from src.utils.logging_handler import logger
from pyspark.sql import SparkSession

S3_BUCKET = os.getenv("S3_BUCKET")
NOME = os.getenv("NOME")
SOBRENOME = os.getenv("SOBRENOME")

#Spark SQL a partir de arquivo
def executar_sql_de_arquivo(spark: SparkSession,caminho_arquivo_sql: str,descricao: str = "Consulta SPARK.SQL") -> Optional[DataFrame]:
    
    try:
        # Lê o conteúdo do arquivo SQL
        with open(f"consultas/{caminho_arquivo_sql}.sql", "r", encoding="utf-8") as arquivo_sql:
            consulta = arquivo_sql.read()
            
        return sparksql(spark, consulta, descricao)
    
    except FileNotFoundError:
        logger.error(f"Arquivo SQL não encontrado: {caminho_arquivo_sql}.sql")
        return None
    except Exception as e:
        logger.exception(f"Erro ao ler o arquivo SQL {caminho_arquivo_sql}.sql: {str(e)}")
        return None
    
# Consulta Spart nativa
def sparksql(spark: SparkSession, consulta: str, descricao: str = "Consulta SPARK.SQL") -> Optional[DataFrame]:
    try:
        logger.info(f"Executando: {descricao}...")
        df_resultado = spark.sql(consulta)
        logger.info(f"{descricao} executada com sucesso!")
        return df_resultado
    except Exception as e:
        logger.exception(f"Erro ao executar {descricao}: {str(e)}")
        return None
    
# Salva no aws s3
def salva_s3_parquet(df: DataFrame, sheets_name: str, key: str, etapa: str = "raw") -> None:
    
    output_s3_path = f"s3a://{S3_BUCKET}/{NOME}_{SOBRENOME}/{etapa}/{sheets_name}/"
    df.write.mode("overwrite") \
        .partitionBy(key) \
        .parquet(output_s3_path, compression="snappy")

    logger.info(f"Dados salvos na etapa '{etapa}' no S3 em: {output_s3_path}")

def merge_salva_delta(df: DataFrame, sheets_name: str, chave: str, spark: SparkSession) -> None:
    
    path_s3 = f"s3a://{S3_BUCKET}/{NOME}_{SOBRENOME}/stage/{sheets_name}/"
    
    dt_att = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    df = df.withColumn("data_atualizacao", lit(dt_att))
    
    if DeltaTable.isDeltaTable(spark, path_s3):
        
        delta_table = DeltaTable.forPath(spark, path_s3)

        (
            delta_table.alias("target")
            .merge(
                df.alias("source"),
                f"target.{chave} = source.{chave}"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        logger.info(f"Merge realizado com sucesso em {path_s3}")

    else:
        df.write.format("delta").mode("overwrite").save(path_s3)
        logger.info(f"Tabela Delta criada em {path_s3}")

#Pegar dados no aws s3
def ler_s3(spark: SparkSession, sheets_name: str, etapa: str = "raw", format: str = "parquet") -> DataFrame:
    path = f"s3a://{S3_BUCKET}/{NOME}_{SOBRENOME}/{etapa}/{sheets_name}/"
    df = spark.read.format(format).load(path)
    logger.info(f"Dados lidos da etapa '{etapa}' no S3 em: {path}")
    return df

# Função para registrar erros
def registrar_erro(df: DataFrame) -> None:
    logger.info("Registrando erros encontrados durante a validação...")
    for row in df.filter(col("erro").isNotNull()).toLocalIterator():
        if "erro" in df.columns and row["erro"]:
            erros = row["erro"].split(", ")
            for erro in erros:
                partes = erro.split(" ", 1)
                campo = partes[0].strip() if len(partes) > 0 else "desconhecido"
                motivo = partes[1].strip() if len(partes) > 1 else erro.strip()
                valor = getattr(row, campo, "NULL") if campo in df.columns else "NULL"
                
                logger.info(
                    "",
                    extra={
                        "linha": row.id_endereco if hasattr(row, "id_endereco") else row.id_cliente if hasattr(row, "id_cliente") else "NULL",
                        "campo": campo,
                        "valor": valor,
                        "motivo": motivo,
                    },
                )