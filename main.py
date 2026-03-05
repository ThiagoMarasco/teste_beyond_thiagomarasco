#Bibliotecas
from dotenv import load_dotenv
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from src.aws.crawler import aws_glue_crawler
from src.utils.functions import *
from src.regras_negocio.transform import *
from src.utils.logging_handler import logger

#Variáveis de ambiente
load_dotenv()
DADOS_ENTRADA_XLSX = os.getenv("DADOS_ENTRADA_XLSX")

#SparkSession Builder
spark = SparkSession.builder \
    .appName("ConexaoS3") \
    .config("spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
            "io.delta:delta-spark_2.12:3.0.0") \
    .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider","com.amazonaws.auth.DefaultAWSCredentialsProviderChain")\
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.hadoop.native.lib", "false") \
    .config("spark.hadoop.io.native.lib.available", "false") \
    .getOrCreate()
    
spark.sparkContext.setLogLevel("ERROR")
## =====================================================================================

def part_1(sheets_name: str, sql_file: str, descricao: str) -> None:
    try:
        logger.info(f"Iniciando leitura da planilha {sheets_name}")

        pandas_df = pd.read_excel(DADOS_ENTRADA_XLSX, sheet_name=sheets_name)
        df = spark.createDataFrame(pandas_df)
        df.createOrReplaceTempView(sheets_name)
        df = executar_sql_de_arquivo(
            spark,
            sql_file,
            descricao=descricao
        )
        registrar_erro(df)

        if "erro" in df.columns:
            df = df.filter(col("erro").isNull() | (col("erro") == ""))
            df = df.drop("erro")

        data_processamento = datetime.now().strftime("%Y-%m-%d")

        df = df.withColumn("data_processamento", lit(data_processamento))
        salva_s3_parquet(df, sheets_name, "data_processamento", "raw")

        logger.info(f"Processamento da tabela {sheets_name} finalizado")

    except Exception as e:
        logger.exception(f"Erro no processamento da tabela {sheets_name}: {e}")
        raise
    
def part_2(camada: str, table: str, unique_key: str) -> None:
    try:

        logger.info(f"Iniciando processamento da tabela {table} na camada {camada}")

        df = ler_s3(spark, table, camada)
        df = filtro_chave_unica(df, unique_key)
        merge_salva_delta(df, table, unique_key, spark)

        logger.info(f"Tabela {table} processada com sucesso na camada stage")

    except Exception as e:
        logger.exception(f"Erro na part_2 para tabela {table}: {e}")
        raise

def part_3(camada: str) -> None:
    try:
        logger.info("Iniciando criação da camada analytics")
        
        df_clientes = ler_s3(spark, "clientes", camada, "delta")
        df_enderecos = ler_s3(spark, "enderecos", camada, "delta")
        df_clientes = cliente_ativo(df_clientes)
        df = join_table(df_clientes, df_enderecos, "id_cliente")
        df = calcular_idade(df, "data_nascimento")
        df = analytics_optimization(df, "id_cliente")
        salva_s3_parquet(df, "clientes", "estado", "analytics")

        logger.info("Camada analytics criada com sucesso")
    except Exception as e:
        logger.exception(f"Erro na criação da camada analytics: {e}")
        raise

#======================================================================================
def main():
    #Input - Part 1
    part_1("clientes","valida_clientes","Validação de Clientes")
    part_1("enderecos","valida_enderecos","Validação de Endereços")
    
    #Tratamento - Part 2
    part_2("raw","clientes","id_cliente")
    part_2("raw","enderecos","id_endereco")
    
    #Analytics - Part 3
    part_3("stage")
    
    #Crawler AWS Glue
    aws_glue_crawler()
    
main()

