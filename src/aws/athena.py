import boto3
import time
import os
from dotenv import load_dotenv

# ===============================
# Carregar variáveis de ambiente
# ===============================
load_dotenv()

AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET = os.getenv("S3_BUCKET")

DATABASE = "thiago_marasco"
QUERY = "SELECT * FROM clientes"

OUTPUT_LOCATION = f"s3://bkt-dev1-data-avaliacoes/thiago_marasco/athena_results/"
athena = boto3.client("athena", region_name="sa-east-1")

# ===============================
# 1 - Executar Query
# ===============================

response = athena.start_query_execution(
    QueryString=QUERY,
    QueryExecutionContext={
        "Database": DATABASE
    },
    ResultConfiguration={
        "OutputLocation":"s3://bkt-dev1-data-avaliacoes/thiago_marasco/athena_results/"
    }
)

query_execution_id = response["QueryExecutionId"]

print("Query iniciada:", query_execution_id)

# ===============================
# 2 - Esperar terminar
# ===============================

while True:

    status = athena.get_query_execution(
        QueryExecutionId=query_execution_id
    )

    state = status["QueryExecution"]["Status"]["State"]

    if state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
        print("Status:", state)
        break

    time.sleep(2)

if state != "SUCCEEDED":
    raise Exception("Query falhou!")

# ===============================
# 3 - Buscar resultados
# ===============================

results = athena.get_query_results(
    QueryExecutionId=query_execution_id
)

rows = results["ResultSet"]["Rows"]

# ===============================
# 4 - Converter para CSV
# ===============================

s3 = boto3.client("s3")

file_key = f"thiago_marasco/athena_results/{query_execution_id}.csv"

s3.download_file(
    "bkt-dev1-data-avaliacoes",
    file_key,
    "resultado_athena.csv"
)

print("CSV salvo:")