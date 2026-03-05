#Bibliotecas
import os
import boto3
from src.utils.logging_handler import logger

# carrega .env
S3_BUCKET = os.getenv("S3_BUCKET")
AWS_REGION = os.getenv("AWS_REGION")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
NOME = os.getenv("NOME")
SOBRENOME = os.getenv("SOBRENOME")

crawler_name = f"{NOME}_{SOBRENOME}_crawler"
database_name = f"{NOME}_{SOBRENOME}"
role_arn = f"{database_name}_glue_crawler_role"

s3_target = f"s3://{S3_BUCKET}/{NOME}_{SOBRENOME}/analytics/clientes/"

def aws_glue_crawler():
    glue = boto3.client("glue", region_name=AWS_REGION)
    
    try:
        glue.create_crawler(
            Name=crawler_name,
            Role=role_arn,
            DatabaseName=database_name,
            Targets={
                "S3Targets": [
                    {
                        "Path": s3_target
                    }
                ]
            },
            SchemaChangePolicy={
                "UpdateBehavior": "UPDATE_IN_DATABASE",
                "DeleteBehavior": "DEPRECATE_IN_DATABASE"
            },
            RecrawlPolicy={
                "RecrawlBehavior": "CRAWL_EVERYTHING"
            }
        )

        logger.info("Crawler criado com sucesso")

    except glue.exceptions.AlreadyExistsException:
        logger.info("Crawler já existe")

    # Iniciar crawler
    try:
        glue.start_crawler(Name=crawler_name)
        logger.info("Crawler iniciado")

    except glue.exceptions.CrawlerRunningException:
        logger.warning("Crawler já está rodando")