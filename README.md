Segue o **README completamente em Markdown**, pronto para colocar no `README.md` do repositório.

---

```markdown
# 📊 Data Pipeline – PySpark + AWS S3 + Delta Lake

Pipeline de engenharia de dados desenvolvido em **Python + PySpark**, responsável por realizar ingestão, validação, transformação e disponibilização de dados em um **Data Lake no AWS S3** utilizando arquitetura em camadas.

O pipeline também integra com **AWS Glue Crawler** para catalogação automática dos dados.

---

# 🏗️ Arquitetura do Projeto

O projeto segue uma arquitetura modular para facilitar **manutenção, escalabilidade e testes**.

```

TesteBeyond/
│
├── pipeline.py
│
├── src/
│   ├── aws/
│   │   └── crawler.py
│   │
│   ├── regras_negocio/
│   │   └── transform.py
│   │
│   ├── utils/
│   │   ├── functions.py
│   │   └── logging_handler.py
│
├── consultas/
│   ├── valida_clientes.sql
│   └── valida_enderecos.sql
│
├── dados/
│   └── dados_entrada.xlsx
│
├── tests/
│   └── test_transform.py
│
├── .env
├── requirements.txt
└── README.md

````

---

# 📌 Arquitetura de Dados

O pipeline segue o modelo **Medallion Architecture**.

```

Excel Input
│
▼
RAW (S3 - Parquet)
│
▼
STAGE (Delta Lake)
│
▼
ANALYTICS (Parquet otimizado)
│
▼
AWS Glue Catalog

````

## Camadas

| Camada    | Descrição                                   |
| --------- | ------------------------------------------- |
| RAW       | Dados ingeridos após validação              |
| STAGE     | Dados tratados e atualizados com Delta Lake |
| ANALYTICS | Dados otimizados para análise               |

---

# ⚙️ Decisões Arquiteturais

### PySpark

Utilizado para processamento distribuído e escalabilidade.

### Delta Lake

Usado na camada **stage** para permitir:

* `MERGE UPSERT`
* versionamento
* ACID transactions

### Parquet

Utilizado nas camadas **raw** e **analytics** devido a:

* compressão
* leitura colunar
* melhor performance em queries analíticas

### AWS Glue Crawler

Responsável por:

* catalogar automaticamente datasets no **Glue Data Catalog**
* permitir consultas via **Athena**

### SQL Externalizado

As regras de validação estão em arquivos `.sql` para:

* separar lógica de negócio do código
* facilitar manutenção

---

# 🔧 Setup do Ambiente

## 1️⃣ Clonar o repositório

```bash
git clone <repo>
cd TesteBeyond
```

---

## 2️⃣ Criar ambiente virtual

```bash
python -m venv venv
```

Ativar ambiente virtual.

### Windows

```bash
venv\Scripts\activate
```

### Linux / Mac

```bash
source venv/bin/activate
```

---

## 3️⃣ Instalar dependências

```bash
pip install -r requirements.txt
```

---

# 📦 Dependências do Projeto

Principais bibliotecas utilizadas:

* PySpark
* Delta Lake
* Pandas
* Boto3
* Pytest
* Python Dotenv

---

# 🔑 Variáveis de Ambiente

Crie um arquivo `.env` na raiz do projeto.

```
S3_BUCKET=bkt-dev1-data-avaliacoes
AWS_REGION=sa-east-1
AWS_ACCESS_KEY_ID=<credencial_fornecida>
AWS_SECRET_ACCESS_KEY=<credencial_fornecida>

DADOS_ENTRADA_XLSX=dados/dados_entrada.xlsx

NOME=thiago
SOBRENOME=marasco
```

---

# ▶️ Executando o Pipeline

Para executar o pipeline:

```bash
python pipeline.py
```

Fluxo executado:

1. Leitura do Excel
2. Validação com Spark SQL
3. Gravação na camada RAW
4. Tratamento e UPSERT com Delta Lake
5. Construção da camada Analytics
6. Execução do AWS Glue Crawler

---

# ☁️ Infraestrutura AWS

O projeto inclui um script Python para criação do **Glue Crawler**.

Arquivo:

```
src/aws/crawler.py
```

Esse script utiliza **boto3** para:

* criar crawler
* apontar para o bucket S3
* atualizar o Glue Catalog

---

## Exemplo de criação do crawler

```python
import boto3

client = boto3.client("glue")

client.create_crawler(
    Name="crawler-data-pipeline",
    Role="AWSGlueServiceRole",
    DatabaseName="data_lake",
    Targets={
        "S3Targets": [
            {"Path": "s3://bkt-dev1-data-avaliacoes/"}
        ]
    }
)
```

---

# 🧪 Testes Unitários

Testes foram implementados utilizando **pytest**.

Local:

```
tests/
```

Exemplo:

```
tests/test_transform.py
```

---

## Executar testes

```bash
pytest
```

ou

```bash
python -m pytest
```

---

# 🧪 Exemplo de teste

```python
def test_calcular_idade(spark):
    df = spark.createDataFrame([
        ("2000-01-01",)
    ], ["data_nascimento"])

    df_result = calcular_idade(df, "data_nascimento")

    assert "idade" in df_result.columns
```

---

# 📊 Logs do Pipeline

O projeto possui um **logger centralizado**.

Arquivo:

```
src/utils/logging_handler.py
```

Logs registram:

* execução de queries
* leitura/escrita no S3
* erros de validação
* exceções do pipeline

---

# 📈 Otimizações Implementadas

* Particionamento de dados
* Compressão Snappy
* Delta Lake Merge
* Spark SQL otimizado
* Arquitetura modular
* Logging estruturado

---

# 🚀 Melhorias Futuras

* Orquestração com **Apache Airflow**
* CI/CD com **GitHub Actions**
* Monitoramento com **CloudWatch**
* Data Quality com **Great Expectations**

---

# 👨‍💻 Autor

**Thiago Marasco**
Engenheiro de Dados

```

---

Se quiser, também posso te entregar uma versão **muito mais forte para portfólio / entrevista de Data Engineer** com:

- **diagrama de arquitetura (AWS + Spark)**
- **README nível projeto open-source**
- **badges (Python, Spark, AWS, CI)**
- **exemplo de output do pipeline**
- **estrutura usada em empresas grandes**

Isso deixa seu projeto **muito mais profissional no GitHub**.
```
