# Tech Challenge 3MLET
**Membros:**<br/> 
Kleryton de Souza Maria, Lucas Paim de Paula,Maiara Giavoni,Rafael Tafelli dos Santos.

Este projeto implementa um pipeline completo para extrair, processar e analisar dados do pregão da B3 utilizando AWS S3, Glue, Lambda e Athena. O objetivo é permitir a ingestão, transformação e análise dos dados históricos do mercado de ações da B3, conforme descrito no desafio.

## Arquitetura

A arquitetura do pipeline segue o seguinte fluxo:

![Diagrama sem nome drawio (6)](https://github.com/user-attachments/assets/579b8781-1676-4acb-b018-dd955ae0e8aa)


1. **Lambda Scraping**: Uma função Lambda é responsável por extrair os dados do site da B3 e enviar para o bucket S3 de dados brutos.
2. **S3 - Dados Brutos**: O bucket S3 armazena os dados extraídos em formato bruto. Esses dados são armazenados em arquivos CSV e organizados por data.
3. **Lambda Trigger Glue**: Sempre que novos arquivos chegam ao bucket de dados brutos, uma trigger Lambda é acionada para iniciar o job de transformação no AWS Glue.
4. **Glue ETL Transformação Bovespa**: O job Glue realiza diversas transformações nos dados, como:
   - Agrupamento de dados numéricos (sum, count, etc).
   - Renomeação de colunas.
   - Cálculo da diferença entre datas.
5. **S3 - Dados Refinados**: Os dados transformados são salvos em formato Parquet, particionados por data e nome/abreviação da ação no bucket S3 de dados refinados.
6. **Glue Catalog**: O Glue Catalog é automaticamente atualizado com os metadados dos dados transformados, criando uma tabela no banco de dados default.
7. **Athena**: O Athena permite a consulta SQL dos dados catalogados e a visualização das análises.
8. **Cliente**: O cliente pode realizar consultas e análises através do Athena.

## Requisitos

### Pipeline Batch Bovespa

1. **Scrap de Dados do Site da B3**:
   - A função Lambda extrai os dados diretamente do site da B3 e envia para o bucket S3.

2. **Ingestão de Dados no S3**:
   - Os dados brutos são armazenados em S3 no formato CSV e convertidos para o formato Parquet.
   - O armazenamento é particionado diariamente.

3. **Lambda para Acionar Glue**:
   - A Lambda aciona automaticamente o job de ETL no Glue sempre que novos dados são enviados para o bucket S3.

4. ## Transformações no Glue

### **Agrupamento numérico, sumarização e contagem (5A):**
- **Realizado no nó:** `Aggregate_node1736383572625`.
- **Operações executadas:**
  - **Sumarização:** Soma da coluna `val_participacao_setor`.
  - **Contagem:** Contagem distinta de `cod_empresa`.
- ✅ **Cumpre o requisito.**

---

### **Renomear colunas (5B):**
- **Realizado no nó:** `ChangeSchema_node1736384274380`.
- **Colunas renomeadas:**
  - `nme_acao` → `NomeAcao`.
  - `dt_referencia_carteira` → `DataReferenciaCarteira`.
- ✅ **Cumpre o requisito.**

---

### **Cálculo com campos de data (5C):**
- **Realizado na função:** `MyTransform`.
- **Descrição:**
  - Calcula a diferença de dias entre a data atual (`current_date()`) e a coluna `DataReferenciaCarteira`.
  - O resultado é armazenado na coluna `DiferencaDias`.
- ✅ **Cumpre o requisito.**

---

## Salvar os dados refinados no S3 particionados por data e nome da ação (R6):
- **Local de salvamento:** `s3://refined-bovespa/tb_transformacao_dados_bovespa/`.
- **Partições:** `DataReferenciaCarteira` e `NomeAcao`.
- ✅ **Cumpre totalmente.**

---

## Catalogar dados no Glue Catalog (R7):
- **Descrição:**
  - O código utiliza `setCatalogInfo` para registrar os dados no Glue Catalog.
  - Cria uma tabela no banco de dados default do Glue Catalog.
- ✅ **Cumpre totalmente.**

---

## Disponibilizar dados no Athena (R8):
- **Descrição:**
  - Como os dados são catalogados automaticamente, eles estão disponíveis no Athena.
- ✅ **Cumpre totalmente.**


### Estrutura do Código

#### Job ETL Glue

O job de transformação ETL foi desenvolvido utilizando o AWS Glue, com as seguintes etapas:

- **Leitura de Dados do S3**: Dados brutos são lidos de um bucket S3 utilizando o formato CSV.
- **Transformação dos Dados**:
  - **Mudança de esquema**: As colunas do DataFrame são renomeadas e ajustadas.
  - **Agregação**: Realiza agrupamento de dados por nome da ação e data de referência da carteira.
  - **Cálculos de Data**: Calcula a diferença em dias entre a data atual e a data de referência da carteira.
- **Escrita dos Dados Transformados no S3**: Os dados transformados são salvos em formato Parquet no S3, particionados por data e nome da ação.
- **Catalogação no Glue Catalog**: O job atualiza o Glue Catalog com os metadados dos dados refinados.

#### Função Lambda

A função Lambda é responsável por acionar o job Glue sempre que novos arquivos são enviados para o bucket S3. A função utiliza o Boto3 para interagir com o AWS Glue.

```python
import boto3
import json

glue_job_name = "etl-transformacao-dados-bovespa" 
glue_client = boto3.client('glue')

def lambda_handler(event, context):
    try:
        response = glue_client.start_job_run(JobName=glue_job_name)
        print(f"Job {glue_job_name} iniciado com sucesso! JobRunId: {response['JobRunId']}")
    except Exception as e:
        print(f"Erro ao iniciar o Job Glue: {str(e)}")
```
## Código do Job Glue

O código do job Glue foi escrito utilizando o AWS Glue Studio, utilizando um script Python com as seguintes transformações:

- Leitura de dados CSV do S3.
- Mudança de esquema das colunas.
- Agregação de dados utilizando funções de agregação do Spark.
- Transformação personalizada com cálculos e renomeações de colunas.
- Escrita dos dados refinados em S3 no formato Parquet e particionado por data e nome da ação.

### Tabela no Glue Catalog
A tabela no Glue Catalog é automaticamente criada e atualizada após o processamento dos dados, permitindo consultas rápidas e eficientes no Athena.

### Athena
Após os dados serem processados e catalogados, o Athena é utilizado para realizar consultas SQL sobre os dados. O Athena permite a análise interativa e a visualização dos dados.

## Como Executar

### Configuração Inicial:
1. Carregue os dados brutos no bucket S3 `s3://stage-dados-brutos/`.
2. A trigger Lambda será acionada automaticamente quando novos arquivos forem enviados.

### Execução do Job ETL:
1. A Lambda iniciará o job Glue automaticamente, realizando todas as transformações e salvando os dados no bucket `s3://refined-bovespa/tb_transformacao_dados_bovespa/`.

### Consultas no Athena:
1. Após a execução do job, você pode acessar os dados no Athena para realizar consultas SQL e análises.

## Conclusão
Este pipeline automatiza a ingestão, transformação e análise dos dados do pregão da B3 utilizando os serviços da AWS. Ele permite que os dados sejam extraídos, processados e armazenados de forma eficiente, tornando-os facilmente acessíveis para análise.

