# Tech Challenge 3MLET

## Equipe
**Membros:**  
- Kleryton de Souza Maria  
- Lucas Paim de Paula  
- Maiara Giavoni  
- Rafael Tafelli dos Santos  

## Descrição do Projeto
Este projeto implementa um pipeline de dados completo para extrair, processar e analisar dados do pregão da B3 utilizando serviços da AWS, como **S3**, **Glue**, **Lambda** e **Athena**. O objetivo é automatizar a ingestão, transformação e análise de dados históricos do mercado de ações da B3.
---

## Arquitetura
A arquitetura do pipeline segue o seguinte fluxo:

![401786235-579b8781-1676-4acb-b018-dd955ae0e8aa](https://github.com/user-attachments/assets/9e438ab8-809c-47df-9b8f-b3359d797b1b)


1. **Scrap de Dados (Lambda)**: A função Lambda extrai dados do site da B3 e os envia para o bucket S3 de dados brutos.  
2. **Bucket S3 (Dados Brutos)**: Armazena os dados brutos em formato CSV, particionados diariamente.  
3. **Trigger Lambda**: Quando novos arquivos são carregados no S3, uma trigger Lambda inicia o job Glue.  
4. **ETL no Glue**: O job Glue realiza transformações nos dados, como:
   - Agrupamento, sumarização e contagem.
   - Renomeação de colunas.
   - Cálculo da diferença entre datas.
5. **Bucket S3 (Dados Refinados)**: Os dados transformados são salvos em formato **Parquet**, particionados por data e nome da ação.  
6. **Glue Catalog**: Os metadados dos dados refinados são automaticamente catalogados no Glue, criando uma tabela no banco de dados padrão.  
7. **Athena**: Permite consultas SQL interativas e análises dos dados catalogados.
8. **Notebook**: Oferece uma interface interativa para executar consultas SQL diretamente nos dados catalogados no Glue, permitindo análises exploratórias, visualização de resultados e criação de relatórios de forma prática, sem necessidade de ferramentas externas. 

---

## Requisitos Atendidos

### 1. **Scrap de Dados do Site da B3**
- A função Lambda extrai os dados diretamente do site da B3 e os envia ao bucket S3 de dados brutos. `Lambda request-ibov`

### 2. **Ingestão de Dados no S3**
- Os dados são armazenados em S3 no formato **Parquet**, organizados em partições diárias.

### 3. **Trigger Lambda**
- A Lambda monitora o bucket de dados brutos e aciona o job Glue automaticamente quando novos arquivos são carregados. `Lambda triggerGlueBovespa`

### 4. **Transformações no Glue**
`etl-transformacao-dados-bovespa`.

#### **Agrupamento numérico, sumarização e contagem (5A):**
- **Nó usado:** `Aggregate_node1736383572625`.
- **Operações realizadas:**
  - **Sumarização:** Soma da coluna `val_participacao_setor`.
  - **Contagem:** Contagem distinta de `cod_empresa`.

#### **Renomeação de colunas (5B):**
- **Nó usado:** `ChangeSchema_node1736384274380`.
- **Colunas renomeadas:**
  - `nme_acao` → `NomeAcao`.
  - `dt_referencia_carteira` → `DataReferenciaCarteira`.

#### **Cálculo com campos de data (5C):**
- **Função personalizada:** `MyTransform`.
- **Descrição:** Calcula a diferença de dias entre a data atual (`current_date()`) e a coluna `DataReferenciaCarteira`. O resultado é armazenado em uma nova coluna, `DiferencaDias`.

### 5. **Salvar os Dados Refinados no S3**
- **Local de salvamento:** `s3://refined-bovespa/tb_transformacao_dados_bovespa/`.
- **Partições:** `DataReferenciaCarteira` e `NomeAcao`.

### 6. **Catalogar Dados no Glue Catalog**
- Os dados transformados são automaticamente catalogados no Glue Catalog, permitindo consultas no Athena.

### 7. **Disponibilizar Dados no Athena**
- Como os dados são catalogados automaticamente, eles ficam disponíveis para consultas SQL no Athena.

### 8. **Criação do Notebook no Athena**
- No notebook, inclua o código necessário para manipulação de dados e análises adicionais.

---

## Etapas do Pipeline

### **Job ETL no Glue**
O job foi desenvolvido no AWS Glue Studio e realiza as seguintes operações:
- **Leitura dos Dados**: Importa os dados brutos do S3 no formato **Parquet**.
- **Transformação dos Dados**:
  - Renomeia colunas para adequação do esquema.
  - Agrega e calcula métricas numéricas.
  - Calcula diferenças entre datas.
- **Escrita dos Dados**: Salva os dados transformados em formato **Parquet** no S3, particionados por data e nome da ação.
- **Catalogação**: Atualiza automaticamente o Glue Catalog com os metadados dos dados refinados.

### **Lambda para Acionar o Glue**
A função Lambda é responsável por iniciar o job Glue automaticamente quando novos arquivos são carregados no bucket S3.

Exemplo do código da Lambda:
```python
import boto3

glue_job_name = "etl-transformacao-dados-bovespa"
glue_client = boto3.client('glue')

def lambda_handler(event, context):
    try:
        response = glue_client.start_job_run(JobName=glue_job_name)
        print(f"Job {glue_job_name} iniciado com sucesso! JobRunId: {response['JobRunId']}")
    except Exception as e:
        print(f"Erro ao iniciar o Job Glue: {str(e)}")
```

### **Lambda para Extração de Empresas IBOV**
A função Lambda coleta dados de empresas listadas no índice IBOV, processa as informações e exporta os dados como arquivos Parquet para o S3.  

Exemplo do código da Lambda:
```python
import requests
import pandas as pd
import json
import base64
from constantes import *
from utilidades import MLET3
from numpy import int64, float64
from datetime import datetime
import boto3
import io

def lambda_handler(event, context):
    u = MLET3()

    # Converter payload em base64
    def payload_2_base64(pPayload_Dict) -> base64.b64encode:
        vPayload_Json = json.dumps(pPayload_Dict)
        vPayload_base64 = base64.b64encode(vPayload_Json.encode()).decode()
        return vPayload_base64

    # Requisição para API IBOV
    def empresas_IBOV(pLink_API: str = LINK_API_IBOV, pNumeroPagina: int = 1, pQtd_Itens_Pagina: int = 100, pSegmentoEmpresas: int = SEGMENTO_CONSULTAR_POR_CODIGO) -> tuple:
        # Payload
        vPayload = {
            'language': 'pt-br',
            'pageNumber': pNumeroPagina,
            'pageSize': pQtd_Itens_Pagina,
            'index': 'IBOV',
            'segment': pSegmentoEmpresas
        }
        vLinkAPI = ''.join([pLink_API, payload_2_base64(vPayload)])
        vRequisicao = requests.get(vLinkAPI)
        vCabecalho_Json = vRequisicao.json().get("header", [])
        vEmpresas_Json = vRequisicao.json().get("results", [])
        vTotalPaginas = vRequisicao.json()['page']['totalPages']
        return (vTotalPaginas, vCabecalho_Json, vEmpresas_Json)

    # Converter DataFrame para parquet em memória
    def converterDataFrame_Parquet_Memoria(pDataFrame: pd.DataFrame) -> io.BytesIO:
        vDataFrame_Buffer = io.BytesIO()
        pDataFrame.to_parquet(vDataFrame_Buffer, index=False)
        vDataFrame_Buffer.seek(0)
        return vDataFrame_Buffer

    # Exportar para bucket S3
    def exportarParaBucketS3(pDataFrameIO: io.BytesIO, pEndpoint: str, pId: str, pSenha: str, pRegiao: str, pBucket: str, pNomeArquivoNoBucket: str, pLocalExecucao: int = 0):

        # Quando pLocalExecucao = 0, LocalStack
        # Quando pLocalExecucao = 1, AWS Lambda
        if pLocalExecucao == 0:
            vCliente_S3 = boto3.client('s3', endpoint_url=pEndpoint, aws_access_key_id=pId, aws_secret_access_key=pSenha, region_name=pRegiao)
        else:
            vCliente_S3 = boto3.client('s3')

        vCliente_S3.upload_fileobj(pDataFrameIO, pBucket, pNomeArquivoNoBucket)


    # Pegar empresas via API
    vListaEmpresas_Json = []
    vContadorPagina = 1
    vTotalPaginas = 1

    while vContadorPagina <= vTotalPaginas:
        vResultadoEmpresas = empresas_IBOV(pNumeroPagina=vContadorPagina, pQtd_Itens_Pagina=100, pSegmentoEmpresas=SEGMENTO_CONSULTAR_POR_SETOR_ATUACAO)
        vTotalPaginas = vResultadoEmpresas[0]
        vCabecalhoEmpresas_Json = vResultadoEmpresas[1]
        vListaEmpresas_Json.extend(vResultadoEmpresas[2])
        vContadorPagina += 1

    # DataFrame
    if vListaEmpresas_Json:
        # Converter a lista em DataFrame
        vBase = pd.DataFrame(vListaEmpresas_Json)

        # Adicionar valores do header
        vBase['dt_referencia_carteira'] = int(pd.to_datetime(vCabecalhoEmpresas_Json['date'], format='%d/%m/%y').strftime('%Y%m%d'))
        vBase['qtd_teorica_total'] = float64(vCabecalhoEmpresas_Json['theoricalQty'].replace('.', '').replace(',', '.'))
        vBase['qtd_redutor'] = float64(vCabecalhoEmpresas_Json['reductor'].replace('.', '').replace(',', '.'))
        vBase['dt_extracao'] = datetime.now()

        # Renomear colunas e formatar
        vBase.rename(columns={
            'segment': 'nme_setor',
            'cod': 'cod_empresa',
            'asset': 'nme_acao',
            'type': 'dsc_tipo',
            'part': 'val_participacao_setor',
            'partAcum': 'val_participacao_acumulada_setor',
            'theoricalQty': 'qtd_teorica'
        }, inplace=True)
        vBase.columns = [u.normalizarTexto(i) for i in vBase.columns]

        # Converter colunas
        vBase[['VAL_PARTICIPACAO_SETOR', 'VAL_PARTICIPACAO_ACUMULADA_SETOR', 'QTD_TEORICA']] = vBase[['VAL_PARTICIPACAO_SETOR', 'VAL_PARTICIPACAO_ACUMULADA_SETOR', 'QTD_TEORICA']].apply(lambda x: x.str.replace('.', '').str.replace(',', '.')).astype(float64)

        # Remover excesso de espaços
        vBase = vBase.apply(lambda x: x.str.replace(r'\s+', ' ', regex=True).str.strip() if x.dtypes == 'object' else x)

        # Extraindo ano, mês e dia da data de extração
        ano = vBase['DT_EXTRACAO'].max().year
        mes = vBase['DT_EXTRACAO'].max().month
        dia = vBase['DT_EXTRACAO'].max().day

        # Definindo o caminho particionado no S3
        vNomeArquivo = f"{ano}/{mes:02}/{dia:02}/Empresas_IBOV_{vBase['DT_REFERENCIA_CARTEIRA'].max()}_{vBase['DT_EXTRACAO'].max().strftime('%Y%m%d')}.parquet"

        # Exportar para o S3
        exportarParaBucketS3(
            pDataFrameIO = converterDataFrame_Parquet_Memoria(vBase),
            pEndpoint = r'http://localhost:4566',
            pId = None,
            pSenha = None,
            pBucket = 'stage-dados-brutos',
            pRegiao = 'us-east-1',
            pNomeArquivoNoBucket = vNomeArquivo,
            pLocalExecucao = 1,
            pDiretorio= vBase['DT_EXTRACAO'].max().strftime('ano=%Y/mes=%m/dia=%d')
            )
        
    return {"statusCode": 200, "body": f"Arquivo '{vNomeArquivo}' enviado com sucesso!"}
```
