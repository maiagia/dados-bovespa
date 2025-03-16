# Tech Challenge 3MLET

## Equipe
**Membros:**  
- Kleryton de Souza Maria  
- Lucas Paim de Paula  
- Maiara Giavoni  
- Rafael Tafelli dos Santos 

## **Tech Challenge - Fase 3**

## 🎥 Demonstração do Projeto  
<video width="100%" controls>
  <source src="https://github.com/maiagia/dados-bovespa/raw/main/Entrega%20Fase%203.mp4" type="video/mp4">
  Seu navegador não suporta a reprodução de vídeos.
</video>


Nesta fase, foi desenvolvido um modelo de **Machine Learning** para previsão de dados do mercado financeiro, utilizando dados extraídos e armazenados na **Fase 2**. O objetivo foi aprimorar as análises e fornecer uma interface interativa para interpretação dos resultados.

### **Objetivo do Projeto**

Criar um modelo preditivo para identificar tendências no mercado de ações, utilizando técnicas avançadas de Machine Learning. O pipeline completo inclui coleta de dados, processamento, modelagem e visualização interativa.

### **Atendimento aos Requisitos**

A seguir, indicamos onde cada requisito do **Tech Challenge - Fase 3** foi atendido:

1. **API para coleta de dados em tempo real e armazenamento em Data Lake:**  
   - Reaproveitamos a **API desenvolvida na Fase 2** para coletar dados do mercado financeiro e armazená-los no **AWS S3**.  
   - **Mais detalhes:** Seção **"Tech Challenge - Fase 2"**.

2. **Construção de um modelo de Machine Learning:**  
   - Desenvolvemos um modelo utilizando **Random Forest Regressor** para prever tendências do mercado financeiro.  
   - **Mais detalhes:** Seção **"Construção do Modelo de Machine Learning"**.

3. **Código no GitHub com documentação completa:**  
   - Todo o código está disponível no repositório, junto com este README e demais documentações.  

4. **Storytelling explicando todas as etapas do projeto em vídeo:**  
   - Criamos um vídeo explicativo abordando desde a coleta de dados até a previsão dos resultados.  

5. **Modelo produtivo alimentando uma aplicação ou dashboard:**  
   - Implementamos um **dashboard interativo** que apresenta os resultados das previsões do modelo, permitindo análise dinâmica.  
   - **Mais detalhes:** Seção **"Dashboard Interativo"**.

### **Principais Entregáveis:**

- **Modelo de Machine Learning** baseado em **Random Forest Regressor**.
- **API de coleta de dados**, reaproveitada da **Fase 2**.
- **Armazenamento no AWS S3**, garantindo escalabilidade.
- **Dashboard interativo** para análise das previsões.
- **Storytelling do projeto**, documentado em vídeo.

### **Construção do Modelo de Machine Learning**

#### **Pré-processamento dos Dados:**
- Tratamento de valores nulos e outliers.
- Normalização e padronização das variáveis.
- Seleção das principais características.
- Conversão de formatos para compatibilidade com o modelo.

#### **Divisão dos Dados:**
- Separação em **80% treino** e **20% teste** para garantir uma avaliação justa do modelo.

#### **Escolha do Algoritmo:**
- **Random Forest Regressor** foi escolhido por sua robustez, capacidade de generalização e eficiência na modelagem de séries temporais e dados financeiros.

#### **Treinamento do Modelo:**
- Ajuste de hiperparâmetros, incluindo:
  - Número de trees na Random Forest.
  - Profundidade máxima das trees.
  - Critério de divisão das trees.
- Validação cruzada para evitar overfitting e melhorar a generalização do modelo.
- Ajuste de pesos para lidar com variáveis de maior impacto no mercado.

#### **Avaliação do Modelo:**
- **MAE (Erro Absoluto Médio):** 0.0417
- **MSE (Erro Quadrático Médio):** 0.0101
- **RMSE (Raiz do Erro Quadrático Médio):** 0.1006

Esses resultados indicam que o modelo apresenta um erro baixo e previsões confiáveis, permitindo uma boa adaptação às variações do mercado.

#### **Exportação do Modelo:**
- O modelo foi salvo no formato serializado para futuras execuções e otimizações.
- Implementação de um pipeline automatizado para reentrenamento conforme novos dados forem incorporados.
- Integração direta com o **dashboard interativo** para análise contínua dos resultados.

### **Dashboard Interativo**

O código do projeto inclui a implementação de um dashboard interativo para visualizar as previsões geradas pelo modelo. Esse dashboard permite a análise detalhada das tendências do mercado financeiro e a comparação entre os valores previstos e os valores reais.

---

## **Conclusão sobre o Modelo**

O modelo desenvolvido demonstrou **boa capacidade preditiva**, com baixos índices de erro e uma abordagem escalável para análise financeira. A combinação de **Random Forest Regressor** com um pipeline estruturado permitiu previsões confiáveis, ajudando na tomada de decisão no mercado financeiro.

Pontos-chave do modelo:
- **Baixo erro médio**, indicando alta precisão nas previsões.
- **Adaptação às flutuações do mercado**, garantindo previsões consistentes.
- **Facilidade de reentrenamento**, permitindo melhoria contínua com novos dados.
- **Integração com dashboard**, tornando os insights acessíveis e fáceis de interpretar.

Com essa abordagem, garantimos uma solução **precisa, escalável e eficiente**, agregando valor real à análise do mercado financeiro.

📌 **Repositório:** [GitHub do Projeto](https://github.com/maiagia/dados-bovespa)  


## Tech Challenge - Fase 2
https://github.com/user-attachments/assets/d8f6d0f6-3d8c-4ed4-9d62-667b1078d489

## Descrição do Projeto
Este projeto implementa um pipeline de dados completo para extrair, processar e analisar dados do pregão da B3 utilizando serviços da AWS, como **S3**, **Glue**, **Lambda** e **Athena**. O objetivo é automatizar a ingestão, transformação e análise de dados históricos do mercado de ações da B3.
---

## Arquitetura
A arquitetura do pipeline segue o seguinte fluxo:

![401786235-579b8781-1676-4acb-b018-dd955ae0e8aa](https://github.com/user-attachments/assets/f5b55231-d0c7-4a10-be8f-6e64c7d82033)


1. **Scrap de Dados (Lambda)**: A função Lambda extrai dados do site da B3 e os envia para o bucket S3 de dados brutos.  
2. **Bucket S3 (Dados Brutos)**: Armazena os dados brutos em formato parquet, particionados diariamente.  
3. **Trigger Lambda**: Quando novos arquivos são carregados no S3, uma trigger Lambda inicia o job Glue.  
4. **ETL no Glue**: O job Glue realiza transformações nos dados, como:
   - Agrupamento, sumarização e contagem.
   - Renomeação de colunas.
   - Cálculo da diferença entre datas.
5. **Bucket S3 (Dados Refinados)**: Os dados transformados são salvos em formato **Parquet**, particionados por data e nome da ação.
   * O nome do bucket deve ser único na aws devido a isso, foi nomeado de refined-bovespa.  
7. **Glue Catalog**: Os metadados dos dados refinados são automaticamente catalogados no Glue, criando uma tabela no banco de dados padrão.  
8. **Athena**: Permite consultas SQL interativas e análises dos dados catalogados.
9. **Notebook**: Oferece uma interface interativa para executar consultas SQL diretamente nos dados catalogados no Glue, permitindo análises exploratórias, visualização de resultados e criação de relatórios de forma prática, sem necessidade de ferramentas externas. 

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
Exemplo do código do ETL:
```python
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

# Script generated for node Custom Transform
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    # Importando as bibliotecas necessárias dentro da função
    from pyspark.sql.functions import col, datediff, to_date, current_date
    from awsglue.dynamicframe import DynamicFrame

    # Configura o Spark para usar o parser de data legado
    glueContext.spark_session.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    # Acessa o DynamicFrame específico dentro do DynamicFrameCollection
    dynamic_frame = dfc["ChangeSchema_node1736451072089"]

    # Converte o DynamicFrame para DataFrame para usar as funções Spark
    df = dynamic_frame.toDF()

    # Converte as colunas para 'timestamp' (se necessário)
    df = df.withColumn(
        "DataReferenciaCarteira", 
        to_date(col("DataReferenciaCarteira"), "yyyyMMdd")  # Converte 'DataReferenciaCarteira' para tipo Date
    )

    # Calcula a diferença entre a data atual e a coluna 'DataReferenciaCarteira'
    df = df.withColumn(
        "DiferencaDias", 
        datediff(current_date(), col("DataReferenciaCarteira"))
    )

    # Substitui valores nulos por 0
    df = df.fillna({"DiferencaDias": 0})

    # Reorganiza as colunas para a ordem desejada
    df = df.select(
        "NomeAcao", 
        "DataReferenciaCarteira", 
        "QtdTeoricaTotal", 
        "DiferencaDias", 
        "SomaParticipacaoSetor", 
        "ContagemEmpresas"
    )

    # Converte o DataFrame de volta para DynamicFrame
    transformed_dynamic_frame = DynamicFrame.fromDF(df, glueContext, "transformed_dynamic_frame")

    # Retorna o DynamicFrame dentro de um DynamicFrameCollection
    return DynamicFrameCollection({"transformed_dynamic_frame": transformed_dynamic_frame}, glueContext)
def sparkAggregate(glueContext, parentFrame, groups, aggs, transformation_ctx) -> DynamicFrame:
    aggsFuncs = []
    for column, func in aggs:
        aggsFuncs.append(getattr(SqlFuncs, func)(column))
    result = parentFrame.toDF().groupBy(*groups).agg(*aggsFuncs) if len(groups) > 0 else parentFrame.toDF().agg(*aggsFuncs)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1736304019816 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://stage-dados-brutos/ano=2025/"], "recurse": True}, transformation_ctx="AmazonS3_node1736304019816")

# Script generated for node Change Schema
ChangeSchema_node1736384274380 = ApplyMapping.apply(frame=AmazonS3_node1736304019816, mappings=[("nme_setor", "string", "nme_setor", "string"), ("cod_empresa", "string", "cod_empresa", "string"), ("nme_acao", "string", "NomeAcao", "string"), ("dsc_tipo", "string", "dsc_tipo", "string"), ("val_participacao_setor", "double", "val_participacao_setor", "double"), ("val_participacao_acumulada_setor", "double", "val_participacao_acumulada_setor", "string"), ("qtd_teorica", "double", "qtd_teorica", "string"), ("dt_referencia_carteira", "bigint", "dt_referencia_carteira", "string"), ("qtd_teorica_total", "double", "qtd_teorica_total", "string"), ("qtd_redutor", "double", "qtd_redutor", "string"), ("dt_extracao", "string", "dt_extracao", "timestamp")], transformation_ctx="ChangeSchema_node1736384274380")

# Script generated for node Aggregate
Aggregate_node1736383572625 = sparkAggregate(glueContext, parentFrame = ChangeSchema_node1736384274380, groups = ["NomeAcao", "dt_referencia_carteira"], aggs = [["val_participacao_setor", "sum"], ["cod_empresa", "countDistinct"]], transformation_ctx = "Aggregate_node1736383572625")

# Script generated for node Select Fields
SelectFields_node1736449430913 = SelectFields.apply(frame=ChangeSchema_node1736384274380, paths=["QtdTeoricaTotal", "NomeAcao", "DataReferenciaCarteira", "dt_referencia_carteira", "qtd_teorica_total"], transformation_ctx="SelectFields_node1736449430913")

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1736819163285 = ApplyMapping.apply(frame=SelectFields_node1736449430913, mappings=[("NomeAcao", "string", "right_NomeAcao", "string"), ("dt_referencia_carteira", "string", "right_dt_referencia_carteira", "string"), ("qtd_teorica_total", "string", "right_qtd_teorica_total", "string")], transformation_ctx="RenamedkeysforJoin_node1736819163285")

# Script generated for node Join
Join_node1736449634945 = Join.apply(frame1=Aggregate_node1736383572625, frame2=RenamedkeysforJoin_node1736819163285, keys1=["NomeAcao", "dt_referencia_carteira"], keys2=["right_NomeAcao", "right_dt_referencia_carteira"], transformation_ctx="Join_node1736449634945")

# Script generated for node Change Schema
ChangeSchema_node1736451072089 = ApplyMapping.apply(frame=Join_node1736449634945, mappings=[("NomeAcao", "string", "NomeAcao", "string"), ("dt_referencia_carteira", "string", "DataReferenciaCarteira", "string"), ("`sum(val_participacao_setor)`", "double", "SomaParticipacaoSetor", "string"), ("`count(cod_empresa)`", "long", "`count(cod_empresa)`", "long"), ("right_qtd_teorica_total", "string", "QtdTeoricaTotal", "string")], transformation_ctx="ChangeSchema_node1736451072089")

# Script generated for node Custom Transform
CustomTransform_node1736384318504 = MyTransform(glueContext, DynamicFrameCollection({"ChangeSchema_node1736451072089": ChangeSchema_node1736451072089}, glueContext))

# Script generated for node Select From Collection
SelectFromCollection_node1736385595940 = SelectFromCollection.apply(dfc=CustomTransform_node1736384318504, key=list(CustomTransform_node1736384318504.keys())[0], transformation_ctx="SelectFromCollection_node1736385595940")

# Script generated for node Amazon S3
AmazonS3_node1736387675044 = glueContext.getSink(path="s3://refined-bovespa/tb_transformacao_dados_bovespa/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=["DataReferenciaCarteira", "NomeAcao"], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1736387675044")
AmazonS3_node1736387675044.setCatalogInfo(catalogDatabase="default",catalogTableName="tb_transformacao_dados_bovespa")
AmazonS3_node1736387675044.setFormat("glueparquet", compression="snappy")
AmazonS3_node1736387675044.writeFrame(SelectFromCollection_node1736385595940)
job.commit()
```

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
A função Lambda coleta dados de empresas listadas no índice IBOV, processa as informações e exporta os dados como arquivos Parquet para o S3 em partições diárias.  
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
