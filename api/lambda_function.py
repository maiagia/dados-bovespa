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
            pDataFrameIO=converterDataFrame_Parquet_Memoria(vBase),
            pEndpoint='http://localhost:4566',
            pId=None,
            pSenha=None,
            pBucket='stage-dados-brutos',
            pRegiao='us-east-1',
            pNomeArquivoNoBucket=vNomeArquivo,
            pLocalExecucao=1
        )

    return {"statusCode": 200, "body": f"Arquivo '{vNomeArquivo}' enviado com sucesso!"}
