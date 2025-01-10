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
AmazonS3_node1736304019816 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://stage-dados-brutos"], "recurse": True}, transformation_ctx="AmazonS3_node1736304019816")

# Script generated for node Change Schema
ChangeSchema_node1736384274380 = ApplyMapping.apply(frame=AmazonS3_node1736304019816, mappings=[("nme_setor", "string", "nme_setor", "string"), ("cod_empresa", "string", "cod_empresa", "string"), ("nme_acao", "string", "NomeAcao", "string"), ("dsc_tipo", "string", "dsc_tipo", "string"), ("val_participacao_setor", "string", "val_participacao_setor", "double"), ("val_participacao_acumulada_setor", "string", "val_participacao_acumulada_setor", "string"), ("qtd_teorica", "string", "QtdTeoricaTotal", "string"), ("dt_referencia_carteira", "string", "DataReferenciaCarteira", "string"), ("qtd_teorica_total", "string", "qtd_teorica_total", "string"), ("qtd_redutor", "string", "qtd_redutor", "string"), ("dt_extracao", "string", "dt_extracao", "string")], transformation_ctx="ChangeSchema_node1736384274380")

# Script generated for node Aggregate
Aggregate_node1736383572625 = sparkAggregate(glueContext, parentFrame = ChangeSchema_node1736384274380, groups = ["NomeAcao", "DataReferenciaCarteira"], aggs = [["val_participacao_setor", "sum"], ["cod_empresa", "countDistinct"]], transformation_ctx = "Aggregate_node1736383572625")

# Script generated for node Select Fields
SelectFields_node1736449430913 = SelectFields.apply(frame=ChangeSchema_node1736384274380, paths=["QtdTeoricaTotal", "NomeAcao", "DataReferenciaCarteira"], transformation_ctx="SelectFields_node1736449430913")

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1736449660559 = ApplyMapping.apply(frame=SelectFields_node1736449430913, mappings=[("QtdTeoricaTotal", "string", "right_QtdTeoricaTotal", "string"), ("NomeAcao", "string", "right_NomeAcao", "string"), ("DataReferenciaCarteira", "string", "right_DataReferenciaCarteira", "string")], transformation_ctx="RenamedkeysforJoin_node1736449660559")

# Script generated for node Join
Join_node1736449634945 = Join.apply(frame1=Aggregate_node1736383572625, frame2=RenamedkeysforJoin_node1736449660559, keys1=["NomeAcao", "DataReferenciaCarteira"], keys2=["right_NomeAcao", "right_DataReferenciaCarteira"], transformation_ctx="Join_node1736449634945")

# Script generated for node Change Schema
ChangeSchema_node1736451072089 = ApplyMapping.apply(frame=Join_node1736449634945, mappings=[("DataReferenciaCarteira", "string", "DataReferenciaCarteira", "string"), ("`count(DISTINCT cod_empresa)`", "long", "ContagemEmpresas", "long"), ("right_QtdTeoricaTotal", "string", "QtdTeoricaTotal", "string"), ("`sum(val_participacao_setor)`", "double", "SomaParticipacaoSetor", "string"), ("NomeAcao", "string", "NomeAcao", "string")], transformation_ctx="ChangeSchema_node1736451072089")

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