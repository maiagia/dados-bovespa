import boto3

glue_job_name = "etl-transformacao-dados-bovespa" 
glue_client = boto3.client('glue')

def lambda_handler(event, context):
    try:
        response = glue_client.start_job_run(JobName=glue_job_name)
        print(f"Job {glue_job_name} iniciado com sucesso! JobRunId: {response['JobRunId']}")
    except Exception as e:
        print(f"Erro ao iniciar o Job Glue: {str(e)}")