import os
from typing import Sequence, Mapping, Any


def getBuckets():
    BUCKETS: Sequence[Mapping[str, Any]] = [
        {
            's3_dados_brutos': os.environ.get('S3_BUCKET_BRUTOS')
        },
        {
            's3_dados_tratados': os.environ.get('S3_BUCKET_TRATADOS')
        },
        {
            's3_logs': os.environ.get('S3_BUCKET_LOGS')
        }
    ]
    return BUCKETS

def getAmbiente():
    ## AMBIENTE AZURE
    AMBIENTE: Sequence[Mapping[str, Any]] = [
        {
            'prod-url': os.environ.get('URL_DATABASE_AZURE_PROD'),
            'user': os.environ.get('USER_DATABASE_AZURE_PROD'),
            'pass': os.environ.get('PASS_DATABASE_AZURE_PROD'),
            'dw-url': os.environ.get('URL_DW_AZURE_PROD')
        },
        {
            'dev-url': os.environ.get('URL_DATABASE_AZURE_DEV'),
            'user': os.environ.get('USER_DATABASE_AZURE_DEV'),
            'pass': os.environ.get('PASS_DATABASE_AZURE_DEV'),
        }
    ]
    return AMBIENTE
