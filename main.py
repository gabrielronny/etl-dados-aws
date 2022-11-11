import os
import boto3
import time
import utils
import etl
from datetime import datetime

s3 = boto3.resource('s3')
bucket_dados_brutos = s3.Bucket(utils.getBuckets()[0].get("s3_dados_brutos").replace('s3a://', ''))
fila_processamento = []
fila_processado = []

# DATA DE PROCESSAMENTO
data_processamento = str(datetime.now().strftime("%d_%m_%Y"))

while True:
    for bucket in bucket_dados_brutos.objects.all():
        if bucket.key not in fila_processado:
            fila_processamento.append(bucket.key)

    i = len(fila_processamento) - 1
    while fila_processamento:
        origem = fila_processamento[i].split('/')[0]
        nome_arquivo = fila_processamento[i].split('/')[1]
        if origem == 'enem':
            if nome_arquivo in 'MICRODADOS':
                etl.processar_microdados_enem(nome_arquivo, data_processamento)
            elif nome_arquivo in 'ITENS_PROVA':
                etl.processar_dados_itens_prova(nome_arquivo, data_processamento)
        elif origem == 'fies':
            etl.processar_dados_fies(nome_arquivo, data_processamento)
        elif origem == 'inmet':
            etl.processar_dados_inmet(nome_arquivo, data_processamento)
        elif origem == 'prouni':
            etl.processar_dados_prouni(nome_arquivo, data_processamento)
        fila_processado.append(fila_processamento[i])
        fila_processamento.remove(fila_processamento[i])
        i -= 1

    if not fila_processamento:
        print('Sem arquivos para processar')
    time.sleep(60)
