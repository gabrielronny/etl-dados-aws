import boto3
import time
import utils
import processamento_spark
import dw
from datetime import datetime
import logging
import sys

s3 = boto3.resource('s3')
bucket_dados_brutos = s3.Bucket(utils.getBuckets()[0].get("s3_dados_brutos").replace('s3a://', ''))
fila_processamento = []
fila_processado = []

# DATA DE PROCESSAMENTO
data_processamento = str(datetime.now().strftime("%d_%m_%Y"))


logger = logging.getLogger()
logger.setLevel(logging.INFO)

formatter = logging.Formatter(fmt="%(asctime)s %(name)s.%(levelname)s: %(message)s", datefmt="%Y.%m.%d %H:%M:%S")
handler = logging.StreamHandler(stream=sys.stdout)
handler.setFormatter(formatter)
logger.addHandler(handler)

while True:
    for bucket in bucket_dados_brutos.objects.all():
        if bucket.key not in fila_processado:
            fila_processamento.append(bucket.key)

    i = len(fila_processamento) - 1
    while fila_processamento:
        origem = fila_processamento[i].split('/')[0]
        nome_arquivo = fila_processamento[i].split('/')[1]
        if origem == 'enem':
            if 'MICRODADOS' in nome_arquivo:
                processamento_spark.processar_microdados_enem(nome_arquivo, data_processamento)
            elif 'ITENS_PROVA' in nome_arquivo:
                processamento_spark.processar_dados_itens_prova(nome_arquivo, data_processamento)
        elif origem == 'fies':
            processamento_spark.processar_dados_fies(nome_arquivo, data_processamento)
        elif origem == 'inmet':
            processamento_spark.processar_dados_inmet(nome_arquivo, data_processamento)
        elif origem == 'prouni':
            processamento_spark.processar_dados_prouni(nome_arquivo, data_processamento)
        elif origem == 'iot':
            processamento_spark.processar_dados_iot(nome_arquivo, data_processamento)
        elif origem == 'idd':
            processamento_spark.processar_idd(nome_arquivo, data_processamento)
        elif origem == 'sptrans':
            if 'routes' in nome_arquivo:
                processamento_spark.processar_routes_sptrans(nome_arquivo, data_processamento)
        elif origem == 'fatos':
            if 'fat_idd' in nome_arquivo:
                dw.processar_fat_idd(nome_arquivo, data_processamento)
            elif 'fat_fies' in nome_arquivo:
                dw.processar_fat_fies(nome_arquivo, data_processamento)

        elif origem == 'dimensoes':
            if 'categoria_administrativa' in nome_arquivo:
                dw.processar_dm_categoria_administrativa(nome_arquivo, data_processamento)
            elif 'dm_curso' in nome_arquivo:
                dw.processar_dm_curso(nome_arquivo, data_processamento)
            elif 'dm_estado' in nome_arquivo:
                dw.processar_dm_estado(nome_arquivo, data_processamento)
            elif 'dm_etnia' in nome_arquivo:
                dw.processar_dm_etnia(nome_arquivo, data_processamento)
            elif 'dm_grau_academico' in nome_arquivo:
                dw.processar_dm_grau_academico(nome_arquivo, data_processamento)
            elif 'dm_grau_curso' in nome_arquivo:
                dw.processar_dm_grau_curso(nome_arquivo, data_processamento)
            elif 'dm_instituicao_de_ensino' in nome_arquivo:
                dw.processar_dm_instituicao_ensino(nome_arquivo, data_processamento)
            elif 'dm_modalidade_de_ensino' in nome_arquivo:
                dw.processar_dm_modalidade_ensino(nome_arquivo, data_processamento)
            elif 'dm_municipio' in nome_arquivo:
                dw.processar_dm_municipio(nome_arquivo, data_processamento)
            elif 'dm_organizacao_academica' in nome_arquivo:
                dw.processar_dm_organizacao_academica(nome_arquivo, data_processamento)
            elif 'dm_regiao' in nome_arquivo:
                dw.processar_dm_regiao(nome_arquivo, data_processamento)
            elif 'dm_situacao' in nome_arquivo:
                dw.processar_dm_situacao_inscricao_fies(nome_arquivo, data_processamento)
            elif 'dm_tipo_bolsa' in nome_arquivo:
                dw.processar_dm_tipo_bolsa(nome_arquivo, data_processamento)
            elif 'dm_tipo_escola' in nome_arquivo:
                dw.processar_dm_tipo_escola(nome_arquivo, data_processamento)
            elif 'dm_turno' in nome_arquivo:
                dw.processar_dm_turno(nome_arquivo, data_processamento)

        fila_processado.append(fila_processamento[i])
        fila_processamento.remove(fila_processamento[i])
        i -= 1

    if not fila_processamento:
        logging.info('Sem arquivos para processar')
    time.sleep(60)
