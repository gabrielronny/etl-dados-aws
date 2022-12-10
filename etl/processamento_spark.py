from pyspark import SparkConf
from pyspark.sql import SparkSession

import logging
import sys

from pyspark.sql.functions import lit
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
from pyspark.sql.types import DoubleType

import utils

logger = logging.getLogger()
logger.setLevel(logging.INFO)

formatter = logging.Formatter(fmt="%(asctime)s %(name)s.%(levelname)s: %(message)s", datefmt="%Y.%m.%d %H:%M:%S")
handler = logging.StreamHandler(stream=sys.stdout)
handler.setFormatter(formatter)
logger.addHandler(handler)


logging.info('Sessão do SPARK iniciada')
## Iniciando Sessão no Spark
conf = SparkConf()
conf.set('spark.jars.packages',
         'org.apache.hadoop:hadoop-aws:3.2.2,com.microsoft.azure:spark-mssql-connector_2.12:1.2.0')
conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'com.amazonaws.auth.InstanceProfileCredentialsProvider')
spark = SparkSession.builder.config(conf=conf).getOrCreate()

def processar_dados_fies(nome_arquivo, data_processamento):
    logging.info('Leitura dataset FIES iniciado')
    # Lendo o CSV e tratando o tipo de encoding para a ISO-8859-1
    df_inscricao_fies_bruto = spark.read \
        .option('encoding', 'ISO-8859-1') \
        .option('delimiter', ';') \
        .option('header', 'true') \
        .csv(f'{utils.getBuckets()[0].get("s3_dados_brutos")}/fies/{nome_arquivo}')

    df_inscricao_fies_bruto.createOrReplaceTempView('tb_temp_fies')

    logging.info('Iniciando a equalização do dataset FIES')
    df_inscricao_fies_tratado = spark.sql("""
        SELECT
            cast(trim(`Ano do processo seletivo`) as bigint)                                                          as ANO_PROCESSO_SELETIVO,
            cast(trim(`Semestre do processo seletivo`) as bigint)                                                     as SEMESTRE_PROCESSO_SELETIVO,
            cast(trim(`ID do estudante`) as bigint)                                                                   as ID_DO_ESTUDANTE,
            trim(`Sexo`)                                                                                              as SEXO_DO_ESTUDANTE,
            trim(`Data de Nascimento`)                                                                                as DATA_NASCIMENTO,
            trim(`UF de residência`)                                                                                  as UF_RESIDENCIA,
            trim(`Municipio de residência`)                                                                           as MUNICIPIO_RESIDENCIA,
            trim(`Etnia/Cor`)                                                                                         as ETNIA_COR,
            trim(`Pessoa com deficiência?`)                                                                           as PESSOAL_COM_DEFICIENCIA,
            trim(`Tipo de escola no ensino médio`)                                                                    as TIPO_ESCOLA_ENSINO_MEDIO,
            cast(trim(`Ano conclusão ensino médio`) as bigint)                                                        as ANO_CONCLUSAO_ENSINO_MEDIO,
            trim(`Concluiu curso superior?`)                                                                          as IS_CONCLUIU_CURSO_SUPERIOR,
            trim(`Professor rede pública ensino?`)                                                                    as IS_PROFESSOR_REDE_PUBLICA_ENSINO,
            cast(trim(`Nº de membros Grupo Familiar`) as bigint)                                                      as QTDE_MEMBROS_GRUPO_FAMILIAR,
            cast(replace(trim(`Renda familiar mensal bruta`), ',' , '.')                                              as double) as RENDA_FAMILIAR_MENSAL_BRUTA,
            cast(replace(trim(`Renda mensal bruta per capita`), ',' , '.') as double)                                 as RENDA_MENSAL_BRUTA_PER_CAPITA,
            trim(`Região grupo de preferência`)                                                                       as REGIAO_GRUPO_DE_PREFERENCIA,
            trim(`UF`)                                                                                                as UF,
            cast(trim(`Cod.Microrregião`) as bigint)                                                                  as COD_MICROREGIAO,
            trim(`Microrregião`)                                                                                      as MICROREGIAO,
            cast(trim(`Cod.Mesorregião`) as bigint)                                                                   as COD_MISORREGIAO,
            trim(`Mesorregião`)                                                                                       as MISORREGIAO,
            cast(trim(`Conceito de curso do GP`) as bigint)                                                           as CONCEITO_CURSO_DO_GP,
            trim(`Área do conhecimento`)                                                                              as AREA_CONHECIMENTO,
            trim(`Subárea do conhecimento`)                                                                           as SUBAREA_CONHECIMENTO,
            cast(trim(`Cod. do Grupo de preferência`) as bigint)                                                      as COD_GRUPO_PREFERENCIA,
            cast(replace(trim(`Nota Corte Grupo Preferência`), ',' , '.') as double)                                  as NOTA_CORTE_GRUPO_PREFERENCIA,
            cast(trim(`Opções de cursos da inscrição`) as bigint)                                                     as OPCOES_CURSOS_DA_INSCRICAO,
            trim(`Nome mantenedora`)                                                                                  as NOME_MANTENEDORA,
            trim(`Natureza Jurídica Mantenedora`)                                                                     as NATUREZA_JURIDICA_MANTENEDORA,
            trim(`CNPJ da mantenedora`)                                                                               as CNPJ_MANTENEDORA,
            cast(trim(`Código e-MEC da Mantenedora`) as bigint)                                                       as COD_EMEC_MANTENEDORA,
            trim(`Nome da IES`)                                                                                       as NOME_DA_IES,
            cast(trim(`Código e-MEC da IES`) as bigint)                                                               as COD_EMEC_DA_IES,
            trim(`Organização Acadêmica da IES`)                                                                      as ORGANIZACAO_ACADEMICA_DA_IES,
            trim(`Município da IES`)                                                                                  as MUNICIPIO_DA_IES,
            trim(`UF da IES`)                                                                                         as UF_DA_IES,
            trim(`Nome do Local de oferta`)                                                                           as NOME_DO_LOCAL_DE_OFERTA,
            cast(trim(`Código do Local de Oferta`) as bigint)                                                         as COD_LOCAL_DE_OFERTA,
            trim(`Munícipio do Local de Oferta`)                                                                      as MUNICIPIO_DO_LOCAL_DE_OFERTA,
            trim(`UF do Local de Oferta`)                                                                             as UF_DO_LOCAL_DE_OFERTA,
            cast(trim(`Código do curso`) as bigint)                                                                   as COD_CURSO,
            trim(`Nome do curso`)                                                                                     as NOME_CURSO,
            trim(`Turno`)                                                                                             as TURNO_CURSO,
            trim(`Grau`)                                                                                              as GRAU_CURSO,
            cast(trim(`Conceito`) as bigint)                                                                          as COD_CONCEITO,
            cast(replace(trim(`Média nota Enem`), ',' , '.')                                                          as double) as MEDIA_NOTA_ENEM,
            cast(trim(`Ano do Enem`) as bigint)                                                                       as ANO_DO_ENEM,
            cast(trim(`Redação`) as bigint)                                                                           as NOTA_REDACAO,
            cast(replace(trim(`Matemática e suas Tecnologias`), ',' , '.') as double)                                 as NOTA_MATEMATICA_E_SUAS_TECNOLOGIAS,
            cast(replace(trim(`Linguagens, Códigos e suas Tec`), ',' , '.') as double)                                as NOTA_LINGUAGENS_E_CODICOS_E_SUAS_TECNOLOGIAS,
            cast(replace(trim(`Ciências Natureza e suas Tec`), ',' , '.') as double)                                  as NOTA_CIENCIA_NATUREZA_E_SUAS_TECNOLOGIAS,
            cast(replace(trim(`Ciências Humanas e suas Tec`), ',' , '.') as double)                                   as NOTA_CIENCIA_HUMANAS_E_SUAS_TECNOLOGIAS,
            trim(`Situação Inscrição Fies`)                                                                           as SITUACAO_INSCRICAO_FIES,
            cast(replace(trim(`Percentual de financiamento`), ',' , '.') as double)                                   as PERCENTUAL_DE_FINANCIAMENTO,
            trim(`Semestre do financiamento`)                                                                         as SEMESTRE_DO_FINANCIAMENTO,
            cast(trim(`Qtde semestre financiado`) as bigint)                                                          as QTDE_SEMESTRE_FINANCIADO
        FROM tb_temp_fies
    """)
    logging.info('Equalização do dataset FIES finalizado')

    df_inscricao_fies_tratado = df_inscricao_fies_tratado.withColumn('DATA_PROCESSAMENTO',
                                                                     lit(data_processamento.replace('_', '/')))
    logging.info('Iniciando upload do dataset FIES para S3_DADOS_TRATADOS')
    # Salvando o arquivo processado com os dados de inscrição no fies tratados na S3
    df_inscricao_fies_tratado \
        .coalesce(1) \
        .write \
        .mode('append') \
        .option('encoding', 'UTF-8') \
        .csv(f'{utils.getBuckets()[1].get("s3_dados_tratados")}/{data_processamento}/fies/')
    logging.info('Upload do dataset fies para S3_DADOS_TRATADOS finalizado')


    logging.info(f'Iniciando o insert no database FIES')

    df_inscricao_fies_tratado.write.mode("append") \
        .format("jdbc") \
        .option('url', f'{utils.getAmbiente()[0].get("prod-url")}') \
        .option("dbtable", 'FIES') \
        .option("user", f'{utils.getAmbiente()[0].get("user")}') \
        .option("password", f'{utils.getAmbiente()[0].get("pass")}') \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .save()

    logging.info('Insert no database FIES finalizado')
    logging.info('Removendo dataframe FIES da memória')
    # Liberando o dataframe da memória e disco

    df_inscricao_fies_tratado.unpersist()
    df_inscricao_fies_bruto.unpersist()
    spark.catalog.dropTempView("tb_temp_fies")


    logging.info('Processamento FIES finalizado')


def processar_dados_prouni(nome_arquivo, data_processamento):
    # Dados do prouni
    logging.info('Leitura dataset PROUNI iniciado')
    df_prouni_bruto = spark.read \
        .option('encoding', 'ISO-8859-1') \
        .option('delimiter', ';') \
        .option('header', 'true') \
        .csv(f'{utils.getBuckets()[0].get("s3_dados_brutos")}/prouni/{nome_arquivo}')
    logging.info('Leitura dataset PROUNI finalizado')

    df_prouni_bruto.createOrReplaceTempView('tb_temp_prouni')

    logging.info('Iniciando a equalização do dataset prouni')

    df_prouni_tratado = spark.sql("""
        SELECT
            cast(trim(`ANO_CONCESSAO_BOLSA`) as bigint)                                                               as ANO_CONCESSAO_BOLSA,
            cast(trim(`CODIGO_EMEC_IES_BOLSA`) as bigint)                                                             as CODIGO_EMEC_IES_BOLSA,
            trim(`NOME_IES_BOLSA`)                                                                                    as NOME_IES_BOLSA,
            trim(`TIPO_BOLSA`)                                                                                        as TIPO_BOLSA,
            trim(`MODALIDADE_ENSINO_BOLSA`)                                                                           as MODALIDADE_ENSINO_BOLSA,
            trim(`NOME_CURSO_BOLSA`)                                                                                  as NOME_CURSO_BOLSA,
            trim(`NOME_TURNO_CURSO_BOLSA`)                                                                            as NOME_TURNO_CURSO_BOLSA ,
            trim(`CPF_BENEFICIARIO`)                                                                                  as CPF_BENEFICIARIO,
            trim(`SEXO_BENEFICIARIO`)                                                                                 as SEXO_BENEFICIARIO,
            trim(`RACA_BENEFICIARIO`)                                                                                 as RACA_BENEFICIARIO,
            trim(`DATA_NASCIMENTO`)                                                                                   as DATA_NASCIMENTO,
            trim(`BENEFICIARIO_DEFICIENTE_FISICO`)                                                                    as IS_BENEFICIARIO_DEFICIENTE_FISICO,
            trim(`REGIAO_BENEFICIARIO`)                                                                               as REGIAO_BENEFICIARIO,
            trim(`UF_BENEFICIARIO`),
            trim(`MUNICIPIO_BENEFICIARIO`)
        FROM tb_temp_prouni
    """)

    logging.info('Equalização do dataset prouni finalizado')

    df_prouni_tratado = df_prouni_tratado.withColumn('DATA_PROCESSAMENTO',
                                                     lit(data_processamento.replace('_', '/')))

    logging.info('Iniciando upload do dataset prouni para S3_DADOS_TRATADOS')
    df_prouni_tratado \
        .coalesce(1) \
        .write \
        .mode('append') \
        .option('encoding', 'ISO-8859-1') \
        .csv(f'{utils.getBuckets()[1].get("s3_dados_tratados")}/{data_processamento}/prouni/')
    logging.info('Upload do dataset prouni para S3_DADOS_TRATADOS finalizado')


    logging.info('Iniciando o insert no database PROUNI')
    df_prouni_tratado.write.mode("append") \
        .format("jdbc") \
        .option('url', f'{utils.getAmbiente()[0].get("prod-url")}') \
        .option("dbtable", 'PROUNI') \
        .option("user", f'{utils.getAmbiente()[0].get("user")}') \
        .option("password", f'{utils.getAmbiente()[0].get("pass")}') \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .save()
    logging.info('Insert no database PROUNI finalizado')

    # Liberando o dataframe da memória e disco
    logging.info('Removendo dataframe PROUNI da memória')

    df_prouni_tratado.unpersist()
    df_prouni_bruto.unpersist()
    spark.catalog.dropTempView("tb_temp_prouni")
    logging.info('Processamento PROUNI finalizado')


def processar_dados_inmet(nome_arquivo, data_processamento):

    # Dados de clima Inmet
    logging.info('Leitura dataset INMET iniciado')
    df_inmet_bruto = spark.read \
        .option('encoding', 'ISO-8859-1') \
        .option('delimiter', ';') \
        .option('header', 'true') \
        .csv(
        f'{utils.getBuckets()[0].get("s3_dados_brutos")}/inmet/{nome_arquivo}')
    logging.info('Leitura dataset inmet finalizado')

    df_inmet_bruto.createOrReplaceTempView('tb_temp_inmet')

    logging.info('Iniciando a equalização do dataset INMET')

    df_inmet_tratado = spark.sql("""
        SELECT
            trim(`Data`)                                                                                              as DATA,
            replace(trim(`Hora UTC`), ',', '.' )                                                                      as HORA_UTC,
            replace(trim(`PRECIPITAÇÃO TOTAL, HORÁRIO (mm)`), ',', '.' )                                              as PRECIPTACAO_TOTAL,
            replace(trim(`PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)`), ',', '.' )                         as PRESSAO_ATMOSFERICA_NIVEL_ESTACAO,
            replace(trim(`PRESSÃO ATMOSFERICA MAX.NA HORA ANT. (AUT) (mB)`), ',', '.' )                               as PRESSAO_ATMOSFERICA_MAX,
            replace(trim(`PRESSÃO ATMOSFERICA MIN. NA HORA ANT. (AUT) (mB)`), ',', '.' )                              as PRESSAO_ATMOSFERICA_MIN,
            replace(trim(`RADIACAO GLOBAL (Kj/m²)`), ',', '.' )                                                       as RADIACAO_GLOBAL,
            replace(trim(`TEMPERATURA DO PONTO DE ORVALHO (°C)`), ',', '.' )                                          as TEMPERATURA_PONTO_ORVALHO,
            replace(trim(`TEMPERATURA MÁXIMA NA HORA ANT. (AUT) (°C)`), ',', '.' )                                    as TEMPERATURA_MAX,
            replace(trim(`TEMPERATURA MÍNIMA NA HORA ANT. (AUT) (°C)`), ',', '.' )                                    as TEMPERATURA_MIN,
            replace(trim(`TEMPERATURA ORVALHO MAX. NA HORA ANT. (AUT) (°C)`), ',', '.' )                              as TEMPERATURA_ORVALHO_MAX,
            trim(`UMIDADE REL. MAX. NA HORA ANT. (AUT) (%)`)                                                          as UMIDADE_RELATIVA_MAX,
            trim(`UMIDADE REL. MIN. NA HORA ANT. (AUT) (%)`)                                                          as UMIDADE_RELATIVA_MIN,
            trim(`UMIDADE RELATIVA DO AR, HORARIA (%)`)                                                               as UMIDADE_RELATIVA_AR,
            trim(`VENTO, DIREÇÃO HORARIA (gr) (° (gr))`)                                                              as VENTO_DIRECAO_HORARIA,
            replace(trim(`VENTO, RAJADA MAXIMA (m/s)`), ',', '.' )                                                    as VENTO_REJADA_MAXIMA,
            replace(trim(`VENTO, VELOCIDADE HORARIA (m/s)`), ',', '.' )                                               as VELOCIDADE_VENTO
        FROM tb_temp_inmet
    """)

    logging.info('Equalização do dataset INMET finalizado')

    df_inmet_tratado = df_inmet_tratado.withColumn('DATA_PROCESSAMENTO',
                                                    lit(data_processamento.replace('_', '/')))

    logging.info('Iniciando upload do dataset INMET para S3_DADOS_TRATADOS')
    df_inmet_tratado \
        .coalesce(1) \
        .write \
        .mode('append') \
        .option('encoding', 'ISO-8859-1') \
        .csv(f'{utils.getBuckets()[1].get("s3_dados_tratados")}/{data_processamento}/inmet/')
    logging.info('Upload do dataset INMET para S3_DADOS_TRATADOS finalizado')

    logging.info('Iniciando o insert no database INMET')
    df_inmet_tratado.write.mode("append") \
        .format("jdbc") \
        .option('url', f'{utils.getAmbiente()[0].get("prod-url")}') \
        .option("dbtable", 'INMET_SP') \
        .option("user", f'{utils.getAmbiente()[0].get("user")}') \
        .option("password", f'{utils.getAmbiente()[0].get("pass")}') \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .save()
    logging.info('Insert no database INMET finalizado')

    # Liberando o dataframe da memória e disco
    logging.info('Removendo dataframe INMET da memória')
    df_inmet_tratado.unpersist()
    df_inmet_bruto.unpersist()
    spark.catalog.dropTempView("tb_temp_inmet")


def processar_dados_itens_prova(nome_arquivo, data_processamento):


    # Itens prova ENEM
    logging.info('Leitura dataset ENEM_ITENS_PROVA iniciado')
    df_enem_itens_prova_bruto = spark.read \
        .option('delimiter', ';') \
        .option('header', 'true') \
        .option('encoding', 'ISO-8859-1') \
        .csv(f'{utils.getBuckets()[0].get("s3_dados_brutos")}/enem/{nome_arquivo}')
    logging.info('Leitura dataset INMET finalizado')

    df_enem_itens_prova_bruto.createOrReplaceTempView('tb_temp_itens_prova')

    logging.info('Iniciando a equalização do dataset ENEM_ITENS_PROVA')

    df_enem_itens_prova_tratado = spark.sql("""
        SELECT
            trim(`CO_POSICAO`)                                                                                        as COD_POSICAO,
            trim(`SG_AREA`)                                                                                           as SG_AREA,
            trim(`CO_ITEM`)                                                                                           as COD_ITEM,
            trim(`TX_GABARITO`)                                                                                       as DESC_GABARITO,
            trim(`CO_HABILIDADE`)                                                                                     as COD_HABILIDADE,
            trim(`IN_ITEM_ABAN`)                                                                                      as ITEN_ABANDONO,
            trim(`TX_MOTIVO_ABAN`)                                                                                    as DESC_MOTIVO_ABANDONO,
            trim(`NU_PARAM_A`)                                                                                        as NUM_PARAMETRO_A,
            trim(`NU_PARAM_B`)                                                                                        as NUM_PARAMETRO_B,
            trim(`NU_PARAM_C`)                                                                                        as NUM_PARAMETRO_C,
            trim(`CO_PROVA`)                                                                                          as COD_PROVA,
            trim(`TX_COR`)                                                                                            as DESC_COR_PROVA,
            trim(`TP_LINGUA`)                                                                                         as COD_TIPO_LINGUA,
            trim(`IN_ITEM_ADAPTADO`)                                                                                  as COD_ITEM_ADPTADO
        FROM tb_temp_itens_prova
    """)

    df_enem_itens_prova_tratado = df_enem_itens_prova_tratado.withColumn('DATA_PROCESSAMENTO',
                                                                        lit(data_processamento.replace('_', '/')))

    logging.info('Iniciando upload do dataset ENEM_ITENS_PROVA para S3_DADOS_TRATADOS')
    df_enem_itens_prova_tratado \
        .coalesce(1) \
        .write \
        .mode('append') \
        .csv(f'{utils.getBuckets()[1].get("s3_dados_tratados")}/{data_processamento}/enem/itens_prova/')
    logging.info('Upload do dataset ENEM_ITENS_PROVA para S3_DADOS_TRATADOS finalizado')

    logging.info('Iniciando o insert no database ENEM_ITENS_PROVA')
    df_enem_itens_prova_tratado.write.mode("append") \
        .format("jdbc") \
        .option('url', f'{utils.getAmbiente()[0].get("prod-url")}') \
        .option("dbtable", 'ENEM_ITENS_PROVA') \
        .option("user", f'{utils.getAmbiente()[0].get("user")}') \
        .option("password", f'{utils.getAmbiente()[0].get("pass")}') \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .save()
    logging.info('Insert no database ENEM_ITENS_PROVA finalizado')

    # Liberando o dataframe da memória e disco
    logging.info('Removendo dataframe ENEM_ITENS_PROVA da memória')
    df_enem_itens_prova_tratado.unpersist()
    df_enem_itens_prova_bruto.unpersist()
    spark.catalog.dropTempView("tb_temp_itens_prova")
    logging.info('Processamento ENEM_ITENS_PROVA finalizado')

def processar_microdados_enem(nome_arquivo, data_processamento):

    logging.info('Leitura dataset ENEM_MICRODADOS iniciado')
    df_enem_microdados_bruto = spark.read \
        .option('delimiter', ';') \
        .option('header', 'true') \
        .option('encoding', 'ISO-8859-1') \
        .csv(f'{utils.getBuckets()[0].get("s3_dados_brutos")}/enem/{nome_arquivo}')
    logging.info('Leitura dataset ENEM_MICRODADOS finalizado')


    df_enem_microdados_bruto.createOrReplaceTempView('tb_temp_microdados')

    logging.info('Iniciando a equalização do dataset ENEM_MICRODADOS')

    df_enem_microdados_tratado = spark.sql("""
        SELECT
            cast(trim(`NU_INSCRICAO`)                                                                                 as string) as NUMERO_INSCRICAO,
            cast(trim(`NU_ANO`) as bigint)                                                                            as ANO,
            trim(`TP_FAIXA_ETARIA`)                                                                                   as TP_FAIXA_ETARIA,
            trim(`TP_SEXO`)                                                                                           as TP_SEXO,
            trim(`TP_ESTADO_CIVIL`)                                                                                   as COD_ESTADO_CIVIL,
            cast(trim(`TP_COR_RACA`) as int)                                                                          as COD_COR_RACA,
            cast(trim(`TP_NACIONALIDADE`) as int)                                                                     as COD_NACIONALIDADE,
            cast(trim(`TP_ST_CONCLUSAO`) as int)                                                                      as SEMESTRE_CONCLUSAO,
            cast(trim(`TP_ANO_CONCLUIU`) as int)                                                                      as ANO_CONCLUSAO,
            cast(trim(`TP_ESCOLA`) as int)                                                                            as TIPO_ESCOLA,
            cast(trim(`TP_ENSINO`) as int)                                                                            as TIPO_ENSINO,
            cast(trim(`IN_TREINEIRO`) as int)                                                                         as IN_TREINEIRO,
            cast(trim(`CO_MUNICIPIO_ESC`) as bigint)                                                                  as COD_MUNICIPIO_ESCOLAR,
            trim(`NO_MUNICIPIO_ESC`)                                                                                  as NOME_MUNICIPIO_ESCOLAR,
            cast(trim(`CO_UF_ESC`) as bigint)                                                                         as COD_UF_ESCOLAR,
            trim(`SG_UF_ESC`)                                                                                         as SIGLA_UF_ESCOLAR,
            cast(trim(`TP_DEPENDENCIA_ADM_ESC`) as int)                                                               as COD_TIPO_DEPENDENCIA_ADM_ESCOLAR,
            cast(trim(`TP_LOCALIZACAO_ESC`) as int)                                                                   as COD_LOCALIZACAO_ESCOLAR,
            cast(trim(`TP_SIT_FUNC_ESC`) as int)                                                                      as COD_SITUACAO_FUNCIONAMENTO_ESCOLAR,
            cast(trim(`CO_MUNICIPIO_PROVA`) as bigint)                                                                as COD_MUNICIPIO_PROVA,
            cast(trim(`CO_UF_PROVA`) as int)                                                                          as COD_UF_PROVA,
            trim(`NO_MUNICIPIO_PROVA`)                                                                                as NOME_MUNICIPIO_PROVA,
            trim(`SG_UF_PROVA`)                                                                                       as SIGLA_UF_PROVA,
            cast(trim(`TP_PRESENCA_CN`) as int)                                                                       as COD_PRESENCA_PROVA_CIENCIA_NATUREZA,
            cast(trim(`TP_PRESENCA_CH`) as int)                                                                       as COD_PRESENCA_CIENCIAS_HUMANAS,
            cast(trim(`TP_PRESENCA_LC`) as int)                                                                       as COD_PRESENCA_LINGUAGENS_CODIGOS,
            cast(trim(`TP_PRESENCA_MT`) as int)                                                                       as COD_PRESENCA_MATEMATICA,
            cast(trim(`CO_PROVA_CN`) as int)                                                                          as COD_TIPO_PROVA_CIENCIA_NATUREZA,
            cast(trim(`CO_PROVA_CH`) as int)                                                                          as COD_TIPO_PROVA_CIENCIAS_HUMANAS,
            cast(trim(`CO_PROVA_LC`) as int)                                                                          as COD_TIPO_PROVA_LINGUAGENS_CODIGO,
            cast(trim(`CO_PROVA_MT`) as int)                                                                          as COD_TIPO_PROVA_MATEMATICA,
            trim(`NU_NOTA_CN`)                                                                                        as NOTA_PROVA_CIENCIA_NATUREZA,
            trim(`NU_NOTA_CH`)                                                                                        as NOTA_PROVA_CIENCIAS_HUMANAS,
            trim(`NU_NOTA_LC`)                                                                                        as NOTA_PROVA_LINGUAGENS_CODIGO,
            trim(`NU_NOTA_MT`)                                                                                        as NOTA_PROVA_MATEMATICA,
            trim(`TX_RESPOSTAS_CN`)                                                                                   as RESPOSTAS_CIENCIA_NATUREZA,
            trim(`TX_RESPOSTAS_CH`)                                                                                   as RESPOSTAS_CIENCIAS_HUMANAS,
            trim(`TX_RESPOSTAS_LC`)                                                                                   as RESPOSTAS_LINGUAGENS_CODIGOS,
            trim(`TX_RESPOSTAS_MT`)                                                                                   as RESPOSTAS_MATEMATICA,

            trim(`TP_LINGUA`)                                                                                         as COD_LINGUAGEM_PROVA,
            trim(`TX_GABARITO_CN`)                                                                                    as GABARITO_CIENCIA_NATUREZA,
            trim(`TX_GABARITO_CH`)                                                                                    as GABARITO_CIENCIAS_HUMANAS,
            trim(`TX_GABARITO_LC`)                                                                                    as GABARITO_LINGUAGENS_CODIGOS,
            trim(`TX_GABARITO_MT`)                                                                                    as GABARITO_MATEMATICA,

            cast(trim(`TP_STATUS_REDACAO`) as bigint)                                                                 as COD_STATUS_REDACAO,
            cast(trim(`NU_NOTA_COMP1`) as bigint)                                                                     as NOTA_COMPETENCIA_1,
            cast(trim(`NU_NOTA_COMP2`) as bigint)                                                                     as NOTA_COMPETENCIA_2,
            cast(trim(`NU_NOTA_COMP3`) as bigint)                                                                     as NOTA_COMPETENCIA_3,
            cast(trim(`NU_NOTA_COMP4`) as bigint)                                                                     as NOTA_COMPETENCIA_4,
            cast(trim(`NU_NOTA_COMP5`) as bigint)                                                                     as NOTA_COMPETENCIA_5,

            trim(`NU_NOTA_REDACAO`)                                                                                   as NOTA_REDACAO,
            trim(`Q001`)                                                                                              as Q001,
            trim(`Q002`)                                                                                              as Q002,
            trim(`Q003`)                                                                                              as Q003,
            trim(`Q004`)                                                                                              as Q004,
            trim(`Q005`)                                                                                              as Q005,
            trim(`Q006`)                                                                                              as Q006,
            trim(`Q007`)                                                                                              as Q007,
            trim(`Q008`)                                                                                              as Q008,
            trim(`Q009`)                                                                                              as Q009,
            trim(`Q010`)                                                                                              as Q010,
            trim(`Q011`)                                                                                              as Q011,
            trim(`Q012`)                                                                                              as Q012,
            trim(`Q013`)                                                                                              as Q013,
            trim(`Q014`)                                                                                              as Q014,
            trim(`Q015`)                                                                                              as Q015,
            trim(`Q016`)                                                                                              as Q016,
            trim(`Q017`)                                                                                              as Q017,
            trim(`Q018`)                                                                                              as Q018,
            trim(`Q019`)                                                                                              as Q019,
            trim(`Q020`)                                                                                              as Q020,
            trim(`Q021`)                                                                                              as Q021,
            trim(`Q022`)                                                                                              as Q022,
            trim(`Q023`)                                                                                              as Q023,
            trim(`Q024`)                                                                                              as Q024,
            trim(`Q025`)                                                                                              as Q025
        FROM tb_temp_microdados
    """)

    logging.info('Equalização do dataset ENEM_MICRODADOS')

    df_enem_microdados_tratado = df_enem_microdados_tratado.withColumn('DATA_PROCESSAMENTO', lit(data_processamento.replace('_', '/')))

    logging.info('Iniciando upload do dataset ENEM_MICRODADOS para S3_DADOS_TRATADOS')
    df_enem_microdados_tratado \
        .coalesce(1) \
        .write \
        .mode('append') \
        .csv(f'{utils.getBuckets()[1].get("s3_dados_tratados")}/{data_processamento}/enem/microdados/')
    logging.info('Upload do dataset ENEM_MICRODADOS para S3_DADOS_TRATADOS finalizado')

    logging.info('Iniciando o insert no database ENEM_MICRODADOS')
    df_enem_microdados_tratado.write.mode("append") \
        .format("jdbc") \
        .option('url', f'{utils.getAmbiente()[0].get("prod-url")}') \
        .option("dbtable", 'ENEM_MICRODADOS') \
        .option("user", f'{utils.getAmbiente()[0].get("user")}') \
        .option("password", f'{utils.getAmbiente()[0].get("pass")}') \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .save()
    logging.info('Insert no database ENEM_MICRODADOS finalizado')

    # Liberando o dataframe da memória e disco
    logging.info('Removendo dataframe ENEM_MICRODADOS da memória')
    df_enem_microdados_tratado.unpersist()
    df_enem_microdados_bruto.unpersist()
    spark.catalog.dropTempView("tb_temp_microdados")

def processar_idd(nome_arquivo, data_processamento):

    logging.info('Leitura dataset IDD iniciado')
    df_idd_bruto = spark.read \
        .option('delimiter', ';') \
        .option('header', 'true') \
        .option('encoding', 'ISO-8859-1') \
        .csv(f'{utils.getBuckets()[0].get("s3_dados_brutos")}/idd/{nome_arquivo}')
    logging.info('Leitura dataset IDD finalizado')

    df_idd_bruto.createOrReplaceTempView('tb_temp_idd')

    logging.info('Iniciando a equalização do dataset IDD')



    df_idd_tratado = spark.sql("""

        SELECT
            cast(trim(`ANO`) as bigint)                                                                               as ANO,
            trim(`Código da Área`)                                                                                    as COD_AREA,
            trim(`Área de Avaliação`)                                                                                 as AREA_AVALIACAO,
            trim(`Grau Acadêmico`)                                                                                    as GRAU_ACADEMICO,
            cast(trim(`Código da IES`) as bigint)                                                                     as COD_DA_IES,
            trim(`Nome da IES*`) as NOME_DA_IES
            trim(`Sigla da IES*`) as SIGLA_DA_IES
            trim(`Organização Acadêmica`) as ORGANIZACAO_ACADEMICA
            trim(``)
            trim(``)
            trim(``)
            trim(``)
            trim(``)
            trim(``)
        FROM tb_temp_idd
    """)

# Fechando a sessão do spark
# spark.stop()
