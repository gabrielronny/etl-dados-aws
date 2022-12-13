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


def processar_fat_fies(nome_arquivo, data_processamento):
    logging.info('Leitura dataset FATO FIES iniciado')

    # Lendo o CSV e tratando o tipo de encoding para a ISO-8859-1
    df_fato_fies_bruto = spark.read \
        .option('encoding', 'UTF-8') \
        .option('delimiter', ';') \
        .option('header', 'true') \
        .csv(f'{utils.getBuckets()[0].get("s3_dados_brutos")}/fatos/{nome_arquivo}')

    df_fato_fies_bruto.createOrReplaceTempView('tb_temp_fat_fies')

    logging.info('Iniciando a equalização do dataset FATO FIES')

    df_fato_fies = spark.sql("""
        SELECT
            cast(id_fies as bigint)                                                                                   as id_fies,
            cast(id_uf_residencia as bigint)                                                                          as id_uf_residencia,
            cast(id_municipio as bigint)                                                                              as id_municipio,
            cast(id_etinia as bigint)                                                                                 as id_etinia,
            cast(id_tipo_escola as bigint)                                                                            as id_tipo_escola,
            cast(id_intituicao as bigint)                                                                             as id_intituicao,
            cast(id_organizacao_academica as bigint)                                                                  as id_organizacao_academica,
            cast(id_municipio_intituicao as bigint)                                                                   as id_municipio_intituicao,
            cast(id_uf_intituicao as bigint)                                                                          as id_uf_intituicao,
            cast(id_curso as bigint)                                                                                  as id_curso,
            cast(id_turno as bigint)                                                                                  as id_turno,
            cast(id_grau_curso as bigint)                                                                             as id_grau_curso,
            is_oncluiu_curso_superior,
            pessoal_com_deficiencia,
            cast(ano_processo_seletivo as bigint)                                                                     as ano_processo_seletivo,
            semestre_processo_seletivo,
            sexo_do_estudante,
            data_nascimento,
            cast(qtde_membros_grupo_familiar as bigint)                                                               as qtde_membros_grupo_familiar,
            cast(renda_familiar_mensal_bruta as double)                                                               as renda_familiar_mensal_bruta,
            cast(renda_mensal_bruta_per_capita as double)                                                             as renda_mensal_bruta_per_capita,
            cast(media_nota_enem as double)                                                                           as media_nota_enem,
            cast(ano_do_enem as bigint)                                                                               as ano_do_enem,
            cast(nota_redacao as double)                                                                              as nota_redacao,
            cast(nota_matematica_e_suas_tecnologias as double)                                                        as nota_matematica_e_suas_tecnologias,
            cast(nota_linguagens_e_codicos_e_suas_tecnologias as double)                                              as nota_linguagens_e_codicos_e_suas_tecnologias,
            cast(nota_ciencia_natureza_e_suas_tecnologias as double)                                                  as nota_ciencia_natureza_e_suas_tecnologias,
            cast(nota_ciencia_humanas_e_suas_tecnologias as double)                                                   as nota_ciencia_humanas_e_suas_tecnologias,
            cast(id_situacao_inscricao_fies as bigint)                                                                as id_situacao_inscricao_fies,
            cast(percentual_de_financiamento as double)                                                               as percentual_de_financiamento,
            semestre_do_financiamento,
            cast(qtde_semestre_financiado as bigint)                                                                  as qtde_semestre_financiado
        FROM tb_temp_fat_fies
    """)

    df_fato_fies = df_fato_fies.withColumn('DATA_PROCESSAMENTO', lit(data_processamento.replace('_', '/')))

    logging.info(f'Iniciando o insert no database FATO FIES')

    df_fato_fies.write.mode("overwrite") \
        .format("jdbc") \
        .option('url', f'{utils.getAmbiente()[0].get("dw-url")}') \
        .option("dbtable", 'FAT_FIES') \
        .option("user", f'{utils.getAmbiente()[0].get("user")}') \
        .option("password", f'{utils.getAmbiente()[0].get("pass")}') \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .save()

    logging.info('Insert no database FATO FIES finalizado')

    logging.info('Removendo dataframe FATO FIES da memória')

     # Liberando o dataframe da memória e disco
    df_fato_fies.unpersist()
    df_fato_fies_bruto.unpersist()
    spark.catalog.dropTempView("tb_temp_fat_fies")
    logging.info('Processamento FATO FIES finalizado')

def processar_fat_idd(nome_arquivo, data_processamento):

    logging.info('Leitura dataset FATO IDD iniciado')

    # Lendo o CSV e tratando o tipo de encoding para a ISO-8859-1
    df_fato_idd_bruto = spark.read \
        .option('encoding', 'UTF-8') \
        .option('delimiter', ';') \
        .option('header', 'true') \
        .csv(f'{utils.getBuckets()[0].get("s3_dados_brutos")}/fatos/{nome_arquivo}')

    df_fato_idd_bruto.createOrReplaceTempView('tb_temp_fat_idd')

    logging.info('Iniciando a equalização do dataset FATO IDD')

    df_fato_idd = spark.sql("""
        SELECT
            cast(id_idd as bigint)                                                                                    as id_idd,
            cast(id_curso as bigint)                                                                                  as id_curso,
            cast(id_codigo_intituicao as bigint)                                                                      as id_codigo_intituicao,
            cast(id_municipio as bigint)                                                                              as id_municipio,
            cast(id_estado as bigint)                                                                                 as id_estado,
            cast(id_organizacao_academica as bigint)                                                                  as id_organizacao_academica,
            cast(id_categoria_administrativa as bigint)                                                               as id_categoria_administrativa,
            cast(id_grau_academico as bigint)                                                                         as id_grau_academico,
            cast(id_modalidade_de_ensino as bigint)                                                                   as id_modalidade_de_ensino,
            cast(idd_continuo as bigint)                                                                              as idd_continuo,
            cast(replace(idd_faixa, 'SC', '') as bigint)                                                             as idd_faixa,
            cast(ano as bigint)                                                                                       as ano,
            cast(numero_concluintes as bigint)                                                                        as numero_concluintes,
            cast(numero_participantes as bigint)                                                                      as numero_participantes,
            cast(numero_participantes_nota_enem as bigint)                                                            as numero_participantes_nota_enem,
            cast(proporcao_concluintes as double)                                                                     as proporcao_concluintes,
            cast(nome_bruta_idd as double)                                                                            as numero_bruto_idd
        FROM tb_temp_fat_idd
    """)

    df_fato_idd = df_fato_idd.withColumn('DATA_PROCESSAMENTO', lit(data_processamento.replace('_', '/')))

    logging.info(f'Iniciando o insert no database FATO IDD')

    df_fato_idd.write.mode("overwrite") \
        .format("jdbc") \
        .option('url', f'{utils.getAmbiente()[0].get("dw-url")}') \
        .option("dbtable", 'FAT_IDD') \
        .option("user", f'{utils.getAmbiente()[0].get("user")}') \
        .option("password", f'{utils.getAmbiente()[0].get("pass")}') \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .save()

    logging.info('Insert no database FATO IDD finalizado')

    logging.info('Removendo dataframe FATO IDD da memória')

    # Liberando o dataframe da memória e disco
    df_fato_idd.unpersist()
    df_fato_idd_bruto.unpersist()
    spark.catalog.dropTempView("tb_temp_fat_idd")
    logging.info('Processamento FATO IDD finalizado')

def processar_dm_categoria_administrativa(nome_arquivo, data_processamento):

    logging.info('Leitura dataset DM CAT ADM iniciado')

    # Lendo o CSV e tratando o tipo de encoding para a ISO-8859-1
    df_dm_cat_adm_bruto = spark.read \
        .option('encoding', 'UTF-8') \
        .option('delimiter', ';') \
        .option('header', 'true') \
        .csv(f'{utils.getBuckets()[0].get("s3_dados_brutos")}/dimensoes/{nome_arquivo}')

    df_dm_cat_adm_bruto.createOrReplaceTempView('tb_temp_dm_cat_adm')

    logging.info('Iniciando a equalização do dataset DM CAT ADM')

    df_dm_cat_adm = spark.sql("""
        SELECT
            cast(id_categoria_administrativa as bigint)                                                               as id_categoria_administrativa,
            categoria_administrativa
        FROM tb_temp_dm_cat_adm
    """)

    df_dm_cat_adm = df_dm_cat_adm.withColumn('DATA_PROCESSAMENTO', lit(data_processamento.replace('_', '/')))

    logging.info(f'Iniciando o insert no database DM CAT ADM')

    df_dm_cat_adm.write.mode("overwrite") \
        .format("jdbc") \
        .option('url', f'{utils.getAmbiente()[0].get("dw-url")}') \
        .option("dbtable", 'DM_CATEGORIA_ADMINISTRATIVA') \
        .option("user", f'{utils.getAmbiente()[0].get("user")}') \
        .option("password", f'{utils.getAmbiente()[0].get("pass")}') \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .save()

    logging.info('Insert no database DM CAT ADM finalizado')

    logging.info('Removendo dataframe DM CAT ADM da memória')

    # Liberando o dataframe da memória e disco
    df_dm_cat_adm.unpersist()
    df_dm_cat_adm_bruto.unpersist()
    spark.catalog.dropTempView("tb_temp_dm_cat_adm")
    logging.info('Processamento DM CAT ADM finalizado')

def processar_dm_curso(nome_arquivo, data_processamento):

    logging.info('Leitura dataset DM CURSO iniciado')

    # Lendo o CSV e tratando o tipo de encoding para a ISO-8859-1
    df_dm_curso_bruto = spark.read \
        .option('encoding', 'UTF-8') \
        .option('delimiter', ';') \
        .option('header', 'true') \
        .csv(f'{utils.getBuckets()[0].get("s3_dados_brutos")}/dimensoes/{nome_arquivo}')

    df_dm_curso_bruto.createOrReplaceTempView('tb_temp_dm_curso')

    logging.info('Iniciando a equalização do dataset DM CURSO')

    df_dm_curso = spark.sql("""
        SELECT
            cast(id_curso as bigint)                                                                                  as id_curso,
            nome_curso
        FROM tb_temp_dm_curso
    """)

    df_dm_curso = df_dm_curso.withColumn('DATA_PROCESSAMENTO', lit(data_processamento.replace('_', '/')))

    logging.info(f'Iniciando o insert no database DM CURSO')

    df_dm_curso.write.mode("overwrite") \
        .format("jdbc") \
        .option('url', f'{utils.getAmbiente()[0].get("dw-url")}') \
        .option("dbtable", 'DM_CURSO') \
        .option("user", f'{utils.getAmbiente()[0].get("user")}') \
        .option("password", f'{utils.getAmbiente()[0].get("pass")}') \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .save()

    logging.info('Insert no database DM CURSO finalizado')

    logging.info('Removendo dataframe DM CURSO da memória')

    # Liberando o dataframe da memória e disco
    df_dm_curso.unpersist()
    df_dm_curso_bruto.unpersist()
    spark.catalog.dropTempView("tb_temp_dm_curso")
    logging.info('Processamento DM DM CURSO finalizado')


def processar_dm_estado(nome_arquivo, data_processamento):

    logging.info('Leitura dataset DM ESTADO iniciado')

    # Lendo o CSV e tratando o tipo de encoding para a ISO-8859-1
    df_dm_estado_bruto = spark.read \
        .option('encoding', 'UTF-8') \
        .option('delimiter', ';') \
        .option('header', 'true') \
        .csv(f'{utils.getBuckets()[0].get("s3_dados_brutos")}/dimensoes/{nome_arquivo}')

    df_dm_estado_bruto.createOrReplaceTempView('tb_temp_dm_estado')

    logging.info('Iniciando a equalização do dataset DM ESTADO')

    df_dm_estado = spark.sql("""
        SELECT
            cast(id_estado as bigint)                                                                                 as id_estado,
            estado
        FROM tb_temp_dm_estado
    """)

    df_dm_estado = df_dm_estado.withColumn('DATA_PROCESSAMENTO', lit(data_processamento.replace('_', '/')))

    logging.info(f'Iniciando o insert no database DM ESTADO')

    df_dm_estado.write.mode("overwrite") \
        .format("jdbc") \
        .option('url', f'{utils.getAmbiente()[0].get("dw-url")}') \
        .option("dbtable", 'DM_ESTADO') \
        .option("user", f'{utils.getAmbiente()[0].get("user")}') \
        .option("password", f'{utils.getAmbiente()[0].get("pass")}') \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .save()

    logging.info('Insert no database DM ESTADO finalizado')

    logging.info('Removendo dataframe DM ESTADO da memória')

    # Liberando o dataframe da memória e disco
    df_dm_estado.unpersist()
    df_dm_estado_bruto.unpersist()
    spark.catalog.dropTempView("tb_temp_dm_estado")
    logging.info('Processamento DM ESTADO finalizado')


def processar_dm_etnia(nome_arquivo, data_processamento):

    logging.info('Leitura dataset DM ETNIA iniciado')

    # Lendo o CSV e tratando o tipo de encoding para a ISO-8859-1
    df_dm_etnia_bruto = spark.read \
        .option('encoding', 'UTF-8') \
        .option('delimiter', ';') \
        .option('header', 'true') \
        .csv(f'{utils.getBuckets()[0].get("s3_dados_brutos")}/dimensoes/{nome_arquivo}')

    df_dm_etnia_bruto.createOrReplaceTempView('tb_temp_dm_etnia')

    logging.info('Iniciando a equalização do dataset DM ETNIA')

    df_dm_etnia = spark.sql("""
        SELECT
            cast(id_etinia as bigint)                                                                                 as id_etnia,
            etinia                                                                                                    as etnia
        FROM tb_temp_dm_etnia
    """)

    df_dm_etnia = df_dm_etnia.withColumn('DATA_PROCESSAMENTO', lit(data_processamento.replace('_', '/')))

    logging.info(f'Iniciando o insert no database DM ETNIA')

    df_dm_etnia.write.mode("overwrite") \
        .format("jdbc") \
        .option('url', f'{utils.getAmbiente()[0].get("dw-url")}') \
        .option("dbtable", 'DM_ETNIA') \
        .option("user", f'{utils.getAmbiente()[0].get("user")}') \
        .option("password", f'{utils.getAmbiente()[0].get("pass")}') \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .save()

    logging.info('Insert no database DM ETNIA finalizado')

    logging.info('Removendo dataframe DM ETNIA da memória')

    # Liberando o dataframe da memória e disco
    df_dm_etnia.unpersist()
    df_dm_etnia_bruto.unpersist()
    spark.catalog.dropTempView("tb_temp_dm_etnia")
    logging.info('Processamento DM ETNIA finalizado')


def processar_dm_grau_academico(nome_arquivo, data_processamento):

    logging.info('Leitura dataset DM GRAU ACADEMICO iniciado')

    # Lendo o CSV e tratando o tipo de encoding para a ISO-8859-1
    df_dm_grau_academico_bruto = spark.read \
        .option('encoding', 'UTF-8') \
        .option('delimiter', ';') \
        .option('header', 'true') \
        .csv(f'{utils.getBuckets()[0].get("s3_dados_brutos")}/dimensoes/{nome_arquivo}')

    df_dm_grau_academico_bruto.createOrReplaceTempView('tb_temp_dm_grau')

    logging.info('Iniciando a equalização do dataset DM GRAU ACADEMICO')

    df_dm_grau_academico = spark.sql("""
        SELECT
            cast(id_grau_academico as bigint)                                                                         as id_grau_academico,
            grau_academico
        FROM tb_temp_dm_grau
    """)

    df_dm_grau_academico = df_dm_grau_academico.withColumn('DATA_PROCESSAMENTO', lit(data_processamento.replace('_', '/')))

    logging.info(f'Iniciando o insert no database DM GRAU ACADEMICO')

    df_dm_grau_academico.write.mode("overwrite") \
        .format("jdbc") \
        .option('url', f'{utils.getAmbiente()[0].get("dw-url")}') \
        .option("dbtable", 'DM_GRAU_ACADEMICO') \
        .option("user", f'{utils.getAmbiente()[0].get("user")}') \
        .option("password", f'{utils.getAmbiente()[0].get("pass")}') \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .save()

    logging.info('Insert no database DM GRAU ACADEMICO finalizado')

    logging.info('Removendo dataframe DM GRAU ACADEMICO da memória')

    # Liberando o dataframe da memória e disco
    df_dm_grau_academico.unpersist()
    df_dm_grau_academico_bruto.unpersist()
    spark.catalog.dropTempView("tb_temp_dm_grau")
    logging.info('Processamento DM GRAU ACADEMICO finalizado')

def processar_dm_grau_curso(nome_arquivo, data_processamento):

    logging.info('Leitura dataset DM GRAU CURSO iniciado')

    # Lendo o CSV e tratando o tipo de encoding para a ISO-8859-1
    df_dm_grau_curso_bruto = spark.read \
        .option('encoding', 'UTF-8') \
        .option('delimiter', ';') \
        .option('header', 'true') \
        .csv(f'{utils.getBuckets()[0].get("s3_dados_brutos")}/dimensoes/{nome_arquivo}')

    df_dm_grau_curso_bruto.createOrReplaceTempView('tb_temp_dm_grau_cuso')

    logging.info('Iniciando a equalização do dataset DM GRAU CURSO')

    df_dm_grau_curso = spark.sql("""
        SELECT
            cast(id_grau_curso as bigint)                                                                             as id_grau_curso,
            grau_curso
        FROM tb_temp_dm_grau_cuso
    """)

    df_dm_grau_curso = df_dm_grau_curso.withColumn('DATA_PROCESSAMENTO', lit(data_processamento.replace('_', '/')))

    logging.info(f'Iniciando o insert no database DM GRAU CURSO')

    df_dm_grau_curso.write.mode("overwrite") \
        .format("jdbc") \
        .option('url', f'{utils.getAmbiente()[0].get("dw-url")}') \
        .option("dbtable", 'DM_GRAU_CURSO') \
        .option("user", f'{utils.getAmbiente()[0].get("user")}') \
        .option("password", f'{utils.getAmbiente()[0].get("pass")}') \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .save()

    logging.info('Insert no database DM GRAU CURSO finalizado')

    logging.info('Removendo dataframe DM GRAU CURSO da memória')

    # Liberando o dataframe da memória e disco
    df_dm_grau_curso.unpersist()
    df_dm_grau_curso_bruto.unpersist()
    spark.catalog.dropTempView("tb_temp_dm_grau_cuso")
    logging.info('Processamento DM GRAU CURSO finalizado')

def processar_dm_instituicao_ensino(nome_arquivo, data_processamento):

    logging.info('Leitura dataset DM INSTITUICAO ENSINO iniciado')

    # Lendo o CSV e tratando o tipo de encoding para a ISO-8859-1
    df_dm_if_ensino_bruto = spark.read \
        .option('encoding', 'UTF-8') \
        .option('delimiter', ';') \
        .option('header', 'true') \
        .csv(f'{utils.getBuckets()[0].get("s3_dados_brutos")}/dimensoes/{nome_arquivo}')

    df_dm_if_ensino_bruto.createOrReplaceTempView('tb_temp_dm_if_ensino')

    logging.info('Iniciando a equalização do datasetDM INSTITUICAO ENSINO')

    df_dm_if_ensino = spark.sql("""
        SELECT
            cast(id_codigo_intituicao as bigint)                                                                      as id_codigo_intituicao,
            nome_da_ies
        FROM tb_temp_dm_if_ensino
    """)

    df_dm_if_ensino = df_dm_if_ensino.withColumn('DATA_PROCESSAMENTO', lit(data_processamento.replace('_', '/')))

    logging.info(f'Iniciando o insert no database DM INSTITUICAO ENSINO')

    df_dm_if_ensino.write.mode("overwrite") \
        .format("jdbc") \
        .option('url', f'{utils.getAmbiente()[0].get("dw-url")}') \
        .option("dbtable", 'DM_INSTITUICAO_ENSINO') \
        .option("user", f'{utils.getAmbiente()[0].get("user")}') \
        .option("password", f'{utils.getAmbiente()[0].get("pass")}') \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .save()

    logging.info('Insert no database DM INSTITUICAO ENSINO finalizado')

    logging.info('Removendo dataframe DM INSTITUICAO ENSINO da memória')

    # Liberando o dataframe da memória e disco
    df_dm_if_ensino.unpersist()
    df_dm_if_ensino_bruto.unpersist()
    spark.catalog.dropTempView("tb_temp_dm_if_ensino")
    logging.info('Processamento DM INSTITUICAO ENSINO finalizado')


def processar_dm_modalidade_ensino(nome_arquivo, data_processamento):

    logging.info('Leitura dataset DM MODALIDADE ENSINO iniciado')

    # Lendo o CSV e tratando o tipo de encoding para a ISO-8859-1
    df_dm_modalidade_ensino_bruto = spark.read \
        .option('encoding', 'UTF-8') \
        .option('delimiter', ';') \
        .option('header', 'true') \
        .csv(f'{utils.getBuckets()[0].get("s3_dados_brutos")}/dimensoes/{nome_arquivo}')

    df_dm_modalidade_ensino_bruto.createOrReplaceTempView('tb_temp_dm_md_ensino')

    logging.info('Iniciando a equalização do dataset DM MODALIDADE ENSINO')

    df_dm_modalidade_ensino = spark.sql("""
        SELECT
            cast(id_modalidade_de_ensino as bigint)                                                                   as id_modalidade_de_ensino,
            modalidade_de_ensino
        FROM tb_temp_dm_md_ensino
    """)

    df_dm_modalidade_ensino = df_dm_modalidade_ensino.withColumn('DATA_PROCESSAMENTO', lit(data_processamento.replace('_', '/')))

    logging.info(f'Iniciando o insert no database DM MODALIDADE ENSINO')

    df_dm_modalidade_ensino.write.mode("overwrite") \
        .format("jdbc") \
        .option('url', f'{utils.getAmbiente()[0].get("dw-url")}') \
        .option("dbtable", 'DM_MODALIDADE_ENSINO') \
        .option("user", f'{utils.getAmbiente()[0].get("user")}') \
        .option("password", f'{utils.getAmbiente()[0].get("pass")}') \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .save()

    logging.info('Insert no database DM MODALIDADE ENSINO finalizado')

    logging.info('Removendo dataframe DM MODALIDADE ENSINO da memória')

    # Liberando o dataframe da memória e disco
    df_dm_modalidade_ensino.unpersist()
    df_dm_modalidade_ensino_bruto.unpersist()
    spark.catalog.dropTempView("tb_temp_dm_md_ensino")
    logging.info('Processamento DM MODALIDADE ENSINO finalizado')


def processar_dm_municipio(nome_arquivo, data_processamento):

    logging.info('Leitura dataset DM MUNICIPIO iniciado')

    # Lendo o CSV e tratando o tipo de encoding para a ISO-8859-1
    df_dm_municipio_bruto = spark.read \
        .option('encoding', 'UTF-8') \
        .option('delimiter', ';') \
        .option('header', 'true') \
        .csv(f'{utils.getBuckets()[0].get("s3_dados_brutos")}/dimensoes/{nome_arquivo}')

    df_dm_municipio_bruto.createOrReplaceTempView('tb_temp_dm_municipio')

    logging.info('Iniciando a equalização do dataset DM MUNICIPIO')

    df_dm_municipio = spark.sql("""
        SELECT
            cast(id_municipio as bigint)                                                                              as id_municipio,
            municipio
        FROM tb_temp_dm_municipio
    """)

    df_dm_municipio = df_dm_municipio.withColumn('DATA_PROCESSAMENTO', lit(data_processamento.replace('_', '/')))

    logging.info(f'Iniciando o insert no database DM MUNICIPIO')

    df_dm_municipio.write.mode("overwrite") \
        .format("jdbc") \
        .option('url', f'{utils.getAmbiente()[0].get("dw-url")}') \
        .option("dbtable", 'DM_MUNICIPIO') \
        .option("user", f'{utils.getAmbiente()[0].get("user")}') \
        .option("password", f'{utils.getAmbiente()[0].get("pass")}') \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .save()

    logging.info('Insert no database DM MUNICIPIO finalizado')

    logging.info('Removendo dataframe DM MUNICIPIO da memória')

    # Liberando o dataframe da memória e disco
    df_dm_municipio.unpersist()
    df_dm_municipio_bruto.unpersist()
    spark.catalog.dropTempView("tb_temp_dm_municipio")
    logging.info('Processamento DM MUNICIPIO finalizado')


def processar_dm_organizacao_academica(nome_arquivo, data_processamento):

    logging.info('Leitura dataset DM ORG ACADEMICA')

    # Lendo o CSV e tratando o tipo de encoding para a ISO-8859-1
    df_dm_org_academica_bruto = spark.read \
        .option('encoding', 'UTF-8') \
        .option('delimiter', ';') \
        .option('header', 'true') \
        .csv(f'{utils.getBuckets()[0].get("s3_dados_brutos")}/dimensoes/{nome_arquivo}')

    df_dm_org_academica_bruto.createOrReplaceTempView('tb_temp_dm_org_academica')

    logging.info('Iniciando a equalização do dataset DM ORG ACADEMICA')

    df_dm_org_academica = spark.sql("""
        SELECT
            cast(id_organizacao_academica as bigint)                                                                  as id_organizacao_academica,
            organizacao_academica
        FROM tb_temp_dm_org_academica
    """)

    df_dm_org_academica = df_dm_org_academica.withColumn('DATA_PROCESSAMENTO', lit(data_processamento.replace('_', '/')))

    logging.info(f'Iniciando o insert no database DM ORG ACADEMICA')

    df_dm_org_academica.write.mode("overwrite") \
        .format("jdbc") \
        .option('url', f'{utils.getAmbiente()[0].get("dw-url")}') \
        .option("dbtable", 'DM_ORGANIZACAO_ACADEMICA') \
        .option("user", f'{utils.getAmbiente()[0].get("user")}') \
        .option("password", f'{utils.getAmbiente()[0].get("pass")}') \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .save()

    logging.info('Insert no database DM ORG ACADEMICA finalizado')

    logging.info('Removendo dataframe DM ORG ACADEMICA da memória')

    # Liberando o dataframe da memória e disco
    df_dm_org_academica.unpersist()
    df_dm_org_academica_bruto.unpersist()
    spark.catalog.dropTempView("tb_temp_dm_org_academica")
    logging.info('Processamento DM ORG ACADEMICA finalizado')


def processar_dm_regiao(nome_arquivo, data_processamento):

    logging.info('Leitura dataset DM REGIAO')

    # Lendo o CSV e tratando o tipo de encoding para a ISO-8859-1
    df_dm_regiao_bruto = spark.read \
        .option('encoding', 'UTF-8') \
        .option('delimiter', ';') \
        .option('header', 'true') \
        .csv(f'{utils.getBuckets()[0].get("s3_dados_brutos")}/dimensoes/{nome_arquivo}')

    df_dm_regiao_bruto.createOrReplaceTempView('tb_temp_dm_regiao')

    logging.info('Iniciando a equalização do dataset DM REGIAO')

    df_dm_regiao = spark.sql("""
        SELECT
            cast(id_regiao as bigint)                                                                                 as id_regiao,
            regiao
        FROM tb_temp_dm_regiao
    """)

    df_dm_regiao = df_dm_regiao.withColumn('DATA_PROCESSAMENTO', lit(data_processamento.replace('_', '/')))

    logging.info(f'Iniciando o insert no database DM REGIAO')

    df_dm_regiao.write.mode("overwrite") \
        .format("jdbc") \
        .option('url', f'{utils.getAmbiente()[0].get("dw-url")}') \
        .option("dbtable", 'DM_REGIAO') \
        .option("user", f'{utils.getAmbiente()[0].get("user")}') \
        .option("password", f'{utils.getAmbiente()[0].get("pass")}') \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .save()

    logging.info('Insert no database DM REGIAO finalizado')

    logging.info('Removendo dataframe DM REGIAO da memória')

    # Liberando o dataframe da memória e disco
    df_dm_regiao.unpersist()
    df_dm_regiao_bruto.unpersist()
    spark.catalog.dropTempView("tb_temp_dm_regiao")
    logging.info('Processamento DM REGIAO finalizado')


def processar_dm_situacao_inscricao_fies(nome_arquivo, data_processamento):

    logging.info('Leitura dataset DM SITUACAO INSCRICAO')

    # Lendo o CSV e tratando o tipo de encoding para a ISO-8859-1
    df_dm_sit_inscricao_bruto = spark.read \
        .option('encoding', 'UTF-8') \
        .option('delimiter', ';') \
        .option('header', 'true') \
        .csv(f'{utils.getBuckets()[0].get("s3_dados_brutos")}/dimensoes/{nome_arquivo}')

    df_dm_sit_inscricao_bruto.createOrReplaceTempView('tb_temp_dm_situacao')

    logging.info('Iniciando a equalização do dataset DM SITUACAO INSCRICAO')

    df_dm_sit_inscricao = spark.sql("""
        SELECT
            cast(id_situacao_inscricao_fies as bigint)                                                                as id_situacao_inscricao_fies,
            situacao_inscricao_fies
        FROM tb_temp_dm_situacao
    """)

    df_dm_sit_inscricao = df_dm_sit_inscricao.withColumn('DATA_PROCESSAMENTO', lit(data_processamento.replace('_', '/')))

    logging.info(f'Iniciando o insert no database DM SITUACAO INSCRICAO')

    df_dm_sit_inscricao.write.mode("overwrite") \
        .format("jdbc") \
        .option('url', f'{utils.getAmbiente()[0].get("dw-url")}') \
        .option("dbtable", 'DM_SITUACAO_INSCRICAO') \
        .option("user", f'{utils.getAmbiente()[0].get("user")}') \
        .option("password", f'{utils.getAmbiente()[0].get("pass")}') \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .save()

    logging.info('Insert no database DM SITUACAO INSCRICAO finalizado')

    logging.info('Removendo dataframe DM SITUACAO INSCRICAO da memória')

    # Liberando o dataframe da memória e disco
    df_dm_sit_inscricao.unpersist()
    df_dm_sit_inscricao_bruto.unpersist()
    spark.catalog.dropTempView("tb_temp_dm_situacao")
    logging.info('Processamento DM SITUACAO INSCRICAO finalizado')


def processar_dm_tipo_bolsa(nome_arquivo, data_processamento):

    logging.info('Leitura dataset DM TIPO BOLSA')

    # Lendo o CSV e tratando o tipo de encoding para a ISO-8859-1
    df_dm_tipo_bolsa_bruto = spark.read \
        .option('encoding', 'UTF-8') \
        .option('delimiter', ';') \
        .option('header', 'true') \
        .csv(f'{utils.getBuckets()[0].get("s3_dados_brutos")}/dimensoes/{nome_arquivo}')

    df_dm_tipo_bolsa_bruto.createOrReplaceTempView('tb_temp_dm_tp_bolsa')

    logging.info('Iniciando a equalização do dataset DM TIPO BOLSA')

    df_dm_tipo_bolsa = spark.sql("""
        SELECT
            cast(id_tipo_bolsa as bigint)                                                                             as id_tipo_bolsa,
            tipo_bolsa
        FROM tb_temp_dm_tp_bolsa
    """)

    df_dm_tipo_bolsa = df_dm_tipo_bolsa.withColumn('DATA_PROCESSAMENTO', lit(data_processamento.replace('_', '/')))

    logging.info(f'Iniciando o insert no database DM TIPO BOLSA')

    df_dm_tipo_bolsa.write.mode("overwrite") \
        .format("jdbc") \
        .option('url', f'{utils.getAmbiente()[0].get("dw-url")}') \
        .option("dbtable", 'DM_TIPO_BOLSA') \
        .option("user", f'{utils.getAmbiente()[0].get("user")}') \
        .option("password", f'{utils.getAmbiente()[0].get("pass")}') \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .save()

    logging.info('Insert no database DM TIPO BOLSA finalizado')

    logging.info('Removendo dataframe DM TIPO BOLSA da memória')

    # Liberando o dataframe da memória e disco
    df_dm_tipo_bolsa.unpersist()
    df_dm_tipo_bolsa_bruto.unpersist()
    spark.catalog.dropTempView("tb_temp_dm_tp_bolsa")
    logging.info('Processamento DM TIPO BOLSA finalizado')


def processar_dm_tipo_escola(nome_arquivo, data_processamento):

    logging.info('Leitura dataset DM TIPO ESCOLA')

    # Lendo o CSV e tratando o tipo de encoding para a ISO-8859-1
    df_dm_tipo_escola_bruto = spark.read \
        .option('encoding', 'UTF-8') \
        .option('delimiter', ';') \
        .option('header', 'true') \
        .csv(f'{utils.getBuckets()[0].get("s3_dados_brutos")}/dimensoes/{nome_arquivo}')

    df_dm_tipo_escola_bruto.createOrReplaceTempView('tb_temp_dm_tp_escola')

    logging.info('Iniciando a equalização do dataset DM TIPO ESCOLA')

    df_dm_tipo_escola = spark.sql("""
        SELECT
            cast(id_tipo_escola as bigint)                                                                            as id_tipo_escola,
            tipo_escola
        FROM tb_temp_dm_tp_escola
    """)

    df_dm_tipo_escola = df_dm_tipo_escola.withColumn('DATA_PROCESSAMENTO', lit(data_processamento.replace('_', '/')))

    logging.info(f'Iniciando o insert no database DM TIPO ESCOLA')

    df_dm_tipo_escola.write.mode("overwrite") \
        .format("jdbc") \
        .option('url', f'{utils.getAmbiente()[0].get("dw-url")}') \
        .option("dbtable", 'DM_TIPO_ESCOLA') \
        .option("user", f'{utils.getAmbiente()[0].get("user")}') \
        .option("password", f'{utils.getAmbiente()[0].get("pass")}') \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .save()

    logging.info('Insert no database DM TIPO ESCOLA finalizado')

    logging.info('Removendo dataframe DM TIPO ESCOLA da memória')

    # Liberando o dataframe da memória e disco
    df_dm_tipo_escola.unpersist()
    df_dm_tipo_escola_bruto.unpersist()
    spark.catalog.dropTempView("tb_temp_dm_tp_escola")
    logging.info('Processamento DM TIPO ESCOLA finalizado')


def processar_dm_turno(nome_arquivo, data_processamento):

    logging.info('Leitura dataset DM TURNO')

    # Lendo o CSV e tratando o tipo de encoding para a ISO-8859-1
    df_dm_turno_bruto = spark.read \
        .option('encoding', 'UTF-8') \
        .option('delimiter', ';') \
        .option('header', 'true') \
        .csv(f'{utils.getBuckets()[0].get("s3_dados_brutos")}/dimensoes/{nome_arquivo}')

    df_dm_turno_bruto.createOrReplaceTempView('tb_temp_dm_turno')

    logging.info('Iniciando a equalização do dataset DM TURNO')

    df_dm_turno = spark.sql("""
        SELECT
            cast(id_turno as bigint)                                                                                  as id_turno,
            turno
        FROM tb_temp_dm_turno
    """)

    df_dm_turno = df_dm_turno.withColumn('DATA_PROCESSAMENTO', lit(data_processamento.replace('_', '/')))

    logging.info(f'Iniciando o insert no database DM TURNO')

    df_dm_turno.write.mode("overwrite") \
        .format("jdbc") \
        .option('url', f'{utils.getAmbiente()[0].get("dw-url")}') \
        .option("dbtable", 'DM_TURNO') \
        .option("user", f'{utils.getAmbiente()[0].get("user")}') \
        .option("password", f'{utils.getAmbiente()[0].get("pass")}') \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .save()

    logging.info('Insert no database DM TURNO finalizado')

    logging.info('Removendo dataframe DM TURNO da memória')

    # Liberando o dataframe da memória e disco
    df_dm_turno.unpersist()
    df_dm_turno_bruto.unpersist()
    spark.catalog.dropTempView("tb_temp_dm_turno")
    logging.info('Processamento DM TURNO finalizado')