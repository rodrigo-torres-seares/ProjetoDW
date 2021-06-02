import numpy as np
import pandas as pd
from tqdm import tqdm
import STAGES as stg
import time


def extract_fact_aplicacao_questionario(conn):
    questionario_tbl = stg.read_table_postgres(conn,
                                               schema='STAGED',
                                               table_name='resultado_aluno',
                                               columns=["ID_ESCOLA",
                                                        "ID_ALUNO",
                                                        "ID_MUNICIPIO",
                                                        "ID_PROVA_BRASIL",
                                                        "IN_PREENCHIMENTO",
                                                        "IN_PROFICIENCIA",
                                                        "PROFICIENCIA_LP_SAEB",
                                                        "DESVIO_PADRAO_LP_SAEB",
                                                        "PROFICIENCIA_MT_SAEB",
                                                        "DESVIO_PADRAO_MT_SAEB"]
                                               )

    localidade_tbl = stg.read_table_postgres(conn,
                                             schema='DW',
                                             table_name='D_LOCALIDADE',
                                             columns=["SK_LOCALIDADE", "CD_MUNICÍPIO"]
                                             )

    escola_tbl = stg.read_table_postgres(conn,
                                         schema='DW',
                                         table_name='D_ESCOLA',
                                         columns=["SK_ESCOLA", "CD_ESCOLA"]
                                         )

    turma_tbl = stg.read_table_postgres(conn,
                                        schema='DW',
                                        table_name='D_TURMA',
                                        columns=["SK_TURMA", "CD_ALUNO"]
                                        )

    return localidade_tbl, escola_tbl, turma_tbl, questionario_tbl


def treat_fact_aplicacao_questionario(localidade_tbl, escola_tbl, turma_tbl,
                                      questionario_tbl):

    questionario_tbl = (
        questionario_tbl.merge(localidade_tbl, left_on="ID_MUNICIPIO",
                               right_on="CD_MUNICÍPIO"
                    ).merge(escola_tbl, left_on="ID_ESCOLA", right_on="CD_ESCOLA"
                    ).merge(escola_tbl, left_on="ID_ESCOLA", right_on="CD_ESCOLA"
                    ).merge(turma_tbl, left_on="ID_ALUNO", right_on="CD_ALUNO"
                    ).rename(columns={'ID_PROVA_BRASIL': 'CD_ANO',
                                      'IN_PREENCHIMENTO': 'FL_PREENCHIMENTO',
                                      'IN_PROFICIENCIA': 'FL_PROFICIÊNCIA',
                                      'PROFICIENCIA_LP_SAEB': 'VL_PROFICIÊNCIA_LÍNGUA_PORTUGUESA_SAEB',
                                      'DESVIO_PADRAO_LP_SAEB': 'VL_DESVIO_PADRAO_LÍNGUA_PORTUGUESA_SAEB',
                                      'PROFICIENCIA_MT_SAEB': 'VL_PROFICIÊNCIA_MATEMÁTICA_SAEB',
                                      'DESVIO_PADRAO_MT_SAEB': 'VL_DESVIO_PADRAO_MATEMÁTICA_SAEB'}
                        ).assign(
                    VL_PROFICIÊNCIA_LÍNGUA_PORTUGUESA_SAEB=lambda x: x.VL_PROFICIÊNCIA_LÍNGUA_PORTUGUESA_SAEB.str.replace(',', '.'),
                    VL_DESVIO_PADRAO_LÍNGUA_PORTUGUESA_SAEB=lambda x: x.VL_DESVIO_PADRAO_LÍNGUA_PORTUGUESA_SAEB.str.replace(',', '.'),
                    VL_PROFICIÊNCIA_MATEMÁTICA_SAEB=lambda x: x.VL_PROFICIÊNCIA_MATEMÁTICA_SAEB.str.replace(',', '.'),
                    VL_DESVIO_PADRAO_MATEMÁTICA_SAEB=lambda x: x.VL_DESVIO_PADRAO_MATEMÁTICA_SAEB.str.replace(',', '.')
        )

    )

    questionario_tbl = questionario_tbl.drop(columns=['ID_ALUNO',
                                   'ID_MUNICIPIO',
                                   'ID_ESCOLA',
                                   'CD_ALUNO',
                                   'CD_ESCOLA',
                                   'CD_MUNICÍPIO'])

    print(questionario_tbl.head(20).to_string());

    questionario_tbl = (
        questionario_tbl.assign(
            VL_DESVIO_PADRAO_MATEMÁTICA_SAEB=lambda x:
                x.VL_PROFICIÊNCIA_MATEMÁTICA_SAEB.astype(float),
            VL_PROFICIÊNCIA_MATEMÁTICA_SAEB=lambda x:
                x.VL_PROFICIÊNCIA_MATEMÁTICA_SAEB.astype(float),
            VL_DESVIO_PADRAO_LÍNGUA_PORTUGUESA_SAEB=lambda x:
                x.VL_DESVIO_PADRAO_LÍNGUA_PORTUGUESA_SAEB.astype(float),
            VL_PROFICIÊNCIA_LÍNGUA_PORTUGUESA_SAEB=lambda x:
                x.VL_PROFICIÊNCIA_LÍNGUA_PORTUGUESA_SAEB.astype(float)
        )
    )

    ordemcolunas = ['SK_TURMA', 'SK_ESCOLA', 'SK_LOCALIDADE', 'CD_ANO',
                    'FL_PREENCHIMENTO',
                    'FL_PROFICIÊNCIA', 'VL_DESVIO_PADRAO_MATEMÁTICA_SAEB',
                    'VL_PROFICIÊNCIA_MATEMÁTICA_SAEB',
                    'VL_DESVIO_PADRAO_LÍNGUA_PORTUGUESA_SAEB',
                    'VL_PROFICIÊNCIA_LÍNGUA_PORTUGUESA_SAEB']

    questionario_tbl = questionario_tbl[ordemcolunas]

    return questionario_tbl


def load_fact_aplicacao_quetionario(fact_aplicacao_questionario, conn):
    stg.load_to_sql_postgres(conn,
                             dim=fact_aplicacao_questionario,
                             divisor=50,
                             table_name='F_APLICAÇÃO_QUESTIONÁRIO',
                             schema='DW')


def run_fact_aplicacao_questionario(conn):
    start_time = time.time()
    localidade_tbl, escola_tbl, turma_tbl, questionario_tbl = extract_fact_aplicacao_questionario(
        conn)
    extract_time = time.time()
    print(f'F_Aplicação_Questionário\nextract: {extract_time - start_time:.3f}')

    fact_questionario = treat_fact_aplicacao_questionario(localidade_tbl, escola_tbl,
                                                          turma_tbl, questionario_tbl)
    treat_time = time.time()
    print(f'treat: {treat_time - extract_time:.3f}')

    load_fact_aplicacao_quetionario(fact_questionario, conn)
    load_time = time.time()
    print(f'load: {load_time - treat_time:.3f}')

    return load_time - start_time
