import STAGES as stg
import time
import pandas as pd
import numpy as np


def extract_dim_aluno(conn):
    aluno_sql = '''
    select "ID_ALUNO", "ID_TURMA", "ID_SERIE", "ID_TURNO", "IN_SITUACAO_CENSO"
    FROM "STAGED".resultado_aluno
    '''

    aluno_tbl = pd.read_sql_query(aluno_sql, conn)

    return aluno_tbl


def treat_dim_aluno(aluno_tbl):
    aluno_tbl = aluno_tbl.rename(columns={'ID_ALUNO': 'cd_aluno',
                                          'ID_TURMA': 'cd_turma',
                                          'ID_SERIE': 'cd_série',
                                          'ID_TURNO': 'cd_turno',
                                          'IN_SITUACAO_CENSO': 'fl_situação_censo'})

    aluno_tbl['cd_turno'] = aluno_tbl['cd_turno'].astype(int)
    aluno_tbl['ds_turno'] = aluno_tbl['cd_turno'].apply(lambda x: 'Matutino' if x == 1 else
                                                'Vespertino' if x == 2 else
                                                'Noturno' if x == 3 else
                                                'Intermediário' if x == 4 else
                                                -1)

    aluno_tbl['ds_série'] = aluno_tbl['cd_série'].apply(lambda x: '4ª série/5º ano EF'
    if x == 5
    else
                                                '8ª série/9º ano EF' if x == 9 else
                                                -1 )

    aluno_tbl['fl_situação_censo'] = list(map(lambda x: True if x == 1 else False,
                                              aluno_tbl['fl_situação_censo']))

    return aluno_tbl


def load_dim_aluno(dim_aluno, conn):
    dim_aluno.to_sql(name='D_ALUNO', con=conn, schema='DW',
                      if_exists='replace',
                      index=False,
                      chunksize=100)


def run_dim_aluno(conn):
    start = time.time()
    aluno_tbl = extract_dim_aluno(conn)
    dim_escola = treat_dim_aluno(aluno_tbl)
    end = time.time()
    # load_dim_aluno(dim_escola, conn)