import time
import pandas as pd
import numpy as np
from tqdm import tqdm


def extract_dim_turma(conn):
    turma_sql = '''
    select "ID_ALUNO", "ID_TURMA", "ID_SERIE", "ID_TURNO", "IN_SITUACAO_CENSO"
    FROM "STAGED".resultado_aluno
    '''

    turma_tbl = pd.read_sql_query(turma_sql, conn)

    return turma_tbl


def treat_dim_turma(turma_tbl):
    turma_tbl = turma_tbl.rename(columns={'ID_ALUNO': 'CD_ALUNO',
                                          'ID_TURMA': 'CD_TURMA',
                                          'ID_SERIE': 'CD_SÉRIE',
                                          'ID_TURNO': 'CD_TURNO',
                                          'IN_SITUACAO_CENSO': 'FL_SITUAÇÃO_CENSO'})

    turma_tbl['CD_TURNO'] = turma_tbl['CD_TURNO'].apply(lambda x: -1 if x == ' ' else x)
    turma_tbl['CD_TURNO'] = turma_tbl['CD_TURNO'].astype(int)
    turma_tbl['DS_TURNO'] = turma_tbl['CD_TURNO'].apply(
        lambda x: 'Matutino' if x == 1 else
        'Vespertino' if x == 2 else
        'Noturno' if x == 3 else
        'Intermediário' if x == 4 else -1)

    turma_tbl['DS_SÉRIE'] = turma_tbl['CD_SÉRIE'].apply(lambda x: '4ª série/5º ano EF'
        if x == 5 else '8ª série/9º ano EF'
        if x == 9 else -1)

    turma_tbl['FL_SITUAÇÃO_CENSO'] = turma_tbl['FL_SITUAÇÃO_CENSO'].apply(lambda x: -1
        if x == 1 else 0)

    turma_tbl['SK_TURMA'] = np.arange(1, len(turma_tbl) + 1)

    turma_tbl = pd.concat([pd.DataFrame(
        [[-1, -1, -1, -1, -1, "Não informado", "Não informado", -1],
         [-2, -2, -2, -2, -2, "Não aplicável", "Não aplicável", -2],
         [-3, -3, -3, -3, -3, "Desconhecido", "Desconhecido", -3]],
        columns=[
            'CD_ALUNO', 'CD_TURMA', 'CD_SÉRIE', 'CD_TURNO',
            'FL_SITUAÇÃO_CENSO', 'DS_TURNO', 'DS_SÉRIE', 'SK_TURMA']),
        turma_tbl], ignore_index=True
    )

    ordemturma = ['SK_TURMA', 'CD_ALUNO', 'CD_TURMA', 'CD_SÉRIE', 'DS_SÉRIE',
                  'CD_TURNO',  'DS_TURNO', 'FL_SITUAÇÃO_CENSO']

    turma_tbl = turma_tbl[ordemturma]

    return turma_tbl


def load_dim_turma(dim_turma, conn):
    divisor = 50
    df = np.array_split(dim_turma, divisor)
    print("Load da Dimensão Turma: \n")
    for i in tqdm(range(0, divisor)):
        if i != 0:
            df[i].to_sql(name='D_TURMA', con=conn, schema='DW',
                         if_exists='append', index=False)

        else:
            df[i].to_sql(name='D_TURMA', con=conn, schema='DW',
                         if_exists='replace', index=False)


def run_dim_turma(conn):
    start_time = time.time()
    turma_tbl = extract_dim_turma(conn)
    extract_time = time.time()
    print(f'D_TURMA\nextract: {extract_time - start_time:.3f}')

    dim_turma = treat_dim_turma(turma_tbl)
    treat_time = time.time()
    print(f'treat: {treat_time - extract_time:.3f}')

    load_dim_turma(dim_turma, conn)
    load_time = time.time()
    print(f'load: {load_time - treat_time:.3f}')

    return load_time - start_time
