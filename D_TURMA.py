import time
import pandas as pd
import numpy as np
import STAGES as stg
from tqdm import tqdm


def extract_dim_turma(conn):
    turma_tbl = stg.read_table_postgres(conn,
                                        schema='STAGED',
                                        table_name='resultado_aluno',
                                        columns=["ID_ALUNO",
                                                 "ID_TURMA",
                                                 "ID_SERIE",
                                                 "ID_TURNO",
                                                 "IN_SITUACAO_CENSO"])

    return turma_tbl


def treat_dim_turma(turma_tbl):
    turma_tbl = (turma_tbl.rename(columns={'ID_ALUNO': 'CD_ALUNO',
                                          'ID_TURMA': 'CD_TURMA',
                                          'ID_SERIE': 'CD_SÉRIE',
                                          'ID_TURNO': 'CD_TURNO',
                                          'IN_SITUACAO_CENSO': 'FL_SITUAÇÃO_CENSO'}
                                  ).assign(
                    DS_TURNO=lambda y: y.CD_TURNO.apply(lambda x: 'Matutino' if x == '1' else
                                                                  'Vespertino' if x == '2' else
                                                                  'Noturno' if x == '3' else
                                                                  'Intermediário' if x == '4'
                                                                  else 'Não Informado'),
                    DS_SÉRIE=lambda y: y.CD_SÉRIE.apply(lambda x:
                                                        '4ª série/5º ano EF' if x == 5 else
                                                        '8ª série/9º ano EF' if x == 9
                                                        else 'Não Informado'),
                    CD_TURNO=lambda x: x.CD_TURNO.replace(' ', '-1'),
                    )
    )
    turma_tbl['CD_TURNO'] = turma_tbl['CD_TURNO'].astype(int)

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
    stg.load_to_sql_postgres(conn,
                             dim=dim_turma,
                             divisor=50,
                             table_name='D_TURMA',
                             schema='DW')


def run_dim_turma(conn):
    extract_dim_turma(conn).pipe(treat_dim_turma).pipe(load_dim_turma, conn)
