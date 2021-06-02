import time
import pandas as pd
import numpy as np
import STAGES as stg
from tqdm import tqdm


def extract_dim_escola(conn):
    escola_tbl = stg.read_table_postgres(conn,
                                         schema='STAGED',
                                         table_name='escola',
                                         columns=["PK_COD_ENTIDADE",
                                                  "NO_ENTIDADE",
                                                  "ID_DEPENDENCIA_ADM",
                                                  "ID_LOCALIZACAO"]
                                         )

    return escola_tbl


def treat_dim_escola(escola_tbl):
    escola_tbl = (escola_tbl.rename(columns={'PK_COD_ENTIDADE': 'CD_ESCOLA',
                                            'NO_ENTIDADE': 'NO_ESCOLA',
                                            'ID_DEPENDENCIA_ADM': 'CD_DEPENDÊNCIA_ADMINISTRATIVA',
                                            'ID_LOCALIZACAO': 'CD_LOCALIZAÇÃO'}
                ).assign(
                        CD_DEPENDÊNCIA_ADMINISTRATIVA=lambda y: y.CD_DEPENDÊNCIA_ADMINISTRATIVA.apply(
                                                    lambda x: 'Federal' if x == 1 else
                                                              'Estadual' if x == 2 else
                                                              'Municipal' if x == 3 else
                                                              'Privada' if x == 4 else -1),
                        DS_LOCALIZAÇÃO=lambda y: y.CD_LOCALIZAÇÃO.apply(
                            lambda x: 'Urbana' if x == 1 else
                                      'Rural' if x == 2 else -1)
        )
    )

    escola_tbl['SK_ESCOLA'] = np.arange(1, len(escola_tbl) + 1)

    escola_tbl = pd.concat([pd.DataFrame(
        [[-1, "Não informado", -1, -1, "Não informado", "Não informado", -1],
        [-2, "Não aplicável", -2, -2, "Não aplicável", "Não aplicável", -2],
        [-3, "Desconhecido", -3, -3, "Desconhecido", "Desconhecido", -3]],
        columns=[
            'CD_ESCOLA', 'NO_ESCOLA', 'CD_DEPENDÊNCIA_ADMINISTRATIVA', 'CD_LOCALIZAÇÃO',
            'DS_LOCALIZAÇÃO', 'NO_DEPENDÊNCIA_ADMINISTRATIVA', 'SK_ESCOLA']),
        escola_tbl], ignore_index=True
    )

    ordemcolunas = ['SK_ESCOLA', 'CD_ESCOLA', 'NO_ESCOLA', 'CD_DEPENDÊNCIA_ADMINISTRATIVA',
                    'NO_DEPENDÊNCIA_ADMINISTRATIVA', 'CD_LOCALIZAÇÃO', 'DS_LOCALIZAÇÃO']

    escola_tbl = escola_tbl[ordemcolunas]

    return escola_tbl


def load_dim_escola(dim_escola, conn):
    stg.load_to_sql_postgres(conn,
                             dim=dim_escola,
                             divisor=20,
                             table_name='D_ESCOLA',
                             schema='DW')


def run_dim_escola(conn):
    extract_dim_escola(conn).pipe(treat_dim_escola).pipe(load_dim_escola, conn=conn)
