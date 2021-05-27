import pandas as pd
import numpy as np


def extract_dim_escola(conn):
    escola_sql = '''
    select "PK_COD_ENTIDADE", "NO_ENTIDADE", "ID_DEPENDENCIA_ADM", "ID_LOCALIZACAO"
    FROM "STAGED".escola
    '''
    escola_tbl = pd.read_sql_query(escola_sql, conn)

    return escola_tbl


def treat_dim_escola(escola_tbl):
    escola_tbl = escola_tbl.rename(columns={'PK_COD_ENTIDADE': 'cd_escola',
                                    'NO_ENTIDADE': 'no_escola',
                                    'ID_DEPENDENCIA_ADM': 'cd_dependência_administrativa',
                                    'ID_LOCALIZACAO': 'cd_localização'})

    escola_tbl['ds_dependência_administrativa'] = list(map(lambda x:
                                                           'Federal' if x == 1 else
                                                           'Estadual' if x == 2 else
                                                           'Municipal' if x == 3 else
                                    np.nan, escola_tbl['cd_dependência_administrativa']))

    escola_tbl['ds_localização'] = list(map(lambda x:
                                            'Urbana' if x == 1 else
                                            'Rural' if x == 2 else
                                            np.nan, escola_tbl['cd_localização']))

    escola_tbl['sk_escola'] = np.arange(0, len(escola_tbl))

    return escola_tbl


def load_dim_escola(dim_escola, conn):
    dim_escola.to_sql(name='D_ESCOLA', con=conn, schema='DW',
                      if_exists='replace',
                      index=False,
                      chunksize=100)


def run_dim_escola(conn):
    escola_tbl = extract_dim_escola(conn)
    dim_escola = treat_dim_escola(escola_tbl)
    load_dim_escola(dim_escola, conn)
