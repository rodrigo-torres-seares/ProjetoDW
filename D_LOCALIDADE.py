import pandas as pd
import numpy as np

def extract_dim_localidade(conn):
    município_sql = '''
    select "Cód.", "Brasil Grande Região Unidade da Federação e Município"
    FROM "STAGED".dados_ibge WHERE "Nível" LIKE 'MU'
    '''

    estado_sql = '''
    select "Cód.", "Brasil Grande Região Unidade da Federação e Município"
    FROM "STAGED".dados_ibge WHERE "Nível" LIKE 'UF'
    '''

    município_tbl = pd.read_sql_query(município_sql, conn)
    estado_tbl = pd.read_sql_query(estado_sql, conn)

    return município_tbl, estado_tbl


def treat_dim_localidade(localidade_tbl, estado_tbl):
    localidade_tbl = localidade_tbl.rename(columns={'Cód.' : 'cd_município',
                'Brasil Grande Região Unidade da Federação e Município' : 'no_município'})

    estado_tbl = estado_tbl.rename(columns={'Cód.': 'cd_estado',
                'Brasil Grande Região Unidade da Federação e Município': 'no_estado'})

    localidade_tbl['no_uf'] = list(map(lambda x : x[-3:-1],
                                        localidade_tbl['no_município']))

    localidade_tbl['no_município'] = list(map(lambda x : x[:-5],
                                                localidade_tbl['no_município']))

    localidade_tbl['cd_município'] = localidade_tbl['cd_município'].astype(str)
    localidade_tbl['cd_estado'] = list(map(lambda x : x[:2],
                                             localidade_tbl['cd_município']))
    localidade_tbl['cd_município'] = localidade_tbl['cd_município'].astype(int)

    localidade_tbl['cd_estado'] = localidade_tbl['cd_estado'].astype(int)
    localidade_tbl = localidade_tbl.merge(estado_tbl, left_on='cd_estado',
                                                right_on='cd_estado')

    localidade_tbl['sk_localidade'] = np.arange(0, len(localidade_tbl))
    return localidade_tbl

def load_dim_localidade(dim_localidade, conn):
    dim_localidade.to_sql(name='D_LOCALIDADE', con=conn, schema='DW',
                                                if_exists='replace',
                                                index=False,
                                                chunksize=100)

def run_dim_localidade(conn_output):

    município_tbl, estado_tbl = extract_dim_localidade(conn_output)

    dim_localidade = treat_dim_localidade(município_tbl, estado_tbl)

    load_dim_localidade(dim_localidade, conn_output)


