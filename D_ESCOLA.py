import time
import pandas as pd
import numpy as np
from tqdm import tqdm


def extract_dim_escola(conn):
    escola_sql = '''
    select "PK_COD_ENTIDADE", "NO_ENTIDADE", "ID_DEPENDENCIA_ADM", "ID_LOCALIZACAO"
    FROM "STAGED".escola
    '''
    escola_tbl = pd.read_sql_query(escola_sql, conn)

    return escola_tbl


def treat_dim_escola(escola_tbl):
    escola_tbl = escola_tbl.rename(columns={'PK_COD_ENTIDADE': 'CD_ESCOLA',
         'NO_ENTIDADE': 'NO_ESCOLA',
         'ID_DEPENDENCIA_ADM': 'CD_DEPENDÊNCIA_ADMINISTRATIVA',
         'ID_LOCALIZACAO': 'CD_LOCALIZAÇÃO'})

    escola_tbl['NO_DEPENDÊNCIA_ADMINISTRATIVA'] = escola_tbl[
        'CD_DEPENDÊNCIA_ADMINISTRATIVA'].apply(lambda x: 'Federal' if x == 1 else
        'Estadual' if x == 2 else
        'Municipal' if x == 3 else
        'Privada' if x == 4 else -1)

    escola_tbl['DS_LOCALIZAÇÃO'] = escola_tbl[
        'CD_LOCALIZAÇÃO'].apply(lambda x: 'Urbana' if x == 1 else
        'Rural' if x == 2 else -1)

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
    divisor = 20
    df = np.array_split(dim_escola, divisor)
    print("Load da Dimensão Escola: \n")
    for i in tqdm(range(0, divisor)):
        if i != 0:
            df[i].to_sql(name='D_ESCOLA', con=conn, schema='DW',
                                if_exists='append',
                                index=False)
        else:
            df[i].to_sql(name='D_ESCOLA', con=conn, schema='DW',
                              if_exists='replace',
                              index=False)

def run_dim_escola(conn):
    start_time = time.time()
    escola_tbl = extract_dim_escola(conn)
    extract_time = time.time()
    print(f'D_ESCOLA\nextract: {extract_time - start_time:.3f}', )

    dim_escola = treat_dim_escola(escola_tbl)
    treat_time = time.time()
    print(f'treat: {treat_time - extract_time:.3f}', )

    load_dim_escola(dim_escola, conn)
    load_time = time.time()
    print(f'load: {load_time - treat_time:.3f}')

    return load_time - start_time