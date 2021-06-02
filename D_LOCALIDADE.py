import pandas as pd
import numpy as np
import STAGES as stg
import time


def extract_dim_localidade(conn):
    nivel_mu = "'MU'"
    nivel_uf = "'UF'"

    municipio_tbl = stg.read_table_postgres(conn,
                                            schema='STAGED',
                                            table_name='dados_ibge',
                                            columns=["Cód.",
                                                     "Brasil Grande Região Unidade da Federação e Município"],
                                            where=f'"Nível" LIKE {nivel_mu}'
                                            )

    estado_tbl = stg.read_table_postgres(conn,
                                         schema='STAGED',
                                         table_name='dados_ibge',
                                         columns=["Cód.",
                                                  "Brasil Grande Região Unidade da Federação e Município"],
                                         where=f'"Nível" LIKE {nivel_uf}'
                                         )

    return municipio_tbl, estado_tbl


def treat_tbl_municipio(tbl):
    tbl_municipio = (
        tbl.rename(columns={'Cód.': 'CD_MUNICÍPIO',
                            'Brasil Grande Região Unidade da Federação e Município': 'NO_MUNICÍPIO'}
        ).assign(
                    NO_UF=lambda x: x.NO_MUNICÍPIO.apply(lambda y: y[-3:-1]),
                    NO_MUNICÍPIO=lambda x: x.NO_MUNICÍPIO.apply(lambda y: y[:-5]),
                    CD_MUNICÍPIO=lambda x: x.CD_MUNICÍPIO.astype(str),
                    CD_ESTADO=lambda x: x.CD_MUNICÍPIO.apply(lambda y: y[:2]),
        )
    )

    return tbl_municipio


def treat_tbl_estado(tbl):
    tbl_estado = (
        tbl.rename(columns={'Cód.': 'CD_ESTADO',
                            'Brasil Grande Região Unidade da Federação e Município': 'NO_ESTADO'}
        )
    )

    return tbl_estado


def treat_dim_localidade(municipio_tbl, estado_tbl):
    dim_localidade = (
                municipio_tbl.assign(
                    CD_MUNICÍPIO=lambda x: x.CD_MUNICÍPIO.astype("int64"),
                    CD_ESTADO=lambda x: x.CD_ESTADO.apply("int64")
                ).merge(estado_tbl, left_on='CD_ESTADO', right_on='CD_ESTADO'
        )
    )
    print(dim_localidade)
    dim_localidade['SK_LOCALIDADE'] = np.arange(1, len(dim_localidade) + 1)

    dim_localidade = pd.concat(
        [pd.DataFrame([
            [-1, "Não informado", -1, "Não informado", "Não informado", -1],
            [-2, "Não aplicável", -2, "Não aplicável", "Não aplicável", -2],
            [-3, "Desconhecido", -3, "Desconhecido", "Desconhecido", -3]],
            columns=[
            'CD_MUNICÍPIO', 'NO_MUNICÍPIO', 'CD_ESTADO', 'NO_ESTADO',
            'NO_UF', 'SK_LOCALIDADE']),
            dim_localidade], ignore_index=True
    )

    ordemcolunas = ['SK_LOCALIDADE', 'CD_MUNICÍPIO', 'NO_MUNICÍPIO', 'CD_ESTADO',
                    'NO_ESTADO', 'NO_UF']

    dim_localidade = dim_localidade[ordemcolunas]

    return dim_localidade


def load_dim_localidade(dim_localidade, conn):
    dim_localidade.to_sql(name='D_LOCALIDADE', con=conn, schema='DW',
                          if_exists='replace',
                          index=False,
                          chunksize=40)


def run_dim_localidade(conn_output):
    start_time = time.time()
    municipio_tbl, estado_tbl = extract_dim_localidade(conn_output)
    extract_time = time.time()
    print(f'D_LOCALIDADE\nextract: {extract_time - start_time:.3f}')

    municipio_tbl = treat_tbl_municipio(municipio_tbl)
    print(municipio_tbl)
    estado_tbl = treat_tbl_estado(estado_tbl)
    print(estado_tbl)
    dim_localidade = treat_dim_localidade(municipio_tbl, estado_tbl)
    treat_time = time.time()
    print(f'treat: {treat_time - extract_time:.3f}')

    load_dim_localidade(dim_localidade, conn_output)
    load_time = time.time()
    print(f'load: {load_time - treat_time:.3f}')

    return load_time - start_time
