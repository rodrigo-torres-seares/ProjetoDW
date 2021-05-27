import CONEXAO as con
import STAGES as stg
import pandas as pd
import numpy as np

connection_postgre = con.create_connection_postgre('localhost', 'projetodw',
                                                      'postgres', 'itix.123', '5432')

def extract_dim_aluno(conn):

    sql_aluno = '''
    select "ID_ALUNO", "ID_TURMA", "ID_SERIE", "ID_TURNO", "IN_SITUACAO_CENSO"
    FROM "STAGED".resultado_aluno
    '''

    dim = pd.read_sql_query(sql_aluno, connection_postgre)
    dim.index.name = 'sk_aluno'

    dim = dimensao_aluno.rename(columns={'ID_ALUNO' : 'cd_aluno',
                'ID_TURMA' : 'cd_turma', 'ID_SERIE' : 'cd_série' ,'ID_TURNO' : 'cd_turno',
                                            'IN_SITUACAO_CENSO' : 'fl_situação_censo'})
    #cd_turno contém valores vazios
    return dim

dimensao_aluno['cd_turno']= list(map(lambda x : '-1' if x == ' ' else x,
                                     dimensao_aluno['cd_turno']))

dimensao_aluno['cd_turno'] = dimensao_aluno['cd_turno'].astype(int)
dimensao_aluno['ds_turno'] = list(map(lambda x : 'Matutino' if x == 1 else
                                      'Vespertino' if x == 2 else
                                      'Noturno' if x == 3 else
                                      'Intermediário' if x == 4 else
                                      np.nan, dimensao_aluno['cd_turno']))

dimensao_aluno['ds_série'] = list(map(lambda x : '4ª série/5º ano EF' if x == 5 else
                                            '8ª série/9º ano EF' if x == 9 else
                                            np.nan, dimensao_aluno['cd_série']))

dimensao_aluno['fl_situação_censo'] = list(map(lambda x: True if x == 1 else False,
                                                  dimensao_aluno['fl_situação_censo']))

stg.create_stg_table_from_dataframe(dimensao_aluno, connection_postgre, 'D_Aluno')