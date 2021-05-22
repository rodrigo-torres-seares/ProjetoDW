import numpy as np
import CONNECTION as con
import STAGES as stg
import pandas as pd

connection_postgre = con.create_connection_postgre('localhost', 'projetodw',
                                                      'postgres', 'itix.123', '5432')
sql_localizacao = '''
select "Nível" , "Cód.", "Brasil Grande Região Unidade da Federação e Município", 
"População"
FROM "STAGED".dados_ibge
'''

dados_ibge = pd.read_sql_query(sql_localizacao, connection_postgre)
del dados_ibge['População']

dados_ibge = dados_ibge.rename(columns={'Cód.' : 'cd_município',
                'Brasil Grande Região Unidade da Federação e Município' : 'no_município'})

dimensao_localizacao = dados_ibge.query('Nível == "MU"')

estado_cod = dados_ibge.query('Nível == "UF"')

del dimensao_localizacao['Nível']
del estado_cod['Nível']

print(dimensao_localizacao)

dimensao_localizacao['no_uf'] = list(map(lambda x : x[-3:-1],
                                                dimensao_localizacao['no_município']))

dimensao_localizacao['no_município'] = list(map(lambda x : x[:-5],
                                                dimensao_localizacao['no_município']))

dimensao_localizacao['cd_estado'] = list(map(lambda x : x[:2],
                                             dimensao_localizacao['cd_município']))

estado_cod = estado_cod.rename(columns={'no_município' : 'no_estado'})

dimensao_localizacao = dimensao_localizacao.merge(estado_cod, left_on='cd_estado',
                                                right_on='cd_município')

del dimensao_localizacao['cd_município_y']

dimensao_localizacao = dimensao_localizacao.rename(columns={'cd_município_x' :
                                                                'cd_município'})

stg.create_stg_table_from_dataframe(dimensao_localizacao, connection_postgre,
                                    'D_Localização')