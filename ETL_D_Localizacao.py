import numpy as np
import CONNECTION as con
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

dimensao_localizacao['no_município'] = list(map(lambda x : x[0:-5],
                                                dimensao_localizacao['no_município']))

dimensao_localizacao['cd_estado'] = list(map(lambda x : x[0:2],
                                             dimensao_localizacao['cd_município']))

estado_cod = estado_cod.rename(columns={'no_município' : 'no_estado'})

dimensao_localizacao.merge(estado_cod, left_on='cd_estado',right_on='cd_município')

print(estado_cod)