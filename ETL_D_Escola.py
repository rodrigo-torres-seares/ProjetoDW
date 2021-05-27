import CONNECTION as con
import pandas as pd
import STAGES as stg
import numpy as np
import sqlalchemy

connection_postgre = con.create_connection_postgre('localhost', 'projetodw',
                                                      'postgres', 'itix.123', '5432')
sql_escola = '''
select "PK_COD_ENTIDADE", "NO_ENTIDADE", "ID_DEPENDENCIA_ADM", "ID_LOCALIZACAO"
FROM "STAGED".escola
'''
dimensao_escola = pd.read_sql_query(sql_escola, connection_postgre)
dimensao_escola.index.name = 'sk_escola'
dimensao_escola = dimensao_escola.rename(columns={'PK_COD_ENTIDADE' : 'cd_escola',
 'NO_ENTIDADE' : 'no_escola', 'ID_DEPENDENCIA_ADM' : 'cd_dependência_administrativa',
                                'ID_LOCALIZACAO' : 'cd_localização'} )

dimensao_escola['ds_dependência_administrativa'] = list(map(lambda x:
                                                            'Federal' if x == 1 else
                                                            'Estadual' if x == 2 else
                                                            'Municipal' if x == 3 else
                                np.nan, dimensao_escola['cd_dependência_administrativa']))

dimensao_escola['ds_localização'] = list(map(lambda x :
                                             'Urbana' if x == 1 else
                                             'Rural' if x == 2 else
                                             np.nan, dimensao_escola['cd_localização']))

stg.create_stg_table_from_dataframe(dimensao_escola, connection_postgre, 'D_Escola')

