import CONNECTION as con
import STAGES as stg

connection_postgre = con.create_connection_postgre('localhost', 'projetodw',
                                                      'postgres', 'itix.123', '5432')
coding = 'utf8'

stg.create_stg_tables_from_csv('Datasets/TS_RESULTADO_ALUNO.csv', ';',
                               connection_postgre, coding, 'resultado_aluno')

stg.create_stg_tables_from_csv('Datasets/ESCOLAS.CSV', '|', connection_postgre,
                               'ISO-8859-1', 'escola')

stg.create_stg_tables_from_csv('Datasets/DADOS_IBGE.csv', ';', connection_postgre,
                               coding, 'dados_ibge')
print("Banco atualizado!")