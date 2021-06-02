import CONEXAO as con
import D_LOCALIDADE
import D_ESCOLA
import D_TURMA
import F_APLICACAO_QUESTIONARIO
import time
import STAGES as stg

connection_postgre = con.create_connection_postgre('10.3.152.12', 'projetodw',
                                                      'postgres', 'itix.123', '5432')
coding = 'utf8'

# stg.create_stg_tables_from_csv('Datasets/TS_RESULTADO_ALUNO.csv', ';',
#                                connection_postgre, coding, 'resultado_aluno')
#
# stg.create_stg_tables_from_csv('Datasets/ESCOLAS.CSV', '|', connection_postgre,
#                                'ISO-8859-1', 'escola')
#
# stg.create_stg_tables_from_csv('Datasets/DADOS_IBGE.csv', ';', connection_postgre,
#                                coding, 'dados_ibge')
#
start_time = time.time()
# print(f"Tempo total D_LOCALIDADE: {D_LOCALIDADE.run_dim_localidade(connection_postgre):.4f}",
#       "\n\n---------------------------------------------------------------------------\n")
#
# D_ESCOLA.run_dim_escola(connection_postgre)
#
# D_TURMA.run_dim_turma(connection_postgre)
#       "\n\n---------------------------------------------------------------------------\n")

F_APLICACAO_QUESTIONARIO.run_fact_aplicacao_questionario(connection_postgre)
#
end_time = time.time()
print(f"\nBanco atualizado em : {end_time - start_time:.2f} segundos")



"""
LOG-Saída

D_LOCALIDADE
extract: 0.082
treat: 0.029
load: 0.636
Tempo total D_LOCALIDADE: 0.7478 

---------------------------------------------------------------------------

D_ESCOLA
extract: 1.737
treat: 0.317
Load da Dimensão Escola: 

100%|██████████| 20/20 [00:12<00:00,  1.62it/s]
load: 12.420
Tempo total D_ESCOLA: 14.4743 

---------------------------------------------------------------------------

D_TURMA
extract: 29.460
treat: 7.004
Load da Dimensão Turma: 

100%|██████████| 50/50 [04:24<00:00,  5.28s/it]
load: 264.627
Tempo total D_TURMA: 301.0906 

---------------------------------------------------------------------------

F_Aplicação_Questionário
extract: 102.540
treat: 43.499
Load da Fato Questionário: 

100%|██████████| 50/50 [21:26<00:00, 25.73s/it]
load: 1287.596
Tempo total F_Aplicação_Questionário: 1433.6343 

-----------------------------------------------------------------------------

Banco atualizado em : 1765.74 segundos

"""