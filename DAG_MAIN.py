import CONEXAO as con
import D_LOCALIDADE
import D_ESCOLA
import D_TURMA
import F_APLICACAO_QUESTIONARIO
import time
import STAGES as stg

connection_postgre = con.create_connection_postgre('localhost', 'projetodw',
                                                      'postgres', 'itix.123', '5432')
coding = 'utf8'
#
# stg.create_stg_tables_from_csv('Datasets/TS_RESULTADO_ALUNO.csv', ';',
#                                connection_postgre, coding, 'resultado_aluno')
#
# stg.create_stg_tables_from_csv('Datasets/ESCOLAS.CSV', '|', connection_postgre,
#                                'ISO-8859-1', 'escola')
#
# stg.create_stg_tables_from_csv('Datasets/DADOS_IBGE.csv', ';', connection_postgre,
#                                coding, 'dados_ibge')
#
# start_time = time.time()
# print("Tempo total D_LOCALIDADE: ", D_LOCALIDADE.run_dim_localidade(connection_postgre),"\n\n")
#
# print("Tempo total D_ESTOLA: ", D_ESCOLA.run_dim_escola(connection_postgre),"\n\n")
#
# print("Tempo total D_TURMA: ", D_TURMA.run_dim_turma(connection_postgre),"\n\n")

print("Tempo total F_APLICACAO: ",
      F_APLICACAO_QUESTIONARIO.run_fact_aplicacao_questionario(
    connection_postgre),"\n\n")

end_time = time.time()
print("\nBanco atualizado ! tempo em segundos: ", end_time - start_time)