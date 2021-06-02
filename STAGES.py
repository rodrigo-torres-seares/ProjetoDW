import pandas as pd
import numpy as np
from tqdm import tqdm

def create_stg_tables_from_csv(path_location, delimiter, conn_output, coding, table_name):
    df = pd.read_csv(path_location, sep=delimiter, low_memory=False, encoding=coding
                     ).to_sql(name=table_name,con=conn_output, schema='STAGED',
                              if_exists='replace', chunksize=100, index=False)


def create_stg_table_from_dataframe(df, conn_output, table_name):
    df.to_sql(name=table_name, con=conn_output, schema='DIMENSIONS',
              if_exists='replace', chunksize=100)


#Baseado no dict_ti_str + concat_cols
def concat_cols(str):
    str = [f'"{x}"' for x in str]
    return ', '.join(str)


def read_table_postgres(conn, schema, table_name, columns=None, where=None, distinct=False):
    if distinct:
        distinct_clause = "DISTINCT"
    else:
        distinct_clause = ""

    if where is None:
        where_clause = ""
    else:
        where_clause = f"WHERE {where}"

    if columns is None:
        query = f'SELECT {distinct_clause} * FROM "{schema}"."{table_name}" {where_clause}'
    else:
        query = f'SELECT {distinct_clause} {concat_cols(columns)} FROM "{schema}"."{table_name}" {where_clause}'

    response = pd.read_sql_query(query, conn)

    return response


def load_to_sql_postgres(conn, dim, divisor, table_name, schema):
    dim = np.array_split(dim, divisor)
    print(f"Load: {table_name} \n")
    dim[0].to_sql(name=table_name, con=conn, schema=schema,
                  if_exists='replace',
                  index=False)

    for i in tqdm(range(1, divisor)):
        dim[i].to_sql(name=table_name, con=conn, schema=schema,
                      if_exists='append',
                      index=False)