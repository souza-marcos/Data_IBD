import pandas as pd
import sqlite3

conn = sqlite3.connect("database.db")
c = conn.cursor()

conn1 = sqlite3.connect("databaseRight.db")

df : pd.DataFrame

def insertProdutos():

    query = '''
    SELECT *
        FROM Estado
    '''

    df_base = pd.read_sql(query, conn1)
    print(df_base)
    # # Add a index column
    # df_base['EstadoID'] = df_base.index


    # # Create a new table in the database databaseRight.db
    # create_table_query = '''

    # CREATE TABLE Estado (
    #     EstadoID INTEGER PRIMARY KEY,
    #     Estado TEXT NOT NULL,
    #     Região TEXT NOT NULL
    # );

    # '''
    
    # conn1.execute(create_table_query)

    # # Insert into a table Produto in the database databaseRight.db with a sql query
    # base_insert_query = '''
    # INSERT INTO Estado (EstadoID, Estado, Região) VALUES 
    # '''

    # for row in df_base.itertuples():
    #     insert_query = base_insert_query + "({}, '{}', '{}')".format(row.EstadoID, row.Estado, row.Região)
    #     print(insert_query)
    #     conn1.execute(insert_query)


    # df = pd.read_sql('SELECT * FROM Estado', conn1)


insertProdutos()


conn.close()

conn1.commit()
conn1.close()