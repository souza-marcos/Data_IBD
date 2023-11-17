import pandas as pd
# Connection with the database sqlite3
import sqlite3
conn = sqlite3.connect('database.db')
c = conn.cursor()

conn1 = sqlite3.connect('databaseRight.db')

df : pd.DataFrame

def insertProdutos():

    query = '''

    SELECT DISTINCT Produto
        FROM ProdutosDerivadosRefinaria

    '''

    df_base = pd.read_sql(query, conn)

    # Add a index column
    df_base['ProdID'] = df_base.index

    # Transform string between parentheses in the column 'Produto' in a new column called 'Unidade'
    df_base['Unidade'] = df_base['Produto'].str.extract(r'\((.*?)\)', expand=False)

    # Remove the parentheses and the content between them
    df_base['Produto'] = df_base['Produto'].str.slice_replace(start=-4, stop=-1, repl='').str.replace(')', '')

    # Rename the column 'Produto' to 'Nome'
    df_base.rename(columns={'Produto': 'Nome'}, inplace=True)


    # Create a new table in the database databaseRight.db
    # create_table_query = '''

    # CREATE TABLE Produto (
    #     ProdID INTEGER PRIMARY KEY,
    #     Nome TEXT NOT NULL,
    #     Unidade TEXT NOT NULL
    # );

    # '''
    # conn1.execute(create_table_query)

    # Insert into a table Produto in the database databaseRight.db with a sql query
    base_insert_query = '''
    INSERT INTO Produto (ProdID, Nome, Unidade) VALUES 
    '''

    for row in df_base.itertuples():
        insert_query = base_insert_query + "({}, '{}', '{}')".format(row.ProdID, row.Nome, row.Unidade)
        print(insert_query)
        conn1.execute(insert_query)


    df = pd.read_sql('SELECT * FROM Produto', conn1)



insertProdutos()



conn.close()


conn1.commit()
conn1.close()