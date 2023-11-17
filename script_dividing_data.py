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

def insertStates():

    # EstadoID, Estado, Regiao

    query = '''

    SELECT DISTINCT `Estado`, `Região`
        FROM ProdutosDerivadosRefinaria

    '''

    df_base = pd.read_sql(query, conn)

    # Add a index column
    df_base['EstadoID'] = df_base.index

    # Rename the column 'Produto' to 'Nome'
    df_base.rename(columns={'Região': 'Regiao'}, inplace=True)


    # Create a new table in the database databaseRight.db
    create_table_query = '''

    CREATE TABLE Estado (
        EstadoID INTEGER PRIMARY KEY,
        Estado TEXT NOT NULL,
        Regiao TEXT NOT NULL
    );

    '''
    conn1.execute(create_table_query)

    # Insert into a table Produto in the database databaseRight.db with a sql query
    base_insert_query = '''
    INSERT INTO Estado (EstadoID, Estado, Regiao) VALUES 
    '''

    for row in df_base.itertuples():
        insert_query = base_insert_query + "({}, '{}', '{}')".format(row.EstadoID, row.Estado, row.Regiao)
        print(insert_query)
        conn1.execute(insert_query)


    df = pd.read_sql('SELECT * FROM Estado', conn1)
    print(df)

def insertRefinarias():

    # EstadoID, Estado, Regiao

    # states = pd.read_sql('SELECT * FROM Estado', conn1)

    conn1.execute('DROP TABLE IF EXISTS Refinaria;')

    query = '''

    SELECT `CNPJ`, `Instalação`, `Razão Social`, `Município`
        FROM ProdutosDerivadosRefinaria
        GROUP BY `CNPJ`
    '''

    df_base = pd.read_sql(query, conn)

    # Add a index column
    df_base['EmpID'] = df_base.index

    # Rename the column 'Produto' to 'Nome'
    df_base.rename(columns={'Razão Social': 'RazaoSocial'}, inplace=True)
    df_base.rename(columns={'Instalação': 'Instalacao'}, inplace=True)
    df_base.rename(columns={'Município': 'Municipio'}, inplace=True)


    foreignKeyconstraint = 'PRAGMA foreign_keys = 0;'
    conn1.execute(foreignKeyconstraint)

    # Create a new table in the database databaseRight.db
    create_table_query = '''

    CREATE TABLE Refinaria (
        EmpID INTEGER PRIMARY KEY,
        CNPJ TEXT NOT NULL,
        Instalacao TEXT NOT NULL,
        RazaoSocial TEXT NOT NULL,
        Municipio TEXT NOT NULL,
        FOREIGN KEY (EmpID) REFERENCES Empresa (EmpID)
    );

    '''
    conn1.execute(create_table_query)

    # Insert into a table Produto in the database databaseRight.db with a sql query
    base_insert_query = '''
    INSERT INTO Refinaria (EmpID, CNPJ, Instalacao, RazaoSocial, Municipio) VALUES
    '''

    for row in df_base.itertuples():
        insert_query = base_insert_query + "({}, '{}', '{}', '{}', '{}')".format(row.EmpID, row.CNPJ, row.Instalacao, row.RazaoSocial, row.Municipio) 
        print(insert_query)
        conn1.execute(insert_query)


    df = pd.read_sql('SELECT * FROM Refinaria', conn1)
    print(df)

# insertProdutos()
insertRefinarias()


conn.close()


conn1.commit()
conn1.close()