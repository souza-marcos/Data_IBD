import pandas as pd
import sqlite3
conn = sqlite3.connect('database.db')
c = conn.cursor()

conn1 = sqlite3.connect('databaseRight.db')
c1 = conn1.cursor()

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

    states = pd.read_sql('SELECT * FROM Estado', conn1)

    conn1.execute('DROP TABLE IF EXISTS Refinaria;')
    conn1.execute('DROP TABLE IF EXISTS Empresa;')

    query = '''

    SELECT `CNPJ`, `Instalação`, `Razão Social`, `Município`, `Estado`, `Região`
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
    df_base.rename(columns={'Região': 'Regiao'}, inplace=True)


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


    # Inserindo esses registros na tabela Empresa
    create_table_query = '''

    CREATE TABLE Empresa (
        EmpID INTEGER PRIMARY KEY,
        Nome TEXT NOT NULL,
        EstadoID INTEGER NOT NULL,
        ProdutorIndependente INTEGER NOT NULL,
        FOREIGN KEY (EstadoID) REFERENCES Estado (EstadoID)
    );

    '''

    conn1.execute(create_table_query)
    

    base_insert_query = '''
    INSERT INTO Empresa (EmpID, Nome, EstadoID, ProdutorIndependente) VALUES
    '''

    for row in df_base.itertuples():
        insert_query = base_insert_query + "({}, '{}', {}, {})".format(row.EmpID, row.RazaoSocial, states[states['Estado'] == row.Estado]['EstadoID'].values[0], 0)
        print(insert_query)
        conn1.execute(insert_query)


    df = pd.read_sql('SELECT * FROM Refinaria', conn1) 
    print(df)

def insertPetroquimicas():

    states = pd.read_sql('SELECT * FROM Estado', conn1)

    # EstadoID, Estado, Regiao

    c1.execute('SELECT MAX(EmpID) FROM Empresa')
    lastid = c1.fetchone()[0] 

    conn1.execute('DROP TABLE IF EXISTS CentralPetroquimica;')

    query = '''

    SELECT `CNPJ`, `Município`, `Estado`, `Região`, `Razão Social`
        FROM ProducaoDerivadosCentralPetroquimica
        GROUP BY `CNPJ`
    '''

    df_base = pd.read_sql(query, conn)

    # Add a index column
    df_base['EmpID'] = df_base.index

    # Map the EmpID adding the last id of the table Refinaria
    df_base['EmpID'] = df_base['EmpID'].map(lambda x: x + (lastid + 1))


    df_base.rename(columns={'Município': 'Municipio'}, inplace=True)
    df_base.rename(columns={'Região': 'Regiao'}, inplace=True)
    df_base.rename(columns={'Razão Social': 'RazaoSocial'}, inplace=True)


    foreignKeyconstraint = 'PRAGMA foreign_keys = 0;'
    conn1.execute(foreignKeyconstraint)

    # Create a new table in the database databaseRight.db
    create_table_query = '''

    CREATE TABLE CentralPetroquimica (
        EmpID INTEGER PRIMARY KEY,
        CNPJ TEXT NOT NULL,
        Municipio TEXT NOT NULL,
        FOREIGN KEY (EmpID) REFERENCES Empresa (EmpID)
    );

    '''
    conn1.execute(create_table_query)

    # Insert into a table Produto in the database databaseRight.db with a sql query
    base_insert_query = '''
    INSERT INTO CentralPetroquimica (EmpID, CNPJ, Municipio) VALUES
    '''    

    for row in df_base.itertuples():
        insert_query = base_insert_query + "({}, '{}', '{}')".format(row.EmpID, row.CNPJ, row.Municipio)
        conn1.execute(insert_query)


    # Inserindo esses registros na tabela Empresa
    base_insert_query = '''
    INSERT INTO Empresa (EmpID, Nome, EstadoID, ProdutorIndependente) VALUES
    '''

    for row in df_base.itertuples():
        insert_query = base_insert_query + "({}, '{}', {}, {})".format(row.EmpID, row.RazaoSocial, states[states['Estado'] == row.Estado]['EstadoID'].values[0], 0)
        print(insert_query)
        conn1.execute(insert_query)





    # df = pd.read_sql('SELECT * FROM CentralPetroquimica', conn1)
    # print(df)

def insertAutorizacao():

    conn1.execute('DROP TABLE IF EXISTS Autorizacao;')

    query = '''

    SELECT `Mês/Ano`, `CNPJ`, `Capacidade Autorizada (m³/d)`
        FROM ProcessamentoPetroleo
    '''

    df_base = pd.read_sql(query, conn)

    # Renaming Columns
    df_base.rename(columns={'Mês/Ano': 'DataConcessao'}, inplace=True)
    df_base.rename(columns={'Capacidade Autorizada (m³/d)': 'Capacidade'}, inplace=True)
    
    df_base['DataConcessao'] = pd.to_datetime(df_base['DataConcessao'], format='%m/%Y')

    # Group by 'CNPJ' and 'Capacidade Autorizada (m³/d)', and then aggregate using the first 'Mês/Ano'
    df_base = df_base.groupby(['CNPJ', 'Capacidade']).agg({'DataConcessao': 'first'}).reset_index()

    # Add a index column
    df_base['AutoID'] = df_base.index

    foreignKeyconstraint = 'PRAGMA foreign_keys = 0;'
    conn1.execute(foreignKeyconstraint)

    # Create a new table in the database databaseRight.db
    create_table_query = '''

    CREATE TABLE Autorizacao (
        AutoID INTEGER PRIMARY KEY,
        DataConcessao DATE NOT NULL,
        CNPJ TEXT NOT NULL,
        Capacidade INTEGER NOT NULL,
        FOREIGN KEY (CNPJ) REFERENCES Refinaria (CNPJ)
    );


    '''
    conn1.execute(create_table_query)

    # Insert into a table Produto in the database databaseRight.db with a sql query
    base_insert_query = '''
    INSERT INTO Autorizacao (AutoID, DataConcessao, CNPJ, Capacidade) VALUES
    '''    

    
    for row in df_base.itertuples():
        insert_query = base_insert_query + "({}, '{}', '{}', {})".format(row.AutoID, row.DataConcessao, row.CNPJ, row.Capacidade)
        conn1.execute(insert_query)


    df = pd.read_sql('SELECT * FROM Autorizacao', conn1)
    print(df)

def insertRegitroProcessamento(): 

    autorizacoes = pd.read_sql('SELECT * FROM Autorizacao', conn1)

    conn1.execute('DROP TABLE IF EXISTS Processamento;')

    query = '''

    SELECT *
        FROM ProcessamentoPetroleo
    '''

    df_base = pd.read_sql(query, conn)

    df_base.rename(columns={'Município': 'Municipio'}, inplace=True)
    df_base.rename(columns={'Região': 'Regiao'}, inplace=True)
    df_base.rename(columns={'Razão Social': 'RazaoSocial'}, inplace=True)
    df_base.rename(columns={'Capacidade Autorizada (m³/d)': 'Capacidade'}, inplace=True)
    df_base.rename(columns={'Mês/Ano': 'Data'}, inplace=True)
    df_base.rename(columns={'Volume Processado  (m³/d)': 'Volume'}, inplace=True)


    df_base['Data'] = pd.to_datetime(df_base['Data'], format='%m/%Y')
    df_base['Data'] = df_base['Data'].dt.strftime('%Y-%m-%d')

    autorizacoes['DataConcessao'] = pd.to_datetime(autorizacoes['DataConcessao'], format='%Y-%m-%d %H:%M:%S')
    autorizacoes['DataConcessao'] = autorizacoes['DataConcessao'].dt.strftime('%Y-%m-%d')

    # Ordering the authorization by Date  
    autorizacoes.sort_values(by=['DataConcessao'], inplace=True)

    # print(autorizacoes)
    # print(refinarias)

    def getAutorizacao(row):
        res = autorizacoes[(autorizacoes['CNPJ'] == str(row.CNPJ)) & (autorizacoes['DataConcessao'] <= str(row.Data))].max()['AutoID'] 
        return res

    df_base['AutoID'] = df_base.apply(getAutorizacao, axis=1)

    foreignKeyconstraint = 'PRAGMA foreign_keys = 0;'
    conn1.execute(foreignKeyconstraint)

    # Create a new table in the database databaseRight.db
    create_table_query = '''

    CREATE TABLE Processamento (
        CNPJ INTEGER NOT NULL,
        AutoID INTEGER NOT NULL,
        Data DATE NOT NULL,
        Volume INTEGER NOT NULL,
        PRIMARY KEY (CNPJ, AutoID, Data),
        FOREIGN KEY (CNPJ) REFERENCES Refinaria (CNPJ),
        FOREIGN KEY (AutoID) REFERENCES Autorizacao (AutoID)
    );

    '''
    conn1.execute(create_table_query)

    # Insert into a table Produto in the database databaseRight.db with a sql query
    base_insert_query = '''
    INSERT INTO Processamento (CNPJ, AutoID, Data, Volume) VALUES
    '''  

    print(df_base.columns)  

    for row in df_base.itertuples():
        insert_query = base_insert_query + "({}, {}, '{}', {})".format(row.CNPJ, row.AutoID, row.Data, row.Volume)
        conn1.execute(insert_query)


    df = pd.read_sql('SELECT * FROM Processamento', conn1)
    print(df)

def insertRegistroProducaoRefinaria():

    conn1.execute('DROP TABLE IF EXISTS Producao;')

    query = '''

    SELECT `CNPJ`, `Mês/Ano`, `Produto`, `Produção`
        FROM ProdutosDerivadosRefinaria
    '''

    df_base = pd.read_sql(query, conn)

    df_base.rename(columns={'Mês/Ano': 'Data'}, inplace=True)
    df_base.rename(columns={'Produção': 'Quantidade'}, inplace=True)

    df_base['Data'] = pd.to_datetime(df_base['Data'], format='%m/%Y')
    df_base['Data'] = df_base['Data'].dt.strftime('%Y-%m-%d')

    df_base['Produto'] = df_base['Produto'].str.slice_replace(start=-4, stop=-1, repl='').str.replace(')', '')


    # Linking the ProdID to the df_base
    produtos = pd.read_sql('SELECT * FROM Produto', conn1)

    def getProdutoID(row):
        res = produtos[produtos['Nome'] == str(row.Produto)]['ProdID'].values[0]
        return res

    df_base['ProdID'] = df_base.apply(getProdutoID, axis=1)

    # Linking the EmpID to the df_base
    refinarias = pd.read_sql('SELECT EmpID, CNPJ FROM Refinaria', conn1)

    def getEmpresaID(row):
        res = refinarias[refinarias['CNPJ'] == str(row.CNPJ)]['EmpID'].values[0]
        return res
    
    df_base['EmpID'] = df_base.apply(getEmpresaID, axis=1)



    foreignKeyconstraint = 'PRAGMA foreign_keys = 0;'
    conn1.execute(foreignKeyconstraint)

    # Create a new table in the database databaseRight.db
    create_table_query = '''

    CREATE TABLE Producao (
        EmpID INTEGER NOT NULL,
        ProdID INTEGER NOT NULL,
        Data DATE NOT NULL,
        Quantidade INTEGER NOT NULL,
        PRIMARY KEY (EmpID, ProdID, Data),
        FOREIGN KEY (EmpID) REFERENCES Empresa (EmpID),
        FOREIGN KEY (ProdID) REFERENCES Produto (ProdID)
    );

    '''
    conn1.execute(create_table_query)

    # Insert into a table Produto in the database databaseRight.db with a sql query
    base_insert_query = '''
    INSERT INTO Producao (EmpID, ProdID, Data, Quantidade) VALUES
    '''  

    for row in df_base.itertuples():
        insert_query = base_insert_query + "({}, {}, '{}', {})".format(row.EmpID, row.ProdID, row.Data, row.Quantidade)
        conn1.execute(insert_query)

    df = pd.read_sql('SELECT * FROM Producao', conn1)
    print(df)

def insertRegistroProducaoPetroquimica():

    query = '''

    SELECT `CNPJ`, `Mês/Ano`, `Produto`, `Produção`
        FROM ProducaoDerivadosCentralPetroquimica
    '''

    df_base = pd.read_sql(query, conn)

    df_base.rename(columns={'Mês/Ano': 'Data'}, inplace=True)
    df_base.rename(columns={'Produção': 'Quantidade'}, inplace=True)

    df_base['Data'] = pd.to_datetime(df_base['Data'], format='%m/%Y')
    df_base['Data'] = df_base['Data'].dt.strftime('%Y-%m-%d')

    maping = {
        'GASOLINA A COMUM': 'Gasolina A',
        'GASOLINA A PREMIUM': 'Gasolina A Premium',
        'GLP': 'GLP'
    }

    df_base['Produto'] = df_base['Produto'].map(maping)


    # Linking the ProdID to the df_base
    produtos = pd.read_sql('SELECT ProdID, Nome FROM Produto', conn1)


    mapping_ProdsID = { # BUGHERE
        'Gasolina A': produtos[produtos['Nome'] == 'Gasolina A ']['ProdID'].values[0],
        'Gasolina A Premium': produtos[produtos['Nome'] == 'Gasolina A Premium ']['ProdID'].values[0],
        'GLP': produtos[produtos['Nome'] == 'GLP ']['ProdID'].values[0]
    }


    df_base['ProdID'] = df_base['Produto'].map(mapping_ProdsID)

    # Linking the EmpID to the df_base
    petroquimicas = pd.read_sql('SELECT EmpID, CNPJ FROM CentralPetroquimica', conn1)

    def getEmpresaID(row):
        res = petroquimicas[petroquimicas['CNPJ'] == str(row.CNPJ)]['EmpID'].values[0]
        return res
    
    df_base['EmpID'] = df_base.apply(getEmpresaID, axis=1)

    foreignKeyconstraint = 'PRAGMA foreign_keys = 0;'
    conn1.execute(foreignKeyconstraint)

    # Insert into a table Produto in the database databaseRight.db with a sql query
    base_insert_query = '''
    INSERT OR IGNORE INTO Producao (EmpID, ProdID, Data, Quantidade) VALUES
    '''  

    for row in df_base.itertuples():
        insert_query = base_insert_query + "({}, {}, '{}', {})".format(row.EmpID, row.ProdID, row.Data, row.Quantidade)
        print(insert_query)
        conn1.execute(insert_query)

    df = pd.read_sql('SELECT * FROM Producao', conn1)
    print(df)

# def insertFileDerivadosOutrosProdutores():
    
#     # Inserindo na mao o produto 'OLEO DIESEL'
#     lastid = pd.read_sql('SELECT MAX(ProdID) FROM Produto', conn1).values[0][0]
#     conn1.execute('INSERT INTO Produto (ProdID, Nome, Unidade) VALUES ({}, "Oleo Diesel", "m³");'.format(lastid + 1))

#     query = '''

#     SELECT `ANO`, `MÊS`, `PRODUTOR`,`PRODUTO`, `PRODUÇÃO`
#         FROM ProducaoDerivadosOutrosProdutores
#         WHERE `PRODUÇÃO` != 0 
#     '''

#     df_base = pd.read_sql(query, conn)

#     # Concatenating the columns 'ANO' and 'MÊS' in a new column called 'Data'
#     df_base['Data'] = df_base['ANO'].astype(str) + '-' + df_base['MÊS'].astype(str) + '-01'

#     # Convertin the column 'Data' to datetime
#     df_base['Data'] = pd.to_datetime(df_base['Data'], format='%Y-%m-%d')


#     maping = {
#         'GASOLINA A': 'Gasolina A',
#         'SOLVENTE': 'Solventes',
#         'OLEO DIESEL': 'Oleo Diesel',
#     }

#     df_base['Produto'] = df_base['PRODUTO'].map(maping)
    

#     produtores = df_base['PRODUTOR'].unique()



# insertProdutos()
# insertRefinarias()
# insertPetroquimicas()
# insertAutorizacao()
# insertRegitroProcessamento()
# insertRegistroProducao()
# insertRegistroProducaoPetroquimica()
# insertFileDerivadosOutrosProdutores()

conn.close()

conn1.commit()
conn1.close()