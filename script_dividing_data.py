import pandas as pd
import sqlite3
connAux = sqlite3.connect('databaseAUX.db')
c = connAux.cursor()

conn = sqlite3.connect('database.db')
c1 = conn.cursor()

df : pd.DataFrame

def insertProdutos():
    conn.execute('DROP TABLE IF EXISTS Produto;')

    query = '''

    SELECT DISTINCT Produto
        FROM ProdutosDerivadosRefinaria

    '''

    df_base = pd.read_sql(query, connAux)

    # Add a index column
    df_base['ProdID'] = df_base.index

    # Transform string between parentheses in the column 'Produto' in a new column called 'Unidade'
    df_base['Unidade'] = df_base['Produto'].str.extract(r'\((.*?)\)', expand=False)

    # Remove the parentheses and the content between them
    df_base['Produto'] = df_base['Produto'].str.slice_replace(start=-4, stop=-1, repl='').str.replace(')', '')

    # Rename the column 'Produto' to 'Nome'
    df_base.rename(columns={'Produto': 'Nome'}, inplace=True)


    # Create a new table in the database databaseRight.db
    create_table_query = '''

    CREATE TABLE Produto (
        ProdID INTEGER PRIMARY KEY,
        Nome TEXT NOT NULL,
        Unidade TEXT NOT NULL
    );

    '''
    conn.execute(create_table_query)

    # Insert into a table Produto in the database databaseRight.db with a sql query
    base_insert_query = '''
    INSERT INTO Produto (ProdID, Nome, Unidade) VALUES 
    '''

    for row in df_base.itertuples():
        insert_query = base_insert_query + "({}, '{}', '{}')".format(row.ProdID, row.Nome, row.Unidade)
        # print(insert_query)
        conn.execute(insert_query)

def insertStates():

    conn.execute('DROP TABLE IF EXISTS Estado;')

    # EstadoID, Estado, Regiao

    query = '''

    SELECT DISTINCT `Estado`, `Região`
        FROM ProdutosDerivadosRefinaria

    '''

    df_base = pd.read_sql(query, connAux)

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
    conn.execute(create_table_query)

    # Insert into a table Produto in the database databaseRight.db with a sql query
    base_insert_query = '''
    INSERT INTO Estado (EstadoID, Estado, Regiao) VALUES 
    '''

    for row in df_base.itertuples():
        insert_query = base_insert_query + "({}, '{}', '{}')".format(row.EstadoID, row.Estado, row.Regiao)
        # print(insert_query)
        conn.execute(insert_query)


    df = pd.read_sql('SELECT * FROM Estado', conn)
    # print(df)

def insertRefinarias():

    # EstadoID, Estado, Regiao

    states = pd.read_sql('SELECT * FROM Estado', conn)

    conn.execute('DROP TABLE IF EXISTS Refinaria;')
    conn.execute('DROP TABLE IF EXISTS Empresa;')

    query = '''

    SELECT `CNPJ`, `Instalação`, `Razão Social`, `Município`, `Estado`, `Região`
        FROM ProdutosDerivadosRefinaria
        GROUP BY `CNPJ`
    '''

    df_base = pd.read_sql(query, connAux)

    # Add a index column
    df_base['EmpID'] = df_base.index

    # Rename the column 'Produto' to 'Nome'
    df_base.rename(columns={'Razão Social': 'RazaoSocial'}, inplace=True)
    df_base.rename(columns={'Instalação': 'Instalacao'}, inplace=True)
    df_base.rename(columns={'Município': 'Municipio'}, inplace=True)
    df_base.rename(columns={'Região': 'Regiao'}, inplace=True)


    foreignKeyconstraint = 'PRAGMA foreign_keys = 0;'
    conn.execute(foreignKeyconstraint)

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
    conn.execute(create_table_query)

    # Insert into a table Produto in the database databaseRight.db with a sql query
    base_insert_query = '''
    INSERT INTO Refinaria (EmpID, CNPJ, Instalacao, RazaoSocial, Municipio) VALUES
    '''

    for row in df_base.itertuples():
        insert_query = base_insert_query + "({}, '{}', '{}', '{}', '{}')".format(row.EmpID, row.CNPJ, row.Instalacao, row.RazaoSocial, row.Municipio) 
        # print(insert_query)
        conn.execute(insert_query)


    # Inserindo esses registros na tabela Empresa
    create_table_query = '''

    CREATE TABLE Empresa (
        EmpID INTEGER PRIMARY KEY,
        Nome TEXT NOT NULL,
        EstadoID INTEGER,
        ProdutorIndependente INTEGER NOT NULL,
        FOREIGN KEY (EstadoID) REFERENCES Estado (EstadoID)
    );

    '''

    conn.execute(create_table_query)
    

    base_insert_query = '''
    INSERT INTO Empresa (EmpID, Nome, EstadoID, ProdutorIndependente) VALUES
    '''

    for row in df_base.itertuples():
        insert_query = base_insert_query + "({}, '{}', {}, {})".format(row.EmpID, row.Instalacao, states[states['Estado'] == row.Estado]['EstadoID'].values[0], 0)
        # print(insert_query)
        conn.execute(insert_query)


    df = pd.read_sql('SELECT * FROM Refinaria', conn) 
    # print(df)

def insertPetroquimicas():

    states = pd.read_sql('SELECT * FROM Estado', conn)

    # EstadoID, Estado, Regiao

    c1.execute('SELECT MAX(EmpID) FROM Empresa')
    lastid = c1.fetchone()[0] 

    conn.execute('DROP TABLE IF EXISTS CentralPetroquimica;')

    query = '''

    SELECT `CNPJ`, `Município`, `Estado`, `Região`, `Razão Social`
        FROM ProducaoDerivadosCentralPetroquimica
        GROUP BY `CNPJ`
    '''

    df_base = pd.read_sql(query, connAux)

    # Add a index column
    df_base['EmpID'] = df_base.index

    # Map the EmpID adding the last id of the table Refinaria
    df_base['EmpID'] = df_base['EmpID'].map(lambda x: x + (lastid + 1))


    df_base.rename(columns={'Município': 'Municipio'}, inplace=True)
    df_base.rename(columns={'Região': 'Regiao'}, inplace=True)
    df_base.rename(columns={'Razão Social': 'RazaoSocial'}, inplace=True)


    foreignKeyconstraint = 'PRAGMA foreign_keys = 0;'
    conn.execute(foreignKeyconstraint)

    # Create a new table in the database databaseRight.db
    create_table_query = '''

    CREATE TABLE CentralPetroquimica (
        EmpID INTEGER PRIMARY KEY,
        CNPJ TEXT NOT NULL,
        Municipio TEXT NOT NULL,
        FOREIGN KEY (EmpID) REFERENCES Empresa (EmpID)
    );

    '''
    conn.execute(create_table_query)

    # Insert into a table Produto in the database databaseRight.db with a sql query
    base_insert_query = '''
    INSERT INTO CentralPetroquimica (EmpID, CNPJ, Municipio) VALUES
    '''    

    for row in df_base.itertuples():
        insert_query = base_insert_query + "({}, '{}', '{}')".format(row.EmpID, row.CNPJ, row.Municipio)
        conn.execute(insert_query)


    # Inserindo esses registros na tabela Empresa
    base_insert_query = '''
    INSERT INTO Empresa (EmpID, Nome, EstadoID, ProdutorIndependente) VALUES
    '''

    for row in df_base.itertuples():
        insert_query = base_insert_query + "({}, '{}', {}, {})".format(row.EmpID, row.RazaoSocial, states[states['Estado'] == row.Estado]['EstadoID'].values[0], 0)
        # print(insert_query)
        conn.execute(insert_query)





    # df = pd.read_sql('SELECT * FROM CentralPetroquimica', conn)
    # print(df)

def insertAutorizacao():

    conn.execute('DROP TABLE IF EXISTS Autorizacao;')

    query = '''

    SELECT `Mês/Ano`, `CNPJ`, `Capacidade Autorizada (m³/d)`
        FROM ProcessamentoPetroleo
    '''

    df_base = pd.read_sql(query, connAux)

    # Renaming Columns
    df_base.rename(columns={'Mês/Ano': 'DataConcessao'}, inplace=True)
    df_base.rename(columns={'Capacidade Autorizada (m³/d)': 'Capacidade'}, inplace=True)
    
    df_base['DataConcessao'] = pd.to_datetime(df_base['DataConcessao'], format='%m/%Y')

    # Group by 'CNPJ' and 'Capacidade Autorizada (m³/d)', and then aggregate using the first 'Mês/Ano'
    df_base = df_base.groupby(['CNPJ', 'Capacidade']).agg({'DataConcessao': 'first'}).reset_index()

    # Add a index column
    df_base['AutoID'] = df_base.index

    foreignKeyconstraint = 'PRAGMA foreign_keys = 0;'
    conn.execute(foreignKeyconstraint)

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
    conn.execute(create_table_query)

    # Insert into a table Produto in the database databaseRight.db with a sql query
    base_insert_query = '''
    INSERT INTO Autorizacao (AutoID, DataConcessao, CNPJ, Capacidade) VALUES
    '''    

    
    for row in df_base.itertuples():
        insert_query = base_insert_query + "({}, '{}', '{}', {})".format(row.AutoID, row.DataConcessao, row.CNPJ, row.Capacidade)
        conn.execute(insert_query)


    df = pd.read_sql('SELECT * FROM Autorizacao', conn)
    # print(df)

def insertRegitroProcessamento(): 

    autorizacoes = pd.read_sql('SELECT * FROM Autorizacao', conn)

    conn.execute('DROP TABLE IF EXISTS Processamento;')

    query = '''

    SELECT *
        FROM ProcessamentoPetroleo
    '''

    df_base = pd.read_sql(query, connAux)

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
    conn.execute(foreignKeyconstraint)

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
    conn.execute(create_table_query)

    # Insert into a table Produto in the database databaseRight.db with a sql query
    base_insert_query = '''
    INSERT INTO Processamento (CNPJ, AutoID, Data, Volume) VALUES
    '''  

    # print(df_base.columns)  

    for row in df_base.itertuples():
        insert_query = base_insert_query + "({}, {}, '{}', {})".format(row.CNPJ, row.AutoID, row.Data, row.Volume)
        conn.execute(insert_query)


    df = pd.read_sql('SELECT * FROM Processamento', conn)
    # print(df)

def insertRegistroProducaoRefinaria():

    conn.execute('DROP TABLE IF EXISTS Producao;')

    query = '''

    SELECT `CNPJ`, `Mês/Ano`, `Produto`, `Produção`
        FROM ProdutosDerivadosRefinaria
    '''

    df_base = pd.read_sql(query, connAux)

    df_base.rename(columns={'Mês/Ano': 'Data'}, inplace=True)
    df_base.rename(columns={'Produção': 'Quantidade'}, inplace=True)

    df_base['Data'] = pd.to_datetime(df_base['Data'], format='%m/%Y')
    df_base['Data'] = df_base['Data'].dt.strftime('%Y-%m-%d')

    df_base['Produto'] = df_base['Produto'].str.slice_replace(start=-4, stop=-1, repl='').str.replace(')', '')


    # Linking the ProdID to the df_base
    produtos = pd.read_sql('SELECT * FROM Produto', conn)

    def getProdutoID(row):
        res = produtos[produtos['Nome'] == str(row.Produto)]['ProdID'].values[0]
        return res

    df_base['ProdID'] = df_base.apply(getProdutoID, axis=1)

    # Linking the EmpID to the df_base
    refinarias = pd.read_sql('SELECT EmpID, CNPJ FROM Refinaria', conn)

    def getEmpresaID(row):
        res = refinarias[refinarias['CNPJ'] == str(row.CNPJ)]['EmpID'].values[0]
        return res
    
    df_base['EmpID'] = df_base.apply(getEmpresaID, axis=1)



    foreignKeyconstraint = 'PRAGMA foreign_keys = 0;'
    conn.execute(foreignKeyconstraint)

    # Create a new table in the database databaseRight.db
    create_table_query = '''

    CREATE TABLE Producao (
        EmpID INTEGER NOT NULL,
        ProdID INTEGER NOT NULL,
        Data DATE NOT NULL,
        Quantidade REAL NOT NULL,
        PRIMARY KEY (EmpID, ProdID, Data),
        FOREIGN KEY (EmpID) REFERENCES Empresa (EmpID),
        FOREIGN KEY (ProdID) REFERENCES Produto (ProdID)
    );

    '''
    conn.execute(create_table_query)

    # Insert into a table Produto in the database databaseRight.db with a sql query
    base_insert_query = '''
    INSERT INTO Producao (EmpID, ProdID, Data, Quantidade) VALUES
    '''  

    for row in df_base.itertuples():
        insert_query = base_insert_query + "({}, {}, '{}', {})".format(row.EmpID, row.ProdID, row.Data, row.Quantidade)
        conn.execute(insert_query)

    df = pd.read_sql('SELECT * FROM Producao', conn)
    # print(df)

def insertRegistroProducaoPetroquimica():

    query = '''

    SELECT `CNPJ`, `Mês/Ano`, `Produto`, `Produção`
        FROM ProducaoDerivadosCentralPetroquimica
    '''

    df_base = pd.read_sql(query, connAux)

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
    produtos = pd.read_sql('SELECT ProdID, Nome FROM Produto', conn)


    mapping_ProdsID = { 
        'Gasolina A': produtos[produtos['Nome'] == 'Gasolina A ']['ProdID'].values[0],
        'Gasolina A Premium': produtos[produtos['Nome'] == 'Gasolina A Premium ']['ProdID'].values[0],
        'GLP': produtos[produtos['Nome'] == 'GLP ']['ProdID'].values[0]
    }


    df_base['ProdID'] = df_base['Produto'].map(mapping_ProdsID)

    # Linking the EmpID to the df_base
    petroquimicas = pd.read_sql('SELECT EmpID, CNPJ FROM CentralPetroquimica', conn)

    def getEmpresaID(row):
        res = petroquimicas[petroquimicas['CNPJ'] == str(row.CNPJ)]['EmpID'].values[0]
        return res
    
    df_base['EmpID'] = df_base.apply(getEmpresaID, axis=1)

    foreignKeyconstraint = 'PRAGMA foreign_keys = 0;'
    conn.execute(foreignKeyconstraint)

    # Insert into a table Produto in the database databaseRight.db with a sql query
    base_insert_query = '''
    INSERT OR IGNORE INTO Producao (EmpID, ProdID, Data, Quantidade) VALUES
    '''  

    for row in df_base.itertuples():
        insert_query = base_insert_query + "({}, {}, '{}', {})".format(row.EmpID, row.ProdID, row.Data, row.Quantidade)
        # print(insert_query)
        conn.execute(insert_query)

    df = pd.read_sql('SELECT * FROM Producao', conn)
    # print(df)

def insertFileDerivadosOutrosProdutores():
    
    # Inserindo na mao o produto 'OLEO DIESEL'
    lastid = pd.read_sql('SELECT MAX(ProdID) FROM Produto', conn).values[0][0]
    conn.execute('INSERT INTO Produto (ProdID, Nome, Unidade) VALUES ({}, "Oleo Diesel", "m³");'.format(lastid + 1))

    query = '''

    SELECT `ANO`, `MÊS`, `PRODUTOR`,`PRODUTO`, `PRODUÇÃO`
        FROM ProducaoDerivadosOutrosProdutores
        WHERE `PRODUÇÃO` != 0 
    '''

    df_base = pd.read_sql(query, connAux)


    month_mapping = {
        'JAN': '01',
        'FEV': '02',
        'MAR': '03',
        'ABR': '04',
        'MAI': '05',
        'JUN': '06',
        'JUL': '07',
        'AGO': '08',
        'SET': '09',
        'OUT': '10',
        'NOV': '11',
        'DEZ': '12'
    }

    df_base['MÊS'] = df_base['MÊS'].map(month_mapping)
    df_base['Data'] = df_base['ANO'].astype(str) + '-' + df_base['MÊS'].astype(str) + '-01'
    df_base['Data'] = pd.to_datetime(df_base['Data'], format='%Y-%m-%d')

    produtos = pd.read_sql('SELECT ProdID, Nome FROM Produto', conn)

    prods_mapping = {
        'GASOLINA A': produtos[produtos['Nome'] == 'Gasolina A ']['ProdID'].values[0],
        'SOLVENTE': produtos[produtos['Nome'] == 'Solventes ']['ProdID'].values[0],
        'OLEO DIESEL': produtos[produtos['Nome'] == 'Oleo Diesel']['ProdID'].values[0],
    }

    df_base['ProdID'] = df_base['PRODUTO'].map(prods_mapping)


    produtores = df_base['PRODUTOR'].drop_duplicates().reset_index(drop=True).to_frame()

    c1.execute('SELECT MAX(EmpID) FROM Empresa')
    lastid = c1.fetchone()[0] 

    # Add a index column
    produtores['EmpID'] = produtores.index 
    produtores['EmpID'] = produtores['EmpID'].map(lambda x: x + (lastid + 1))

    # Inserindo esses registros na tabela Empresa
    base_insert_query = '''
    INSERT INTO Empresa (EmpID, Nome, ProdutorIndependente) VALUES
    '''

    for row in produtores.itertuples():
        insert_query = base_insert_query + "({}, '{}', {})".format(row.EmpID, row.PRODUTOR, 1)
        conn.execute(insert_query)

    df_base['EmpID'] = df_base['PRODUTOR'].map(produtores.set_index('PRODUTOR')['EmpID'])

    base_insert_query = '''
    
    INSERT INTO Producao (EmpID, ProdID, Data, Quantidade) VALUES
    '''

    # Renaming column 'PRODUÇÃO' to 'Quantidade'
    df_base.rename(columns={'PRODUÇÃO': 'Quantidade'}, inplace=True)

    for row in df_base.itertuples():
        insert_query = base_insert_query + "({}, {}, '{}', '{}')".format(row.EmpID, row.ProdID, row.Data, row.Quantidade)
        conn.execute(insert_query)

    df = pd.read_sql('SELECT * FROM Producao', conn)
    # print(df)

def insertingXisto():

    # Inserindo na mao o produto 'OLEO DIESEL'
    lastid = pd.read_sql('SELECT MAX(ProdID) FROM Produto', conn).values[0][0]
    conn.execute('INSERT INTO Produto (ProdID, Nome, Unidade) VALUES ({}, "Outros não Energéticos", "");'.format(lastid + 1))

    query = '''

    SELECT `ANO`, `MÊS`, `UNIDADE DA FEDERAÇÃO` as UF, PRODUTOR, PRODUTO, PRODUÇÃO as Quantidade
        FROM ProducaoDerivadosXisto
        WHERE `PRODUÇÃO` != 0 
    '''

    df_base = pd.read_sql(query, connAux)


    month_mapping = {
        'JAN': '01',
        'FEV': '02',
        'MAR': '03',
        'ABR': '04',
        'MAI': '05',
        'JUN': '06',
        'JUL': '07',
        'AGO': '08',
        'SET': '09',
        'OUT': '10',
        'NOV': '11',
        'DEZ': '12'
    }

    df_base['MÊS'] = df_base['MÊS'].map(month_mapping)
    df_base['Data'] = df_base['ANO'].astype(str) + '-' + df_base['MÊS'].astype(str) + '-01'
    df_base['Data'] = pd.to_datetime(df_base['Data'], format='%Y-%m-%d')

    produtos = pd.read_sql('SELECT ProdID, Nome FROM Produto', conn)

    prods_mapping = {
        'OUTROS NÃO ENERGÉTICOS': produtos[produtos['Nome'] == 'Outros não Energéticos']['ProdID'].values[0],
        'NAFTA': produtos[produtos['Nome'] == 'Nafta ']['ProdID'].values[0],
        'GLP' : produtos[produtos['Nome'] == 'GLP ']['ProdID'].values[0],
        'DIESEL': produtos[produtos['Nome'] == 'Oleo Diesel']['ProdID'].values[0],
        'ÓLEO COMBUSTÍVEL': produtos[produtos['Nome'] == 'Óleo Combustível ']['ProdID'].values[0],
    }

    df_base['ProdID'] = df_base['PRODUTO'].map(prods_mapping)


    c1.execute('SELECT MAX(EmpID) FROM Empresa')
    lastid = c1.fetchone()[0] 

    # Getting the id of the state of Paraná
    last_id_state = pd.read_sql('SELECT MAX(EstadoID) FROM Estado', conn).values[0][0]
    conn.execute('INSERT INTO Estado (EstadoID, Estado, Regiao) VALUES ({}, "Paraná", "SUL");'.format(last_id_state + 1))

   # Inserindo esses registros na tabela Empresa

    conn.execute('INSERT INTO Empresa (EmpID, EstadoID, Nome, ProdutorIndependente) VALUES ({}, {}, "SIX", 1);'.format(lastid + 1,last_id_state + 1))

    empID = lastid + 1

    base_insert_query = '''
    
    INSERT INTO Producao (EmpID, ProdID, Data, Quantidade) VALUES
    '''


    for row in df_base.itertuples():
        # print(empID, row.ProdID, row.Data, row.Quantidade)
        insert_query = base_insert_query + "({}, {}, '{}', '{}')".format(empID, row.ProdID, row.Data, row.Quantidade)
        conn.execute(insert_query)

    # df = pd.read_sql('SELECT * FROM Producao', conn)
    # print(df)

insertProdutos()
insertStates()
insertRefinarias()
insertPetroquimicas()
insertAutorizacao()
insertRegitroProcessamento()
insertRegistroProducaoRefinaria()
insertRegistroProducaoPetroquimica()
insertFileDerivadosOutrosProdutores()
insertingXisto()

connAux.close()

conn.commit()
conn.close()