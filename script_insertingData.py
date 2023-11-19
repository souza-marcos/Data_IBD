import pandas as pd


# Connection with the database sqlite3
import sqlite3
conn = sqlite3.connect('databaseAUX.db')
c = conn.cursor()

df = pd.read_csv('Producao_Derivados_de_petroleo_por_refinaria.csv')
df.to_sql('ProdutosDerivadosRefinaria', conn, if_exists='replace', index=False)

df = pd.read_csv('Processamento_Petroleo.csv')
df.to_sql('ProcessamentoPetroleo', conn, if_exists='replace', index=False)

df = pd.read_csv('Producao_Derivados_Central_Petroquimica.csv')
df.to_sql('ProducaoDerivadosCentralPetroquimica', conn, if_exists='replace', index= False)

df = pd.read_csv('Producao_Derivados_Outros_Produtores.csv', sep=';')
df.to_sql('ProducaoDerivadosOutrosProdutores', conn, if_exists='replace', index= False)

df = pd.read_csv('Producao_Derivados_Xisto.csv', sep=';')
df.to_sql('ProducaoDerivadosXisto', conn, if_exists='replace', index= False)

df = pd.read_csv('Producao_Gas_Combustivel_por_Refinaria.csv', sep=';')
df.to_sql('ProducaoGasCombustivelporRefinaria', conn, if_exists='replace', index= False)

conn.commit()
conn.close()