import pandas as pd

# Processamento Petroleo

# Read the data
df = pd.read_csv('Processamento_Petroleo.csv', sep=',', encoding='utf-8')

# Print the col names
# print(df.columns)

# Count the number of different CNPJ
# print("CNPJ: ", df['CNPJ'].nunique())

# TABLE Localidade
# print(df[['Região', 'Estado', 'Município']].drop_duplicates())

# TABLE Empresa
# print(df[['CNPJ', 'Razão Social', 'Instalação']].drop_duplicates())

# TABLE Processamento
# print(df[['CNPJ', 'Mês/Ano', 'Capacidade Autorizada (m³/d)' ,'Volume Processado  (m³/d)' ,'FUE (%)']].drop_duplicates())

# Count the number of different regions, states and municipalities
# print(df.groupby(['Região', 'Estado', 'Município']).size().reset_index(name='counts')['counts'].sum()) // Error
# print(df.groupby(['Região', 'Estado', 'Município']).size().reset_index(name='counts'))


# Podemos usar o método to_sql
