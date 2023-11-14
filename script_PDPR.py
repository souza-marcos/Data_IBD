import pandas as pd

# Producao_Derivados_de_petroleo_por_refinaria

# Read the data
df = pd.read_csv('Producao_Derivados_de_petroleo_por_refinaria.csv', sep=',', encoding='utf-8')

# Print the col names
# print(df.columns)

# Count the number of different CNPJ
# print("CNPJ: ", df['CNPJ'].nunique())

# Count the number of different regions, states and municipalities
# print(df.groupby(['Região', 'Estado', 'Município']).size().reset_index(name='counts'))

# Count the number of different products
print(df.groupby(['Produto']).size().reset_index(name='counts'))