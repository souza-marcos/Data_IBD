import pandas as pd

# Producao_Derivados_Xisto

# Read the data
df = pd.read_csv('Producao_Derivados_Xisto.csv', sep=';', encoding='utf-8')

# Print the col names
# print(df.columns)

# Count the number of different Producers
# print("PRODUTORES: ", df['PRODUTOR'].nunique())

# Count the number of different products
print(df.groupby(['PRODUTO']).size().reset_index(name='counts'))