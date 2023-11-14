import pandas as pd

# Producao_Gas_Combustivel_por_Refinaria

# Read the data
df = pd.read_csv('Producao_Gas_Combustivel_por_Refinaria.csv', sep=';', encoding='utf-8')

# Print the col names
# print(df.columns)

# Count the number of different Producers - Refinarias
# print("Refinarias: ", df['REFINARIA'].nunique())

# Count the number of different products
print(df.groupby(['PRODUTO']).size().reset_index(name='counts'))