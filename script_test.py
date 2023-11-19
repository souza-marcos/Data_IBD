import pandas as pd
# Connection with the database sqlite3
import sqlite3
conn1 = sqlite3.connect('databaseRight.db')
conn = sqlite3.connect('database.db')

# query = '''

#     SELECT `Mês/Ano`, `CNPJ`, `Capacidade Autorizada (m³/d)`
#         FROM ProcessamentoPetroleo
#     '''

# df_base = pd.read_sql(query, conn)

# df_base['Mês/Ano'] = pd.to_datetime(df_base['Mês/Ano'], format='%m/%Y')

# # Group by 'CNPJ' and 'Capacidade Autorizada (m³/d)', and then aggregate using the first 'Mês/Ano'
# result_df = df_base.groupby(['CNPJ', 'Capacidade Autorizada (m³/d)']).agg({'Mês/Ano': 'first'})

result_df = pd.read_sql('SELECT COUNT(*) FROM Processamento ', conn1)

print(result_df)

conn1.close()
