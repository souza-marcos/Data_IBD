import pandas as pd
# Connection with the database sqlite3
import sqlite3
conn1 = sqlite3.connect('databaseRight.db')

query = ''' 
SELECT * FROM Produto
'''

df = pd.read_sql(query, conn1)
print(df)


conn1.close()
