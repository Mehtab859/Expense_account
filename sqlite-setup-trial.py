import sqlite3

conn = sqlite3.connect('expense-account.db')
cursor = conn.cursor()

sql_rename_column = """
ALTER TABLE Expenses
RENAME COLUMN " Amount " TO amount;
"""
cursor.execute(sql_rename_column)
conn.commit()

data=cursor.execute('''SELECT * FROM Expenses''')
for column in data.description: 
    print(column[0])