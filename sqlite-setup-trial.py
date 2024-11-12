import sqlite3
conn = sqlite3.connect('expense-account.db')
cursor = conn.cursor()
conn.close()