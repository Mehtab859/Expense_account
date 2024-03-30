#Import libraries to read the data into a dataframe and to connect sqlite
import pandas as pd
import sqlite3

#Reading the data into a dataframe using pandas 
df = pd.read_csv('/workspaces/Mehtab859/excel-import-test/expenses.csv', encoding='unicode_escape', header = 0)

#Cleaning the data after excel import. Removed missing value columns and table 2 that is not needed at this moment. Storing it in a new Dataframe (df1)
df1 = df.drop(['Unnamed: 4', 'Unnamed: 5', 'Unnamed: 6', 'Unnamed: 7', 'Merchant', 'Id.1', 'Category'], axis = 1)
#Printing the new dataframe
print(df1.head())

#Creating the expense account database by connecting to sqlite3
conn = sqlite3.connect('expense-account.db')

#Convert the dataframe to sql to be stored in the database
df1.to_sql(
    name = 'Expenses',
    con = conn,
    if_exists = 'replace',
    index = False,
    dtype = {'Expense': 'TEXT',
             'Date': 'REAL',
             'Amount': 'REAL',
             'id': 'INT PRIMARY KEY'}
)

#Initialising the cursor
cursor = conn.cursor()

#Update and clean column names
df1 = df1.rename(columns = {' Amount ':'amount'})
print(df1.columns)

#Update data types of columns
df1 = df1.astype({'Date':'str'})
conn.commit()
print(df1.head())

#Trials  data retrieval
cursor.execute('SELECT Date FROM Expenses WHERE Expense = "Transport"')
rows = cursor.fetchall()
for row in rows:
    print(row)
