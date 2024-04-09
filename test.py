#Import libraries to read the data into a dataframe and to connect sqlite
import pandas as pd
import sqlite3
#Import Visualisation libraries for plots
import matplotlib.pyplot as plt
import seaborn as sns

#Reading the data into a dataframe using pandas 
df = pd.read_csv('/workspaces/Expense_account/expenses.csv', encoding='unicode_escape', header = 0)

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
df1 = df1.rename(columns = {" Amount " : "Amount"})
print(df1.columns)

#Update data types of columns
df1 = df1.astype({'Date':'str'})
conn.commit()
print(df1.head())

#Trials for data retrieval
cursor.execute('SELECT Date FROM Expenses WHERE Expense = "Transport"')
rows = cursor.fetchall()
for row in rows:
    print(row)

#Rename column " Amount " to "amount"
sql_rename_column = """
ALTER TABLE Expenses
RENAME COLUMN " Amount " TO amount;
"""
cursor.execute(sql_rename_column)
conn.commit()

#View column names
data=cursor.execute('''SELECT * FROM Expenses''')
for column in data.description: 
    print(column[0])

#VISUALISATION: Count plot for Expenses
plt.figure(figsize = (38,20))
plt.title('Expenses')
sns.countplot(x='Expense', data=df1)

#Function to return an output depending on the select statement from Expenses
def view_expenses_by_expense(user_input_expense):
    #connect to database
    conn = sqlite3.connect('expense-account.db')
    cursor = conn.cursor()

    #SQL statement and execute
    sql = '''SELECT * FROM Expenses WHERE Expense=?;'''
    cursor.execute(sql, (user_input_expense, ))
    rows = cursor.fetchall()

    #Command to print the columns and data in tabular form
    columns = [description[0] for description in cursor.description]
    print("\t".join(columns))
    #Printing rows in tabular form with "\t" which is the command for tabs
    for row in rows:
        print("\t".join(str(col) for col in row))

    return rows

#Validate user input
user_input_expense = input("Enter Expense: ")
if view_expenses_by_expense(user_input_expense):
    print("Database connected. Valid Input.")
else:
    print("Database error. Invalid input.")