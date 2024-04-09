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

#Read new data into a dataframe and the sql databse (Merchants table)
df2 = pd.read_csv('/workspaces/Expense_account/Merchants.csv', encoding = 'unicode_escape', header = 0)
print(df2.columns)
print(df2.head())

#Initialising the cursor
cursor = conn.cursor()

#Update and clean column names
df1 = df1.rename(columns = {" Amount " : "Amount"})
print(df1.columns)

#Update data types of columns
df1 = df1.astype({'Date':'str'})
conn.commit()
print(df1.head())

#Trials for data retrieval df1
cursor.execute('SELECT Date FROM Expenses WHERE Expense = "Transport"')
rows = cursor.fetchall()
for row in rows:
    print(row)

#Trials for data retrieval df2
sql = '''SELECT DISTINCT Category FROM Merchants;'''
cursor.execute(sql)
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

#Clean amount column
sql_update_amount = '''UPDATE Expenses SET amount = REPLACE(amount, 'ï¿½', '');'''
cursor.execute(sql_update_amount)
conn.commit()

#View column names Expenses table
data=cursor.execute('''SELECT * FROM Expenses''')
for column in data.description: 
    print(column[0])

#View columns names Merchants table
data2=cursor.execute('''SELECT * FROM Merchants''')
for column in data.description: 
    print(column[0])

#Trial for data retrieval joining Merchants and Expenses tables
sql = '''SELECT Expenses.Expense, Expenses.Id, Expenses.amount, Merchants.Merchant 
FROM Expenses 
LEFT JOIN Merchants ON Expenses.Id = Merchants.Id
WHERE Expenses.Expense = 'Netflix';'''
cursor.execute(sql)
rows = cursor.fetchall()
for row in rows:
    print(rows)

#VISUALISATION: Count plot for Expenses
plt.figure(figsize = (38,20))
plt.title('Expenses')
sns.countplot(x= 'Expense', data=df1)

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
    
    #VISUALISATION: Count plot for Expenses
    plt.figure(figsize = (38,20))
    plt.title('Expenses')
    sns.countplot(x= 'Expense', data=df1)

    return rows

#Validate user input
user_input_expense = input("Enter Expense: ")
if view_expenses_by_expense(user_input_expense):
    print("Database connected. Valid Input.")
else:
    print("Database error. Invalid input.")

#Function to add an entry into the Expenses table
def add_entry_expenses(expense, date, amount, id):
    #Connect to the database
    conn = sqlite3.connect('expense-account.db')
    #cursor object to retrieve data
    cursor = conn.cursor()
    #sql command to insert new data into the Expenses table
    sql = '''INSERT INTO Expenses (expense, date, amount, id) VALUES (?,?,?,?,)''',(expense, date, amount, id)
    cursor.execute(sql)
    #commit changes
    conn.commit()
    #close connection
    conn.close()

#Function to add an entry into the Merchants table
def add_entry_merchants(merchant, id, category):
    #Connect to sqlit3 database
    conn = sqlite3.connect('expense-account.db')
    #cursor object to retrive data from database
    cursor = conn.cursor()
    #sql command to insert data into the Merchants table
    sql = '''INSERT INTO Merchants (merchant, id, category) VALUES (?,?,?,)''',(merchant, id, category)
    cursor.execute(sql)
    #commit changes
    conn.commit()
    #close connection
    conn.close()
    
#Function for Expense Account menu
def menu():
    print("Expense tracker menu")
    print("Type 1 to insert values into expense table")
    print("Type 2 to insert values into the Merchant table")
    print("Type 3 to delete items from the Expenses table")
    print("Type 4 to delete items from the Merchant table")
    print("Type 5 to view Expenses")
    print("Type 6 to view Merchants")

#while True:
    #menu()
    #choice = input("Type your entery: ")
    #if choice == 1:
        #expense = input("Add your expense: ")
        #date = input("Enter the date of your expense: ")
        #amount = float(input("Enter the amount: "))
        #id = int(input("Enter a valid id for your expense: "))
        #add_entry(expense, date, amount, id)
        #print("Expense added successfully")
        #if ValueError:
            #print("Value error, type a valid input")
    
    