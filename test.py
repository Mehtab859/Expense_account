#Import libraries to read the data into a dataframe and to connect sqlite
import pandas as pd
import numpy as np
import sqlite3
#Import Visualisation libraries for plots
import matplotlib.pyplot as plt
import seaborn as sns

#Reading the data into a dataframe using pandas 
df = pd.read_csv('expenses.csv', encoding =
'unicode_escape', header = 0)
#Cleaning the data after excel import. Removed missing value columns and table 2 that is not needed at this moment. Storing it in a new Dataframe (df1)
df1 = df.drop(['Unnamed: 4', 'Unnamed: 5', 'Unnamed: 6', 
'Unnamed: 7', 'Merchant', 'Id.1', 'Category'], axis = 1)
#Printing the new dataframe
#print(df1.head())

#Creating the expense account database by connecting to sqlite3
conn = sqlite3.connect('expense-account.db')
#Convert the dataframe to sql to be stored in the database
df1.to_sql(
            name = 'Expenses',
            con = conn,
            if_exists = 'replace',
            index = False,
            dtype = {
                    'Expense': 'TEXT',
                    'Date': 'REAL',
                    'Amount': 'REAL',
                    'id': 'INT PRIMARY KEY'}
)

#Read new data into a dataframe and the sql databse (Merchants table)
df2 = pd.read_csv('Merchants.csv', encoding = 'unicode_escape', header = 0)
#print(df2.columns)
#print(df2.head())
df2.to_sql(
            name = 'Merchants',
            con = conn,
            if_exists = 'replace',
            index = False,
            dtype = {
                    'Merchant':'TEXT',
                    'Category':'TEXT',
                    'id_no':'INT PRIMARY KEY'}
)

#Initialising the cursor
cursor = conn.cursor()

#Trials for data retrieval df1
cursor.execute('SELECT Expense, Date FROM Expenses WHERE Expense = "Transport"')
rows = cursor.fetchall()
#for row in rows:
    #print(row)

#Rename column " Amount " to "amount"
sql_rename_column = """
ALTER TABLE Expenses
RENAME COLUMN " Amount " TO amount;
"""
cursor.execute(sql_rename_column)
#Commit changes to rename the amount column
conn.commit()

#View column names Expenses table
data=cursor.execute('''SELECT * FROM Expenses''')
#for column in data.description: 
    #print(column[0])
#View columns names Merchants table
data2 = cursor.execute('''SELECT * FROM Merchants''')
#for column in data.description: 
    #print(column[0])

#Clean amount column
sql_update_amount = '''UPDATE Expenses SET amount = REPLACE(amount, '£', ' ');'''
cursor.execute(sql_update_amount)
#Commit changes
conn.commit()
#Close connection to db
conn.close()

#Function to return an output depending on the select statement from Expenses
def view_expenses_by_expense(user_input_expense, user_input_view_plot):
 #connect to database
 conn = sqlite3.connect('expense-account.db')
 #Initialise cursor
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
 #SQL query to retrieve the total amount spent on expense based on user input
 total_sql = '''SELECT SUM(amount) FROM Expenses WHERE Expense = ?;'''
 cursor.execute(total_sql, (user_input_expense, ))
 vals = cursor.fetchone()[0]
 #Print total amount 
 print("Total spent on expense: ", vals)
 #Input Validation
 if user_input_expense in [row[0] for row in rows]: 
    print('Valid input')
 else:
    print('Invalid input')
 #VISUALISATION: Count plot for Expenses
 plt.figure(figsize = (100,15))
 plt.title('Expenses')
 sns.countplot(x = user_input_view_plot, data=df1)
 plt.show()
 
 #Close connection to db 
 conn.close()

#Function for output from Merchants table
def view_merchants(user_input_merchants, user_input_view_merchants):
 #connect to db
 conn = sqlite3.connect('expense-account.db')
 #initialise cursor to fetch data from db
 cursor = conn.cursor()
 #SQL query
 sql = '''SELECT * FROM Merchants WHERE Merchant = ?'''
 cursor.execute(sql, (user_input_merchants, ))
 rows = cursor.fetchall()
 #Printing columns in output so the data looks tabular
 columns = [description[0] for description in cursor.description]
 print("\t".join(columns))
 #Tabular format for data rows
 for row in rows:
    print("\t".join(str(col) for col in row))
 #Input Validation
 if user_input_merchants in [row[0] for row in rows]:
    print('Valid input')
 else:
    print('Invalid input')
 #VISUALIZATION: Count plot for Merchants 
 plt.figure(figsize = (15,30))
 plt.title('Merchants')
 sns.countplot(x = user_input_view_merchants , data = df2)
 plt.show()
 #CLose connection to db
 conn.close()

#Data retrieval joining Merchants and Expenses tables
def joined_output(joined_output_user_input):
 #connect to db
 conn = sqlite3.connect('expense-account.db')
 #initialise cursor object
 cursor = conn.cursor()
 #SQL command to retrieve Expense name, ID, Amount and Merchant name by joining 2 tables. Order output by Date.
 sql = '''SELECT Expenses.Expense, Expenses.Id, Expenses.Date, Expenses.amount, Merchants.Merchant, Merchants.Category 
 FROM Expenses 
 LEFT JOIN Merchants ON Expenses.Id = Merchants.id_no
 WHERE Expenses.Expense = ? ORDER BY Date;'''
 cursor.execute(sql, (joined_output_user_input, ))
 rows = cursor.fetchall()
 #Printing output in a tabular form
 columns = [description[0] for description in cursor.description]
 print("\t".join(columns))
 for row in rows:
    print("\t".join(str(col) for col in row))
 #Validate user input
 if joined_output_user_input in [row[0] for row in rows]:
    print("Valid entry")
 else:
    print('Invalid entry')
 #Close connection to db
 conn.close()

#Function to add an entry into the Expenses table
def add_entry_expenses(expense, date, amount, id):
 #Connect to the database
 conn = sqlite3.connect('expense-account.db')
 #cursor object to retrieve data
 cursor = conn.cursor()
 #sql command to insert new data into the Expenses table
 sql = '''INSERT INTO Expenses (Expense, Date, amount, Id) VALUES (?,?,?,?);'''
 cursor.execute(sql, (expense, date, amount, id))
 #Commit changes of new data insertion
 conn.commit()
 #Printing the last Id in the db
 sql_rows = '''SELECT COUNT(Id) FROM Expenses;'''
 cursor.execute(sql_rows)
 row_count = cursor.fetchone()[0]
 #Print number of rows after the user inputs fresh data
 print("Number of rows after data entry: ", row_count)
 #Print newly added data
 print("Newly inserted data: ")
 cursor.execute('''SELECT * FROM Expenses WHERE Id = ?''', (cursor.lastrowid, ))
 row = cursor.fetchone()
 print(row)
 #Close connection to db
 conn.close()


#Function to add an entry into the Merchants table
def add_entry_merchants(merchant, id_no, category):
 #Connect to sqlit3 database
 conn = sqlite3.connect('expense-account.db')
 #cursor object to retrive data from database
 cursor = conn.cursor()
 #sql command to insert data into the Merchants table
 sql = '''INSERT INTO Merchants (Merchant, id_no, Category) VALUES (?,?,?);'''
 cursor.execute(sql, (merchant, id_no, category))
 #Commit updates
 conn.commit()
 #Outputs how many rows there are in the table after entry
 sql_rows = '''SELECT COUNT(id_no) FROM Merchants;'''
 cursor.execute(sql_rows)
 row_count = cursor.fetchone()[0]
 print("Number of rows after data entry: ", row_count)
 #Show the user their newly added data in the db
 print("Newly inserted data: ")
 #SQL query that pulls corresponding information relating to the last id (newly added entry) from the db
 cursor.execute('''SELECT * FROM Merchants WHERE id_no = ?''', (cursor.lastrowid, ))
 row = cursor.fetchone()
 print(row)
 #close connection
 conn.close()

#Expense Account menu
print("Expense tracker menu")
print("Type 1 to insert values into expense table")
print("Type 2 to insert values into the Merchant table")
print("Type 3 to view Expenses")
print("Type 4 to view Merchants")
print("Type 5 to view joined result from Expenses and Merchants")

choice = int(input("Type your entry: "))

#Choice 1
if choice == 1:
 #User input for expense
 expense = str(input("Add your expense: "))
 #User input Validation
 if expense.isdigit():
    #Raise ValueError in case of invalid entry type
    raise ValueError("Invalid entry, must be string")
 #User input for date
 date = str(input("Enter the date of your expense: "))
 #Input Validation as numbers can pass for strings
 if date.isalnum() or date.isalpha():
    #Raise ValueError in case of invalid entry type
    raise ValueError("Invalid entry. Must be date in dd/mm/yyyy")
 #User input for amount. No validation as it has been specified for float entry.
 amount = float(input("Enter the amount: "))
 #Connect the database
 conn = sqlite3.connect('expense-account.db')
 cursor = conn.cursor()
 #Retrieve the count of ID from Expenses so the user knows what valid ID can be used for new entry
 sql_rows = '''SELECT COUNT(Id) FROM Expenses;'''
 cursor.execute(sql_rows)
 row_count = cursor.fetchone()[0]
 print("Last Id number is: ", row_count)
 #Close connection
 conn.close()
 #User input for valid ID
 id = int(input("Enter a valid id for your expense: "))
 #Connect to db to check for existing ID
 conn = sqlite3.connect('expense-account.db')
 cursor = conn.cursor()
 #SQL query to check for existing ID
 cursor.execute('''SELECT Id FROM Expenses WHERE Id = ?;''', (id, ))
 id_valid = cursor.fetchone()
 #If ID already exists in the database raise a ValueError
 if id_valid:
    raise ValueError("ID already exists, enter a valid Id. Restart the program.")
 #Close connection to db
 conn.close()
 #Execute the add_entry_expenses function
 add_entry_expenses(expense, date, amount, id)
 #Confirm with the user that the new data has been added successfully
 print("Expense added successfully")

#Choice 2
elif choice == 2:
 merchant = str(input("Enter Merchant name: "))
 #User input validation for data type
 if merchant.isdigit():
    #Raise ValueError if entry is not of string
    raise ValueError("Invalid entry, must be string")
 #Connect to db
 conn = sqlite3.connect('expense-account.db')
 cursor = conn.cursor()
 #SQL query to output the last ID from Merchants table
 sql_rows = '''SELECT COUNT(id_no) FROM Merchants;'''
 cursor.execute(sql_rows)
 row_count = cursor.fetchone()[0]
 #Print last ID number so the user knows what valid ID number would be available
 print("Last Id number is: ", row_count)
 #Close connection to db
 conn.close()
 #User input for ID
 id_no = int(input('Enter a valid id number for new data entry: '))
 #Connect to db to check for existing ID
 conn = sqlite3.connect('expense-account.db')
 cursor = conn.cursor()
 #SQL query to check for existing ID
 cursor.execute('''SELECT id_no FROM Merchants WHERE id_no = ?;''', (id_no, ))
 id_valid = cursor.fetchone()
 #If ID already exists in the database raise a ValueError
 if id_valid:
    raise ValueError("ID already exists, enter a valid Id. Restart the program.")
 #Close connection to db
 conn.close()
 #User input for Category
 category = str(input('Enter Category: '))
 #User input validation for data type of category
 if category.isdigit():
    raise ValueError("Invalid entry, must be string")
 #Call the add_entry_merchants function
 add_entry_merchants(merchant, id_no, category)
 #Print success message for new Merchant entry
 print("Merchant entry added successfully")

#Choice 3
elif choice == 3:
 #user input for Expense
 user_input_expense = input("Input is case sensitive, begin with a capital letter. Enter Expense: ")
 user_input_view_plot = input("Input is case sensitive, begin with a capital letter. What column would you like to plot? ")
 #Call the function
 view_expenses_by_expense(user_input_expense, user_input_view_plot)
 
#Choice 4
elif choice == 4:
 #User input for Merchants
 user_input_merchants = input("Input is case sensitive, begin with a capital letter. Enter Merchant name: ")
 #User input for Visualization Merchants table
 user_input_view_merchants = input('Input is case sensitive, begin with a capital letter. What column would you like to plot from the Merchants table? ')
 #Call view merchant function
 view_merchants(user_input_merchants, user_input_view_merchants)

#Choice 5
elif choice == 5:
 #User input for joined output for corresponding data from both tables
 joined_output_user_input = input('Enter an Expense for joined output: ')
 #Call joined output function
 joined_output(joined_output_user_input)

else:
 print('Exit')