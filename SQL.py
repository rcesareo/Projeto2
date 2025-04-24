# -*- coding: utf-8 -*-

import adodbapi
import pyodbc 

pyodbc.connect('Trusted_Connection=yes',
               driver='{SQL Server}', server='[system_name]',
               database='[databasename]')

pyodbc.connect('Trusted_Connection=yes', uid='me',
               driver='{SQL Server}', server='localhost',
               database='[databasename]')

pyodbc.connect('Trusted_Connection=yes',
               driver='{SQL Server}', server='localhost',
               uid='me', pwd='[windows_pass]', database='[database_name]')

pyodbc.connect('Trusted_Connection=yes',
               driver='{SQL Server}', server='localhost',
               database='[server_name]\\[database_name]')

pyodbc.connect('Trusted_Connection=yes',
               driver='{SQL Server}', server='localhost',
               database='[server_name]\[database_name]')

pyodbc.connect('Trusted_Connection=yes',
               driver='{SQL Server}',
               database='[server_name]\[database_name]')

cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                      "Server=SQLRISCORJ01;"
                      "Database=Mercado_Financeiro_Fundo;"
                      "Trusted_Connection=yes;")

                                    
cursor = cnxn.cursor()
cursor.execute('SELECT * FROM Mercado_Financeiro_Fundos')

for row in cursor:
    print('row = %r' % (row,))
    
# DRIVER

conn = pyodbc.connect("DRIVER={{SQL Server}};SERVER={0}; database={1}; \
       trusted_connection=yes;UID={2};PWD={3}".format("SQLRISCORJ01","Mercado_financeiro_fundo","usuario = format.Fxxxxxxx","senha = format.XXXXXXXX"))
cursor = conn.cursor()

import pandas as pd
import pyodbc
from fast_to_sql import fast_to_sql as fts

# Test Dataframe for insertion
df = pd.DataFrame(your_dataframe_here)

# Create a pyodbc connection
conn = pyodbc.connect(
    Driver={ODBC Driver 17 for SQL Server};
    Server=localhost;
    Database=my_database;
    UID=my_user;
    PWD=my_pass;
)

# If a table is created, the generated sql is returned
create_statement = fts.fast_to_sql(df, "my_great_table", conn, if_exists="replace")

# Commit upload actions and close connection
conn.commit()
conn.close()



import pyodbc

# if you have user id and password then try with this connection string
connection_string = f"DRIVER={SQL Server};SERVER={server_name};UID={user_id};PWD={password}"

# if using in the local system then use the following connection string
connection_string = f"DRIVER={SQL Server};SERVER={server_name}; Trusted_Connection=True;"

connection= pyodbc.connect(connection_string)
cursor = connection.cursor()



sql_create_database = f"CREATE DATABASE {database_name}"
cursor.execute(sql_create_database)

set_database = f"USE {database_name}"
cursor.execute(set_database)

set_database = f"SELECT {database_name}"
cursor.execute(select_database)

set_database = f"UPDATE {database_name}"
cursor.execute(update_database)

set_database = f"DELETE {database_name}"
cursor.execute(delete_database)


# define execution engine using sqlalchemy create_engine() 
# method with your db params

engine = sqlalchemy.create_engine('ibm_db_sa://{user}:{pwd}@{host}:{port}/{db};SECURITY=SSL'.format(
    user=params['username'],
    pwd=params['password'],
    host=params['hostname'],
    port=params['port'],
    db=params['database']
))
db_table = pd.read_sql('SELECT * FROM TEST_SCHEMA.dummy_test', engine)

connection= pyodbc.connect(connection_string)
cursor = connection.cursor()

sql_create_database = f"CREATE DATABASE {database_name}"
cursor.execute(sql_create_database)

set_database = f"USE {database_name}"
cursor.execute(set_database)

set_database = f"SELECT {database_name}"
cursor.execute(select_database)

set_database = f"UPDATE {database_name}"
cursor.execute(update_database)

set_database = f"DELETE {database_name}"
cursor.execute(delete_database)

dbb = MySQLdb.connect(host="localhost", 
       user="user", 
       passwd="pass", 
       db="database") 

try:
   curb = dbb.cursor()
   curb.execute ("UPDATE RadioGroups SET CurrentState=1 WHERE RadioID=11")
   print "Row(s) were updated :" +  str(curb.rowcount)
   curb.close()
except MySQLdb.Error, e:
   print "query failed<br/>"
   print e  
      
   test (x,y)
   
   implenet_test (X_x);
mmmmmmm   
   split_test_X = (implement_test (X,x));
   split_test_Y = (implement_test (Y,y));
   
cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                      "Server=SQLRISCORJ01;"
                      "Database=Mercado_Financeiro_Fundo;"
                      "Trusted_Connection=yes;")

                                    
cursor = cnxn.cursor()
cursor.execute('SELECT * FROM Mercado_Financeiro_Fundos')

for row in cursor:
    print('row = %r' % (row,))   
    
   # Test Dataframe for insertion
   df = pd.DataFrame(your_dataframe_here)

   # Create a pyodbc connection
   conn = pyodbc.connect(
       Driver={ODBC Driver 17 for SQL Server};
       Server=localhost;
       Database=my_database;
       UID=my_user;
       PWD=my_pass;
   )

   # If a table is created, the generated sql is returned
   create_statement = fts.fast_to_sql(df, "my_great_table", conn, if_exists="replace")

   # Commit upload actions and close connection
   conn.commit()
   conn.close()
   for row in cursor:
       print('row = %r' % (row,))   
       
      # Test Dataframe for insertion
      df = pd.DataFrame(your_dataframe_here)

      # Create a pyodbc connection
      conn = pyodbc.connect(
          Driver={ODBC Driver 17 for SQL Server};
          Server=localhost;
          Database=my_database;
          UID=my_user;
          PWD=my_pass;
      )


      # If a table is created, the generated sql is returned
      create_statement = fts.fast_to_sql(df, "my_great_table", conn, if_exists="replace")

      # Commit upload actions and close connection
      conn.commit()
      conn.close()
       
   dbb = MySQLdb.connect(host="localhost", 
          user="user", 
          passwd="pass", 
          db="database") 

   try:
      curb = dbb.cursor()
      curb.execute ("UPDATE RadioGroups SET CurrentState=1 WHERE RadioID=11")
      print "Row(s) were updated :" +  str(curb.rowcount)
      curb.close()
   except MySQLdb.Error, e:
      print "query failed<br/>"
      print e  
         
      test (x,y)
      
      implenet_test (X_x);
   mmmmmmm   
      split_test_X = (implement_test (X,x));
      split_test_Y = (implement_test (Y,y));
      
   cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                         "Server=SQLRISCORJ01;"
                         "Database=Mercado_Financeiro_Fundo;"
                         "Trusted_Connection=yes;")

                                       
   cursor = cnxn.cursor()
   cursor.execute('SELECT * FROM Mercado_Financeiro_Fundos')

   for row in cursor:
       print('row = %r' % (row,))   
       
      # Test Dataframe for insertion
      df = pd.DataFrame(your_dataframe_here)

      # Create a pyodbc connection
      conn = pyodbc.connect(
          Driver={ODBC Driver 17 for SQL Server};
          Server=localhost;
          Database=my_database;
          UID=my_user;
          PWD=my_pass;
      )

      # If a table is created, the generated sql is returned
      create_statement = fts.fast_to_sql(df, "my_great_table", conn, if_exists="replace")

      # Commit upload actions and close connection
      conn.commit()
      conn.close()
       
      for row in cursor:
          print('row = %r' % (row,))   
          
         # Test Dataframe for insertion
         df = pd.DataFrame(your_dataframe_here)

         # Create a pyodbc connection
         conn = pyodbc.connect(
             Driver={ODBC Driver 17 for SQL Server};
             Server=localhost;
             Database=my_database;
             UID=my_user;
             PWD=my_pass;
         )


         # If a table is created, the generated sql is returned
         create_statement = fts.fast_to_sql(df, "my_great_table", conn, if_exists="replace")

         # Commit upload actions and close connection
         conn.commit()
         conn.close()
          
pyodbc.connect('Trusted_Connection=yes',
               driver='{SQL Server}', server='[system_name]',
               database='[databasename]')

pyodbc.connect('Trusted_Connection=yes', uid='me',
               driver='{SQL Server}', server='localhost',
               database='[databasename]')

pyodbc.connect('Trusted_Connection=yes',
               driver='{SQL Server}', server='localhost',
               uid='me', pwd='[windows_pass]', database='[database_name]')

pyodbc.connect('Trusted_Connection=yes',
               driver='{SQL Server}', server='localhost',
               database='[server_name]\\[database_name]')

pyodbc.connect('Trusted_Connection=yes',
               driver='{SQL Server}', server='localhost',
               database='[server_name]\[database_name]')

pyodbc.connect('Trusted_Connection=yes',
               driver='{SQL Server}',
               database='[server_name]\[database_name]')

cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                      "Server=SQLRISCORJ01;"
                      "Database=Mercado_Financeiro_Fundo;"
                      "Trusted_Connection=yes;")

                                    
cursor = cnxn.cursor()
cursor.execute('SELECT * FROM Mercado_Financeiro_Fundos')

for row in cursor:
    print('row = %r' % (row,))
    
# DRIVER

conn = pyodbc.connect("DRIVER={{SQL Server}};SERVER={0}; database={1}; \
       trusted_connection=yes;UID={2};PWD={3}".format("SQLRISCORJ01","Mercado_financeiro_fundo","usuario = format.Fxxxxxxx","senha = format.XXXXXXXX"))
cursor = conn.cursor()

import pandas as pd
import pyodbc
from fast_to_sql import fast_to_sql as fts

# Test Dataframe for insertion
df = pd.DataFrame(your_dataframe_here)

# Create a pyodbc connection
conn = pyodbc.connect(
    Driver={ODBC Driver 17 for SQL Server};
    Server=localhost;
    Database=my_database;
    UID=my_user;
    PWD=my_pass;
)

# If a table is created, the generated sql is returned
create_statement = fts.fast_to_sql(df, "my_great_table", conn, if_exists="replace")

# Commit upload actions and close connection
conn.commit()
conn.close()



import pyodbc

# if you have user id and password then try with this connection string
connection_string = f"DRIVER={SQL Server};SERVER={server_name};UID={user_id};PWD={password}"

# if using in the local system then use the following connection string
connection_string = f"DRIVER={SQL Server};SERVER={server_name}; Trusted_Connection=True;"

connection= pyodbc.connect(connection_string)
cursor = connection.cursor()



sql_create_database = f"CREATE DATABASE {database_name}"
cursor.execute(sql_create_database)

set_database = f"USE {database_name}"
cursor.execute(set_database)

set_database = f"SELECT {database_name}"
cursor.execute(select_database)

set_database = f"UPDATE {database_name}"
cursor.execute(update_database)

set_database = f"DELETE {database_name}"
cursor.execute(delete_database)


# define execution engine using sqlalchemy create_engine() 
# method with your db params

engine = sqlalchemy.create_engine('ibm_db_sa://{user}:{pwd}@{host}:{port}/{db};SECURITY=SSL'.format(
    user=params['username'],
    pwd=params['password'],
    host=params['hostname'],
    port=params['port'],
    db=params['database']
))
db_table = pd.read_sql('SELECT * FROM TEST_SCHEMA.dummy_test', engine)

connection= pyodbc.connect(connection_string)
cursor = connection.cursor()

sql_create_database = f"CREATE DATABASE {database_name}"
cursor.execute(sql_create_database)

set_database = f"USE {database_name}"
cursor.execute(set_database)

set_database = f"SELECT {database_name}"
cursor.execute(select_database)

set_database = f"UPDATE {database_name}"
cursor.execute(update_database)

set_database = f"DELETE {database_name}"
cursor.execute(delete_database)

dbb = MySQLdb.connect(host="localhost", 
       user="user", 
       passwd="pass", 
       db="database") 

try:
   curb = dbb.cursor()
   curb.execute ("UPDATE RadioGroups SET CurrentState=1 WHERE RadioID=11")
   print "Row(s) were updated :" +  str(curb.rowcount)
   curb.close()
except MySQLdb.Error, e:
   print "query failed<br/>"
   print e  
      
   test (x,y)
   
   implenet_test (X_x);
mmmmmmm   
   split_test_X = (implement_test (X,x));
   split_test_Y = (implement_test (Y,y));
   
cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                      "Server=SQLRISCORJ01;"
                      "Database=Mercado_Financeiro_Fundo;"
                      "Trusted_Connection=yes;")

                                    
cursor = cnxn.cursor()
cursor.execute('SELECT * FROM Mercado_Financeiro_Fundos')

for row in cursor:
    print('row = %r' % (row,))   
    
   # Test Dataframe for insertion
   df = pd.DataFrame(your_dataframe_here)

   # Create a pyodbc connection
   conn = pyodbc.connect(
       Driver={ODBC Driver 17 for SQL Server};
       Server=localhost;
       Database=my_database;
       UID=my_user;
       PWD=my_pass;
   )

   # If a table is created, the generated sql is returned
   create_statement = fts.fast_to_sql(df, "my_great_table", conn, if_exists="replace")

   # Commit upload actions and close connection
   conn.commit()
   conn.close()
   for row in cursor:
       print('row = %r' % (row,))   
       
      # Test Dataframe for insertion
      df = pd.DataFrame(your_dataframe_here)

      # Create a pyodbc connection
      conn = pyodbc.connect(
          Driver={ODBC Driver 17 for SQL Server};
          Server=localhost;
          Database=my_database;
          UID=my_user;
          PWD=my_pass;
      )


      # If a table is created, the generated sql is returned
      create_statement = fts.fast_to_sql(df, "my_great_table", conn, if_exists="replace")

      # Commit upload actions and close connection
      conn.commit()
      conn.close()
       
   dbb = MySQLdb.connect(host="localhost", 
          user="user", 
          passwd="pass", 
          db="database") 

   try:
      curb = dbb.cursor()
      curb.execute ("UPDATE RadioGroups SET CurrentState=1 WHERE RadioID=11")
      print "Row(s) were updated :" +  str(curb.rowcount)
      curb.close()
   except MySQLdb.Error, e:
      print "query failed<br/>"
      print e  
         
      test (x,y)
      
      implenet_test (X_x);
   mmmmmmm   
      split_test_X = (implement_test (X,x));
      split_test_Y = (implement_test (Y,y));
      
   cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                         "Server=SQLRISCORJ01;"
                         "Database=Mercado_Financeiro_Fundo;"
                         "Trusted_Connection=yes;")

                                       
   cursor = cnxn.cursor()
   cursor.execute('SELECT * FROM Mercado_Financeiro_Fundos')

   for row in cursor:
       print('row = %r' % (row,))   
       
      # Test Dataframe for insertion
      df = pd.DataFrame(your_dataframe_here)

      # Create a pyodbc connection
      conn = pyodbc.connect(
          Driver={ODBC Driver 17 for SQL Server};
          Server=localhost;
          Database=my_database;
          UID=my_user;
          PWD=my_pass;
      )

      # If a table is created, the generated sql is returned
      create_statement = fts.fast_to_sql(df, "my_great_table", conn, if_exists="replace")

      # Commit upload actions and close connection
      conn.commit()
      conn.close()
       
      for row in cursor:
          print('row = %r' % (row,))   
          
         # Test Dataframe for insertion
         df = pd.DataFrame(your_dataframe_here)

         # Create a pyodbc connection
         conn = pyodbc.connect(
             Driver={ODBC Driver 17 for SQL Server};
             Server=localhost;
             Database=my_database;
             UID=my_user;
             PWD=my_pass;
         )
         # If a table is created, the generated sql is returned
         create_statement = fts.fast_to_sql(df, "my_great_table", conn, if_exists="replace")

         # Commit upload actions and close connection
         conn.commit()
         conn.close()

UPDATE Table
SET Table.col1 = other_table.col1,
 Table.col2 = other_table.col2
FROM
    Table
INNER JOIN other_table ON Table.id = other_table.id
WHERE
    Table.col1 != other_table.col1
OR Table.col2 != other_table.col2
OR (
    other_table.col1 IS NOT NULL
    AND Table.col1 IS NULL
)
OR (
    other_table.col2 IS NOT NULL
    AND Table.col2 IS NULL
)


