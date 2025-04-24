# -*- coding: utf-8 -*-


# @author: Romulo


UPDATE Table 
SET  Table.col1 = other_table.col1,
     Table.col2 = other_table.col2 
--select Table.col1, other_table.col,Table.col2,other_table.col2, *   
FROM     Table 
INNER JOIN     other_table 
    ON     Table.id = other_table.id 
    
    
    UPDATE Table1
INNER JOIN Table2
ON Table1.id = Table2.id
SET Table1.col1 = Table2.col1,
    Table1.col2 = Table2.col2
    UPDATE x
SET    x.col1 = x.newCol1,
       x.col2 = x.newCol2
FROM   (SELECT t.col1,
               t2.col1 AS newCol1,
               t.col2,
               t2.col2 AS newCol2
        FROM   [table] t
               JOIN other_table t2
                 ON t.ID = t2.ID) x

"""

"""UPDATE Table 
SET  Table.col1 = other_table.col1,
     Table.col2 = other_table.col2 
--select Table.col1, other_table.col,Table.col2,other_table.col2, *   
FROM     Table 
INNER JOIN     other_table 
    ON     Table.id = other_table.id 
    
    UPDATE Table1
INNER JOIN Table2
ON Table1.id = Table2.id
SET Table1.col1 = Table2.col1,
    Table1.col2 = Table2.col2



BEGIN TRY
        DECLARE @age INT = 160
        INSERT INTO TEST_TABLE VALUES ('QZ_TEST', @age + 1)
        INSERT INTO TEST_TABLE VALUES ('QZ_TEST', 'not a number')
        INSERT INTO ANOTHER_TABLE VALUES ('QZ_TEST', @age + 2)
END TRY
    BEGIN CATCH
        DECLARE @ErrorMessage NVARCHAR(4000)
        DECLARE @ErrorSeverity INT
        DECLARE @ErrorState INT

        SELECT 
            @ErrorMessage = ERROR_MESSAGE(),
            @ErrorSeverity = ERROR_SEVERITY(),
            @ErrorState = ERROR_STATE()

        RAISERROR (@ErrorMessage,
                   @ErrorSeverity, -- Level 16
                   @ErrorState
                   )
    END CATCH



import pandas as pd
import pyodbc
from fast_to_sql import fast_to_sql as fts

# Test Dataframe for insertion
df = pd.DataFrame(your_dataframe_here)

# Create a pyodbc connection
conn = pyodbc.connect(
    """
    Driver={ODBC Driver 17 for SQL Server};
    Server=localhost;
    Database=my_database;
    UID=my_user;
    PWD=my_pass;
    """
)

# If a table is created, the generated sql is returned
create_statement = fts.fast_to_sql(df, "my_great_table", conn, if_exists="replace")

# Commit upload actions and close connection
conn.commit()
conn.close()

def SaveData(self, aScrapeResult):
    sql = "EXECUTE mc.SaveFundamentalDataCSV @pSource='%s',@pCountry='%s',@pOperator='%s',@pFromCountry='%s',@pFromOperator='%s',@pToCountry='%s',@pToOperator='%s',@pSiteName='%s',@pFactor='%s',@pGranularity='%s',@pDescription='%s',@pDataType='%s',@pTechnology = '%s',@pcsvData='%s'"

    #   Need to convert the data into CSV
    util = ListToCsvUtil()
    csvValues = util.ListToCsv(aScrapeResult.DataPoints)

    formattedSQL = sql % (aScrapeResult.Source ,aScrapeResult.Country,aScrapeResult.Operator ,aScrapeResult.FromCountry ,aScrapeResult.FromOperator ,aScrapeResult.ToCountry ,aScrapeResult.ToOperator ,aScrapeResult.SiteName ,aScrapeResult.Factor ,aScrapeResult.Granularity ,aScrapeResult.Description ,aScrapeResult.DataType ,aScrapeResult.Technology ,csvValues)

    DB = create_engine(self.ConnectionString)
    DB.connect()

    result_proxy = DB.execute(formattedSQL)

    results = result_proxy.fetchall()
    
    import pyodbc

DATABASES = {
    'default': {
        'ENGINE': 'sql_server.pyodbc',
        'NAME': 'db1',
        'USER': 'admin02',
        'PASSWORD': *****,  
        'HOST': '<host_name>.database.windows.net',
        'PORT': '1433',
        'OPTIONS': {
            'driver': 'ODBC Driver 18 for SQL Server',
        },
    },
}



db_settings = DATABASES['default']

try:
    conn = pyodbc.connect(
        driver=db_settings['OPTIONS']['driver'],
        server=db_settings['HOST'],
        database=db_settings['NAME'],
        user=db_settings['USER'],
        password=db_settings['PASSWORD'],
        port=db_settings['PORT'],
    )

    print("Connected to the database successfully!")

    cursor = conn.cursor()
    cursor.execute("SELECT * FROM Table1")
    rows = cursor.fetchall()
    for row in rows:
        print(row)
        

    cursor.close()
    conn.close()

except pyodbc.Error as e:
    print("Error connecting to the database:", e)
    

    UPDATE Table 
    SET  Table.col1 = other_table.col1,
         Table.col2 = other_table.col2 
    --select Table.col1, other_table.col,Table.col2,other_table.col2, *   
    FROM     Table 
    INNER JOIN     other_table 
        ON     Table.id = other_table.id 
        
        UPDATE Table1
    INNER JOIN Table2
    ON Table1.id = Table2.id
    SET Table1.col1 = Table2.col1,
        Table1.col2 = Table2.col2



    BEGIN TRY
            DECLARE @age INT = 160
            INSERT INTO TEST_TABLE VALUES ('QZ_TEST', @age + 1)
            INSERT INTO TEST_TABLE VALUES ('QZ_TEST', 'not a number')
            INSERT INTO ANOTHER_TABLE VALUES ('QZ_TEST', @age + 2)
    END TRY
        BEGIN CATCH
            DECLARE @ErrorMessage NVARCHAR(4000)
            DECLARE @ErrorSeverity INT
            DECLARE @ErrorState INT

            SELECT 
                @ErrorMessage = ERROR_MESSAGE(),
                @ErrorSeverity = ERROR_SEVERITY(),
                @ErrorState = ERROR_STATE()

            RAISERROR (@ErrorMessage,
                       @ErrorSeverity, -- Level 16
                       @ErrorState
                       )
        END CATCH
        
        UPDATE Table 
        SET  Table.col1 = other_table.col1,
             Table.col2 = other_table.col2 
        --select Table.col1, other_table.col,Table.col2,other_table.col2, *   
        FROM     Table 
        INNER JOIN     other_table 
            ON     Table.id = other_table.id 

            
            UPDATE Table1
        INNER JOIN Table2
        ON Table1.id = Table2.id
        SET Table1.col1 = Table2.col1,
            Table1.col2 = Table2.col2
            
            UPDATE x
        SET    x.col1 = x.newCol1,
               x.col2 = x.newCol2
        FROM   (SELECT t.col1,
                       t2.col1 AS newCol1,
                       t.col2,
                       t2.col2 AS newCol2
                FROM   [table] t
                       JOIN other_table t2
                         ON t.ID = t2.ID) x
        """



        """UPDATE Table 
        SET  Table.col1 = other_table.col1,
             Table.col2 = other_table.col2 
        --select Table.col1, other_table.col,Table.col2,other_table.col2, *   
        FROM     Table 
        INNER JOIN     other_table 
            ON     Table.id = other_table.id 
            
            UPDATE Table1
        INNER JOIN Table2
        ON Table1.id = Table2.id
        SET Table1.col1 = Table2.col1,
            Table1.col2 = Table2.col2



        BEGIN TRY
                DECLARE @age INT = 160
                INSERT INTO TEST_TABLE VALUES ('QZ_TEST', @age + 1)
                INSERT INTO TEST_TABLE VALUES ('QZ_TEST', 'not a number')
                INSERT INTO ANOTHER_TABLE VALUES ('QZ_TEST', @age + 2)
        END TRY
            BEGIN CATCH
                DECLARE @ErrorMessage NVARCHAR(4000)
                DECLARE @ErrorSeverity INT
                DECLARE @ErrorState INT

                SELECT 
                    @ErrorMessage = ERROR_MESSAGE(),
                    @ErrorSeverity = ERROR_SEVERITY(),
                    @ErrorState = ERROR_STATE()

                RAISERROR (@ErrorMessage,
                           @ErrorSeverity, -- Level 16
                           @ErrorState
                           )
            END CATCH



        import pandas as pd
        import pyodbc
        from fast_to_sql import fast_to_sql as fts

        # Test Dataframe for insertion
        df = pd.DataFrame(your_dataframe_here)

        # Create a pyodbc connection
        conn = pyodbc.connect(
            """
            Driver={ODBC Driver 17 for SQL Server};
            Server=localhost;
            Database=my_database;
            UID=my_user;
            PWD=my_pass;
            """
        )

        # If a table is created, the generated sql is returned
        create_statement = fts.fast_to_sql(df, "my_great_table", conn, if_exists="replace")

        # Commit upload actions and close connection
        conn.commit()
        conn.close()

        def SaveData(self, aScrapeResult):
            sql = "EXECUTE mc.SaveFundamentalDataCSV @pSource='%s',@pCountry='%s',@pOperator='%s',@pFromCountry='%s',@pFromOperator='%s',@pToCountry='%s',@pToOperator='%s',@pSiteName='%s',@pFactor='%s',@pGranularity='%s',@pDescription='%s',@pDataType='%s',@pTechnology = '%s',@pcsvData='%s'"

            #   Need to convert the data into CSV
            util = ListToCsvUtil()
            csvValues = util.ListToCsv(aScrapeResult.DataPoints)

            formattedSQL = sql % (aScrapeResult.Source ,aScrapeResult.Country,aScrapeResult.Operator ,aScrapeResult.FromCountry ,aScrapeResult.FromOperator ,aScrapeResult.ToCountry ,aScrapeResult.ToOperator ,aScrapeResult.SiteName ,aScrapeResult.Factor ,aScrapeResult.Granularity ,aScrapeResult.Description ,aScrapeResult.DataType ,aScrapeResult.Technology ,csvValues)

            DB = create_engine(self.ConnectionString)
            DB.connect()

            result_proxy = DB.execute(formattedSQL)

            results = result_proxy.fetchall()
            
            import pyodbc

        DATABASES = {
            'default': {
                'ENGINE': 'sql_server.pyodbc',
                'NAME': 'db1',
                'USER': 'admin02',
                'PASSWORD': *****,  
                'HOST': '<host_name>.database.windows.net',
                'PORT': '1433',
                'OPTIONS': {
                    'driver': 'ODBC Driver 18 for SQL Server',
                },
            },
        }



        db_settings = DATABASES['default']

        try:
            conn = pyodbc.connect(
                driver=db_settings['OPTIONS']['driver'],
                server=db_settings['HOST'],
                database=db_settings['NAME'],
                user=db_settings['USER'],
                password=db_settings['PASSWORD'],
                port=db_settings['PORT'],
            )

            print("Connected to the database successfully!")

            cursor = conn.cursor()
            cursor.execute("SELECT * FROM Table1")
            rows = cursor.fetchall()
            for row in rows:
                print(row)
                

            cursor.close()
            conn.close()

        except pyodbc.Error as e:
            print("Error connecting to the database:", e)
            

            UPDATE Table 
            SET  Table.col1 = other_table.col1,
                 Table.col2 = other_table.col2 
            --select Table.col1, other_table.col,Table.col2,other_table.col2, *   
            FROM     Table 
            INNER JOIN     other_table 
                ON     Table.id = other_table.id 
                
                UPDATE Table1
            INNER JOIN Table2
            ON Table1.id = Table2.id
            SET Table1.col1 = Table2.col1,
                Table1.col2 = Table2.col2



            BEGIN TRY
                    DECLARE @age INT = 160
                    INSERT INTO TEST_TABLE VALUES ('QZ_TEST', @age + 1)
                    INSERT INTO TEST_TABLE VALUES ('QZ_TEST', 'not a number')
                    INSERT INTO ANOTHER_TABLE VALUES ('QZ_TEST', @age + 2)
            END TRY
                BEGIN CATCH
                    DECLARE @ErrorMessage NVARCHAR(4000)
                    DECLARE @ErrorSeverity INT
                    DECLARE @ErrorState INT

                    SELECT 
                        @ErrorMessage = ERROR_MESSAGE(),
                        @ErrorSeverity = ERROR_SEVERITY(),
                        @ErrorState = ERROR_STATE()

                    RAISERROR (@ErrorMessage,
                               @ErrorSeverity, -- Level 16
                               @ErrorState
                               )
                END CATCH

import pyodbc

DATABASES = {
    'default': {
        'ENGINE': 'sql_server.pyodbc',
        'NAME': 'db1',
        'USER': 'admin02',
        'PASSWORD': *****,  
        'HOST': '<host_name>.database.windows.net',
        'PORT': '1433',
        'OPTIONS': {
            'driver': 'ODBC Driver 18 for SQL Server',
        },
    },
}

db_settings = DATABASES['default']

try:
    conn = pyodbc.connect(
        driver=db_settings['OPTIONS']['driver'],
        server=db_settings['HOST'],
        database=db_settings['NAME'],
        user=db_settings['USER'],
        password=db_settings['PASSWORD'],
        port=db_settings['PORT'],
    )

    print("Connected to the database successfully!")

    cursor = conn.cursor()
    cursor.execute("SELECT * FROM Table1")
    rows = cursor.fetchall()
    for row in rows:
        print(row)
        
    connect_str = "Driver={ODBC Driver 17 for SQL Server};" + \
    "Server={server},1433;".format(server='tcp:ipaddress.database.windows.net') + \
    "Database={database};".format(database='mydb') + \
    "uid={uid};".format(uid='myuserid') + \
    "pwd={pwd};".format(pwd='secretpswd') + \
    "Encrypt=yes;TrustServerCertificate=no;"
     cnxn = pyodbc.connect(connect_str)

    cursor.close()
    conn.close()

    except pyodbc.Error as e:
    print("Error connecting to the database:", e)
    
    
    UPDATE Table 
    SET  Table.col1 = other_table.col1,
         Table.col2 = other_table.col2 
    --select Table.col1, other_table.col,Table.col2,other_table.col2, *   
    FROM     Table 
    INNER JOIN     other_table 
        ON     Table.id = other_table.id 

        
        UPDATE Table1
    INNER JOIN Table2
    ON Table1.id = Table2.id
    SET Table1.col1 = Table2.col1,
        Table1.col2 = Table2.col2
        UPDATE x
    SET    x.col1 = x.newCol1,
           x.col2 = x.newCol2
    FROM   (SELECT t.col1,
                   t2.col1 AS newCol1,
                   t.col2,
                   t2.col2 AS newCol2
            FROM   [table] t
                   JOIN other_table t2
                     ON t.ID = t2.ID) x

    """



    """UPDATE Table 
    SET  Table.col1 = other_table.col1,
         Table.col2 = other_table.col2 
    --select Table.col1, other_table.col,Table.col2,other_table.col2, *   
    FROM     Table 
    INNER JOIN     other_table 
        ON     Table.id = other_table.id 
        
        UPDATE Table1
    INNER JOIN Table2
    ON Table1.id = Table2.id
    SET Table1.col1 = Table2.col1,
        Table1.col2 = Table2.col2



    BEGIN TRY
            DECLARE @age INT = 160
            INSERT INTO TEST_TABLE VALUES ('QZ_TEST', @age + 1)
            INSERT INTO TEST_TABLE VALUES ('QZ_TEST', 'not a number')
            INSERT INTO ANOTHER_TABLE VALUES ('QZ_TEST', @age + 2)
    END TRY
        BEGIN CATCH
            DECLARE @ErrorMessage NVARCHAR(4000)
            DECLARE @ErrorSeverity INT
            DECLARE @ErrorState INT

            SELECT 
                @ErrorMessage = ERROR_MESSAGE(),
                @ErrorSeverity = ERROR_SEVERITY(),
                @ErrorState = ERROR_STATE()

            RAISERROR (@ErrorMessage,
                       @ErrorSeverity, -- Level 16
                       @ErrorState
                       )
        END CATCH



    import pandas as pd
    import pyodbc
    from fast_to_sql import fast_to_sql as fts

    # Test Dataframe for insertion
    df = pd.DataFrame(your_dataframe_here)

    # Create a pyodbc connection
    conn = pyodbc.connect(
        """
        Driver={ODBC Driver 17 for SQL Server};
        Server=localhost;
        Database=my_database;
        UID=my_user;
        PWD=my_pass;
        """
    )

    # If a table is created, the generated sql is returned
    create_statement = fts.fast_to_sql(df, "my_great_table", conn, if_exists="replace")

    # Commit upload actions and close connection
    conn.commit()
    conn.close()

    def SaveData(self, aScrapeResult):
        sql = "EXECUTE mc.SaveFundamentalDataCSV @pSource='%s',@pCountry='%s',@pOperator='%s',@pFromCountry='%s',@pFromOperator='%s',@pToCountry='%s',@pToOperator='%s',@pSiteName='%s',@pFactor='%s',@pGranularity='%s',@pDescription='%s',@pDataType='%s',@pTechnology = '%s',@pcsvData='%s'"

        #   Need to convert the data into CSV
        util = ListToCsvUtil()
        csvValues = util.ListToCsv(aScrapeResult.DataPoints)

        formattedSQL = sql % (aScrapeResult.Source ,aScrapeResult.Country,aScrapeResult.Operator ,aScrapeResult.FromCountry ,aScrapeResult.FromOperator ,aScrapeResult.ToCountry ,aScrapeResult.ToOperator ,aScrapeResult.SiteName ,aScrapeResult.Factor ,aScrapeResult.Granularity ,aScrapeResult.Description ,aScrapeResult.DataType ,aScrapeResult.Technology ,csvValues)

        DB = create_engine(self.ConnectionString)
        DB.connect()

        result_proxy = DB.execute(formattedSQL)

        results = result_proxy.fetchall()
        
        import pyodbc

    DATABASES = {
        'default': {
            'ENGINE': 'sql_server.pyodbc',
            'NAME': 'db1',
            'USER': 'admin02',
            'PASSWORD': *****,  
            'HOST': '<host_name>.database.windows.net',
            'PORT': '1433',
            'OPTIONS': {
                'driver': 'ODBC Driver 18 for SQL Server',
            },
        },
    }



    db_settings = DATABASES['default']

    try:
        conn = pyodbc.connect(
            driver=db_settings['OPTIONS']['driver'],
            server=db_settings['HOST'],
            database=db_settings['NAME'],
            user=db_settings['USER'],
            password=db_settings['PASSWORD'],
            port=db_settings['PORT'],
        )

        print("Connected to the database successfully!")

        cursor = conn.cursor()
        cursor.execute("SELECT * FROM Table1")
        rows = cursor.fetchall()
        for row in rows:
            print(row)
            

        cursor.close()
        conn.close()

    except pyodbc.Error as e:
        print("Error connecting to the database:", e)
        

        UPDATE Table 
        SET  Table.col1 = other_table.col1,
             Table.col2 = other_table.col2 
        --select Table.col1, other_table.col,Table.col2,other_table.col2, *   
        FROM     Table 
        INNER JOIN     other_table 
            ON     Table.id = other_table.id 
            
            UPDATE Table1
        INNER JOIN Table2
        ON Table1.id = Table2.id
        SET Table1.col1 = Table2.col1,
            Table1.col2 = Table2.col2



        BEGIN TRY
                DECLARE @age INT = 160
                INSERT INTO TEST_TABLE VALUES ('QZ_TEST', @age + 1)
                INSERT INTO TEST_TABLE VALUES ('QZ_TEST', 'not a number')
                INSERT INTO ANOTHER_TABLE VALUES ('QZ_TEST', @age + 2)
        END TRY
            BEGIN CATCH
                DECLARE @ErrorMessage NVARCHAR(4000)
                DECLARE @ErrorSeverity INT
                DECLARE @ErrorState INT

                SELECT 
                    @ErrorMessage = ERROR_MESSAGE(),
                    @ErrorSeverity = ERROR_SEVERITY(),
                    @ErrorState = ERROR_STATE()

                RAISERROR (@ErrorMessage,
                           @ErrorSeverity, -- Level 16
                           @ErrorState
                           )
            END CATCH
            
            UPDATE Table 
            SET  Table.col1 = other_table.col1,
                 Table.col2 = other_table.col2 
            --select Table.col1, other_table.col,Table.col2,other_table.col2, *   
            FROM     Table 
            INNER JOIN     other_table 
                ON     Table.id = other_table.id 

                
                UPDATE Table1
            INNER JOIN Table2
            ON Table1.id = Table2.id
            SET Table1.col1 = Table2.col1,
                Table1.col2 = Table2.col2
                
                UPDATE x
            SET    x.col1 = x.newCol1,
                   x.col2 = x.newCol2
            FROM   (SELECT t.col1,
                           t2.col1 AS newCol1,
                           t.col2,
                           t2.col2 AS newCol2
                    FROM   [table] t
                           JOIN other_table t2
                             ON t.ID = t2.ID) x
            """



            """UPDATE Table 
            SET  Table.col1 = other_table.col1,
                 Table.col2 = other_table.col2 
            --select Table.col1, other_table.col,Table.col2,other_table.col2, *   
            FROM     Table 
            INNER JOIN     other_table 
                ON     Table.id = other_table.id 
                
                UPDATE Table1
            INNER JOIN Table2
            ON Table1.id = Table2.id
            SET Table1.col1 = Table2.col1,
                Table1.col2 = Table2.col2



            BEGIN TRY
                    DECLARE @age INT = 160
                    INSERT INTO TEST_TABLE VALUES ('QZ_TEST', @age + 1)
                    INSERT INTO TEST_TABLE VALUES ('QZ_TEST', 'not a number')
                    INSERT INTO ANOTHER_TABLE VALUES ('QZ_TEST', @age + 2)
            END TRY
                BEGIN CATCH
                    DECLARE @ErrorMessage NVARCHAR(4000)
                    DECLARE @ErrorSeverity INT
                    DECLARE @ErrorState INT

                    SELECT 
                        @ErrorMessage = ERROR_MESSAGE(),
                        @ErrorSeverity = ERROR_SEVERITY(),
                        @ErrorState = ERROR_STATE()

                    RAISERROR (@ErrorMessage,
                               @ErrorSeverity, -- Level 16
                               @ErrorState
                               )
                END CATCH



            import pandas as pd
            import pyodbc
            from fast_to_sql import fast_to_sql as fts

            # Test Dataframe for insertion
            df = pd.DataFrame(your_dataframe_here)

            # Create a pyodbc connection
            conn = pyodbc.connect(
                """
                Driver={ODBC Driver 17 for SQL Server};
                Server=localhost;
                Database=my_database;
                UID=my_user;
                PWD=my_pass;
                """
            )

            # If a table is created, the generated sql is returned
            create_statement = fts.fast_to_sql(df, "my_great_table", conn, if_exists="replace")

            # Commit upload actions and close connection
            conn.commit()
            conn.close()

            def SaveData(self, aScrapeResult):
                sql = "EXECUTE mc.SaveFundamentalDataCSV @pSource='%s',@pCountry='%s',@pOperator='%s',@pFromCountry='%s',@pFromOperator='%s',@pToCountry='%s',@pToOperator='%s',@pSiteName='%s',@pFactor='%s',@pGranularity='%s',@pDescription='%s',@pDataType='%s',@pTechnology = '%s',@pcsvData='%s'"

                #   Need to convert the data into CSV
                util = ListToCsvUtil()
                csvValues = util.ListToCsv(aScrapeResult.DataPoints)

                formattedSQL = sql % (aScrapeResult.Source ,aScrapeResult.Country,aScrapeResult.Operator ,aScrapeResult.FromCountry ,aScrapeResult.FromOperator ,aScrapeResult.ToCountry ,aScrapeResult.ToOperator ,aScrapeResult.SiteName ,aScrapeResult.Factor ,aScrapeResult.Granularity ,aScrapeResult.Description ,aScrapeResult.DataType ,aScrapeResult.Technology ,csvValues)

                DB = create_engine(self.ConnectionString)
                DB.connect()

                result_proxy = DB.execute(formattedSQL)

                results = result_proxy.fetchall()
                
                import pyodbc
                
                

            DATABASES = {
                'default': {
                    'ENGINE': 'sql_server.pyodbc',
                    'NAME': 'db1',
                    'USER': 'admin02',
                    'PASSWORD': *****,  
                    'HOST': '<host_name>.database.windows.net',
                    'PORT': '1433',
                    'OPTIONS': {
                        'driver': 'ODBC Driver 18 for SQL Server',
                    },
                },
            }



            db_settings = DATABASES['default']

            try:
                conn = pyodbc.connect(
                    driver=db_settings['OPTIONS']['driver'],
                    server=db_settings['HOST'],
                    database=db_settings['NAME'],
                    user=db_settings['USER'],
                    password=db_settings['PASSWORD'],
                    port=db_settings['PORT'],
                )

                print("Connected to the database successfully!")

                cursor = conn.cursor()
                cursor.execute("SELECT * FROM Table1")
                rows = cursor.fetchall()
                for row in rows:
                    print(row)
                cursor.close()
                conn.close()

            except pyodbc.Error as e:
                print("Error connecting to the database:", e)
                
                UPDATE Table 
                SET  Table.col1 = other_table.col1,
                     Table.col2 = other_table.col2 
                --select Table.col1, other_table.col,Table.col2,other_table.col2, *   
                FROM     Table 
                INNER JOIN     other_table 
                    ON     Table.id = other_table.id 
                    
                    UPDATE Table1
                INNER JOIN Table2
                ON Table1.id = Table2.id
                SET Table1.col1 = Table2.col1,
                    Table1.col2 = Table2.col2

UPDATE Table 
SET  Table.col1 = other_table.col1,
     Table.col2 = other_table.col2 
--select Table.col1, other_table.col,Table.col2,other_table.col2, *   
FROM     Table 
INNER JOIN     other_table 
    ON     Table.id = other_table.id 
    
    
    UPDATE Table1
INNER JOIN Table2
ON Table1.id = Table2.id
SET Table1.col1 = Table2.col1,
    Table1.col2 = Table2.col2
    UPDATE x
SET    x.col1 = x.newCol1,
       x.col2 = x.newCol2
FROM   (SELECT t.col1,
               t2.col1 AS newCol1,
               t.col2,
               t2.col2 AS newCol2
        FROM   [table] t
               JOIN other_table t2
                 ON t.ID = t2.ID) x

"""

"""UPDATE Table 
SET  Table.col1 = other_table.col1,
     Table.col2 = other_table.col2 
--select Table.col1, other_table.col,Table.col2,other_table.col2, *   
FROM     Table 
INNER JOIN     other_table 
    ON     Table.id = other_table.id 
    
    UPDATE Table1
INNER JOIN Table2
ON Table1.id = Table2.id
SET Table1.col1 = Table2.col1,
    Table1.col2 = Table2.col2



BEGIN TRY
        DECLARE @age INT = 160
        INSERT INTO TEST_TABLE VALUES ('QZ_TEST', @age + 1)
        INSERT INTO TEST_TABLE VALUES ('QZ_TEST', 'not a number')
        INSERT INTO ANOTHER_TABLE VALUES ('QZ_TEST', @age + 2)
END TRY
    BEGIN CATCH
        DECLARE @ErrorMessage NVARCHAR(4000)
        DECLARE @ErrorSeverity INT
        DECLARE @ErrorState INT

        SELECT 
            @ErrorMessage = ERROR_MESSAGE(),
            @ErrorSeverity = ERROR_SEVERITY(),
            @ErrorState = ERROR_STATE()

        RAISERROR (@ErrorMessage,
                   @ErrorSeverity, -- Level 16
                   @ErrorState
                   )
    END CATCH



import pandas as pd
import pyodbc
from fast_to_sql import fast_to_sql as fts

# Test Dataframe for insertion
df = pd.DataFrame(your_dataframe_here)

# Create a pyodbc connection
conn = pyodbc.connect(
    """
    Driver={ODBC Driver 17 for SQL Server};
    Server=localhost;
    Database=my_database;
    UID=my_user;
    PWD=my_pass;
    """
)

# If a table is created, the generated sql is returned
create_statement = fts.fast_to_sql(df, "my_great_table", conn, if_exists="replace")

# Commit upload actions and close connection
conn.commit()
conn.close()

def SaveData(self, aScrapeResult):
    sql = "EXECUTE mc.SaveFundamentalDataCSV @pSource='%s',@pCountry='%s',@pOperator='%s',@pFromCountry='%s',@pFromOperator='%s',@pToCountry='%s',@pToOperator='%s',@pSiteName='%s',@pFactor='%s',@pGranularity='%s',@pDescription='%s',@pDataType='%s',@pTechnology = '%s',@pcsvData='%s'"

    #   Need to convert the data into CSV
    util = ListToCsvUtil()
    csvValues = util.ListToCsv(aScrapeResult.DataPoints)

    formattedSQL = sql % (aScrapeResult.Source ,aScrapeResult.Country,aScrapeResult.Operator ,aScrapeResult.FromCountry ,aScrapeResult.FromOperator ,aScrapeResult.ToCountry ,aScrapeResult.ToOperator ,aScrapeResult.SiteName ,aScrapeResult.Factor ,aScrapeResult.Granularity ,aScrapeResult.Description ,aScrapeResult.DataType ,aScrapeResult.Technology ,csvValues)

    DB = create_engine(self.ConnectionString)
    DB.connect()

    result_proxy = DB.execute(formattedSQL)

    results = result_proxy.fetchall()
    
    import pyodbc

DATABASES = {
    'default': {
        'ENGINE': 'sql_server.pyodbc',
        'NAME': 'db1',
        'USER': 'admin02',
        'PASSWORD': *****,  
        'HOST': '<host_name>.database.windows.net',
        'PORT': '1433',
        'OPTIONS': {
            'driver': 'ODBC Driver 18 for SQL Server',
        },
    },
}



db_settings = DATABASES['default']

try:
    conn = pyodbc.connect(
        driver=db_settings['OPTIONS']['driver'],
        server=db_settings['HOST'],
        database=db_settings['NAME'],
        user=db_settings['USER'],
        password=db_settings['PASSWORD'],
        port=db_settings['PORT'],
    )

    print("Connected to the database successfully!")

    cursor = conn.cursor()
    cursor.execute("SELECT * FROM Table1")
    rows = cursor.fetchall()
    for row in rows:
        print(row)
        

    cursor.close()
    conn.close()

except pyodbc.Error as e:
    print("Error connecting to the database:", e)
    

    UPDATE Table 
    SET  Table.col1 = other_table.col1,
         Table.col2 = other_table.col2 
    --select Table.col1, other_table.col,Table.col2,other_table.col2, *   
    FROM     Table 
    INNER JOIN     other_table 
        ON     Table.id = other_table.id 
        
        UPDATE Table1
    INNER JOIN Table2
    ON Table1.id = Table2.id
    SET Table1.col1 = Table2.col1,
        Table1.col2 = Table2.col2



    BEGIN TRY
            DECLARE @age INT = 160
            INSERT INTO TEST_TABLE VALUES ('QZ_TEST', @age + 1)
            INSERT INTO TEST_TABLE VALUES ('QZ_TEST', 'not a number')
            INSERT INTO ANOTHER_TABLE VALUES ('QZ_TEST', @age + 2)
    END TRY
        BEGIN CATCH
            DECLARE @ErrorMessage NVARCHAR(4000)
            DECLARE @ErrorSeverity INT
            DECLARE @ErrorState INT

            SELECT 
                @ErrorMessage = ERROR_MESSAGE(),
                @ErrorSeverity = ERROR_SEVERITY(),
                @ErrorState = ERROR_STATE()

            RAISERROR (@ErrorMessage,
                       @ErrorSeverity, -- Level 16
                       @ErrorState
                       )
        END CATCH
        
        UPDATE Table 
        SET  Table.col1 = other_table.col1,
             Table.col2 = other_table.col2 
        --select Table.col1, other_table.col,Table.col2,other_table.col2, *   
        FROM     Table 
        INNER JOIN     other_table 
            ON     Table.id = other_table.id 

            
            UPDATE Table1
        INNER JOIN Table2
        ON Table1.id = Table2.id
        SET Table1.col1 = Table2.col1,
            Table1.col2 = Table2.col2
            
            UPDATE x
        SET    x.col1 = x.newCol1,
               x.col2 = x.newCol2
        FROM   (SELECT t.col1,
                       t2.col1 AS newCol1,
                       t.col2,
                       t2.col2 AS newCol2
                FROM   [table] t
                       JOIN other_table t2
                         ON t.ID = t2.ID) x
        """



        """UPDATE Table 
        SET  Table.col1 = other_table.col1,
             Table.col2 = other_table.col2 
        --select Table.col1, other_table.col,Table.col2,other_table.col2, *   
        FROM     Table 
        INNER JOIN     other_table 
            ON     Table.id = other_table.id 
            
            UPDATE Table1
        INNER JOIN Table2
        ON Table1.id = Table2.id
        SET Table1.col1 = Table2.col1,
            Table1.col2 = Table2.col2



        BEGIN TRY
                DECLARE @age INT = 160
                INSERT INTO TEST_TABLE VALUES ('QZ_TEST', @age + 1)
                INSERT INTO TEST_TABLE VALUES ('QZ_TEST', 'not a number')
                INSERT INTO ANOTHER_TABLE VALUES ('QZ_TEST', @age + 2)
        END TRY
            BEGIN CATCH
                DECLARE @ErrorMessage NVARCHAR(4000)
                DECLARE @ErrorSeverity INT
                DECLARE @ErrorState INT

                SELECT 
                    @ErrorMessage = ERROR_MESSAGE(),
                    @ErrorSeverity = ERROR_SEVERITY(),
                    @ErrorState = ERROR_STATE()

                RAISERROR (@ErrorMessage,
                           @ErrorSeverity, -- Level 16
                           @ErrorState
                           )
            END CATCH



        import pandas as pd
        import pyodbc
        from fast_to_sql import fast_to_sql as fts

        # Test Dataframe for insertion
        df = pd.DataFrame(your_dataframe_here)

        # Create a pyodbc connection
        conn = pyodbc.connect(
            """
            Driver={ODBC Driver 17 for SQL Server};
            Server=localhost;
            Database=my_database;
            UID=my_user;
            PWD=my_pass;
            """
        )

        # If a table is created, the generated sql is returned
        create_statement = fts.fast_to_sql(df, "my_great_table", conn, if_exists="replace")

        # Commit upload actions and close connection
        conn.commit()
        conn.close()

        def SaveData(self, aScrapeResult):
            sql = "EXECUTE mc.SaveFundamentalDataCSV @pSource='%s',@pCountry='%s',@pOperator='%s',@pFromCountry='%s',@pFromOperator='%s',@pToCountry='%s',@pToOperator='%s',@pSiteName='%s',@pFactor='%s',@pGranularity='%s',@pDescription='%s',@pDataType='%s',@pTechnology = '%s',@pcsvData='%s'"

            #   Need to convert the data into CSV
            util = ListToCsvUtil()
            csvValues = util.ListToCsv(aScrapeResult.DataPoints)

            formattedSQL = sql % (aScrapeResult.Source ,aScrapeResult.Country,aScrapeResult.Operator ,aScrapeResult.FromCountry ,aScrapeResult.FromOperator ,aScrapeResult.ToCountry ,aScrapeResult.ToOperator ,aScrapeResult.SiteName ,aScrapeResult.Factor ,aScrapeResult.Granularity ,aScrapeResult.Description ,aScrapeResult.DataType ,aScrapeResult.Technology ,csvValues)

            DB = create_engine(self.ConnectionString)
            DB.connect()

            result_proxy = DB.execute(formattedSQL)

            results = result_proxy.fetchall()
            
            import pyodbc

        DATABASES = {
            'default': {
                'ENGINE': 'sql_server.pyodbc',
                'NAME': 'db1',
                'USER': 'admin02',
                'PASSWORD': *****,  
                'HOST': '<host_name>.database.windows.net',
                'PORT': '1433',
                'OPTIONS': {
                    'driver': 'ODBC Driver 18 for SQL Server',
                },
            },
        }



        db_settings = DATABASES['default']

        try:
            conn = pyodbc.connect(
                driver=db_settings['OPTIONS']['driver'],
                server=db_settings['HOST'],
                database=db_settings['NAME'],
                user=db_settings['USER'],
                password=db_settings['PASSWORD'],
                port=db_settings['PORT'],
            )

            print("Connected to the database successfully!")

            cursor = conn.cursor()
            cursor.execute("SELECT * FROM Table1")
            rows = cursor.fetchall()
            for row in rows:
                print(row)
                

            cursor.close()
            conn.close()

        except pyodbc.Error as e:
            print("Error connecting to the database:", e)
            

            UPDATE Table 
            SET  Table.col1 = other_table.col1,
                 Table.col2 = other_table.col2 
            --select Table.col1, other_table.col,Table.col2,other_table.col2, *   
            FROM     Table 
            INNER JOIN     other_table 
                ON     Table.id = other_table.id 
                
                UPDATE Table1
            INNER JOIN Table2
            ON Table1.id = Table2.id
            SET Table1.col1 = Table2.col1,
                Table1.col2 = Table2.col2



            BEGIN TRY
                    DECLARE @age INT = 160
                    INSERT INTO TEST_TABLE VALUES ('QZ_TEST', @age + 1)
                    INSERT INTO TEST_TABLE VALUES ('QZ_TEST', 'not a number')
                    INSERT INTO ANOTHER_TABLE VALUES ('QZ_TEST', @age + 2)
            END TRY
                BEGIN CATCH
                    DECLARE @ErrorMessage NVARCHAR(4000)
                    DECLARE @ErrorSeverity INT
                    DECLARE @ErrorState INT

                    SELECT 
                        @ErrorMessage = ERROR_MESSAGE(),
                        @ErrorSeverity = ERROR_SEVERITY(),
                        @ErrorState = ERROR_STATE()

                    RAISERROR (@ErrorMessage,
                               @ErrorSeverity, -- Level 16
                               @ErrorState
                               )
                END CATCH

import pyodbc

DATABASES = {
    'default': {
        'ENGINE': 'sql_server.pyodbc',
        'NAME': 'db1',
        'USER': 'admin02',
        'PASSWORD': *****,  
        'HOST': '<host_name>.database.windows.net',
        'PORT': '1433',
        'OPTIONS': {
            'driver': 'ODBC Driver 18 for SQL Server',
        },
    },
}

db_settings = DATABASES['default']

try:
    conn = pyodbc.connect(
        driver=db_settings['OPTIONS']['driver'],
        server=db_settings['HOST'],
        database=db_settings['NAME'],
        user=db_settings['USER'],
        password=db_settings['PASSWORD'],
        port=db_settings['PORT'],
    )

    print("Connected to the database successfully!")

    cursor = conn.cursor()
    cursor.execute("SELECT * FROM Table1")
    rows = cursor.fetchall()
    for row in rows:
        print(row)
        
    connect_str = "Driver={ODBC Driver 17 for SQL Server};" + \
    "Server={server},1433;".format(server='tcp:ipaddress.database.windows.net') + \
    "Database={database};".format(database='mydb') + \
    "uid={uid};".format(uid='myuserid') + \
    "pwd={pwd};".format(pwd='secretpswd') + \
    "Encrypt=yes;TrustServerCertificate=no;"
     cnxn = pyodbc.connect(connect_str)

    cursor.close()
    conn.close()

    except pyodbc.Error as e:
    print("Error connecting to the database:", e)
    
    
    UPDATE Table 
    SET  Table.col1 = other_table.col1,
         Table.col2 = other_table.col2 
    --select Table.col1, other_table.col,Table.col2,other_table.col2, *   
    FROM     Table 
    INNER JOIN     other_table 
        ON     Table.id = other_table.id 

        
        UPDATE Table1
    INNER JOIN Table2
    ON Table1.id = Table2.id
    SET Table1.col1 = Table2.col1,
        Table1.col2 = Table2.col2
        UPDATE x
    SET    x.col1 = x.newCol1,
           x.col2 = x.newCol2
    FROM   (SELECT t.col1,
                   t2.col1 AS newCol1,
                   t.col2,
                   t2.col2 AS newCol2
            FROM   [table] t
                   JOIN other_table t2
                     ON t.ID = t2.ID) x

    """



    """UPDATE Table 
    SET  Table.col1 = other_table.col1,
         Table.col2 = other_table.col2 
    --select Table.col1, other_table.col,Table.col2,other_table.col2, *   
    FROM     Table 
    INNER JOIN     other_table 
        ON     Table.id = other_table.id 
        
        UPDATE Table1
    INNER JOIN Table2
    ON Table1.id = Table2.id
    SET Table1.col1 = Table2.col1,
        Table1.col2 = Table2.col2



    BEGIN TRY
            DECLARE @age INT = 160
            INSERT INTO TEST_TABLE VALUES ('QZ_TEST', @age + 1)
            INSERT INTO TEST_TABLE VALUES ('QZ_TEST', 'not a number')
            INSERT INTO ANOTHER_TABLE VALUES ('QZ_TEST', @age + 2)
    END TRY
        BEGIN CATCH
            DECLARE @ErrorMessage NVARCHAR(4000)
            DECLARE @ErrorSeverity INT
            DECLARE @ErrorState INT

            SELECT 
                @ErrorMessage = ERROR_MESSAGE(),
                @ErrorSeverity = ERROR_SEVERITY(),
                @ErrorState = ERROR_STATE()

            RAISERROR (@ErrorMessage,
                       @ErrorSeverity, -- Level 16
                       @ErrorState
                       )
        END CATCH



    import pandas as pd
    import pyodbc
    from fast_to_sql import fast_to_sql as fts

    # Test Dataframe for insertion
    df = pd.DataFrame(your_dataframe_here)

    # Create a pyodbc connection
    conn = pyodbc.connect(
        """
        Driver={ODBC Driver 17 for SQL Server};
        Server=localhost;
        Database=my_database;
        UID=my_user;
        PWD=my_pass;
        """
    )

    # If a table is created, the generated sql is returned
    create_statement = fts.fast_to_sql(df, "my_great_table", conn, if_exists="replace")

    # Commit upload actions and close connection
    conn.commit()
    conn.close()

    def SaveData(self, aScrapeResult):
        sql = "EXECUTE mc.SaveFundamentalDataCSV @pSource='%s',@pCountry='%s',@pOperator='%s',@pFromCountry='%s',@pFromOperator='%s',@pToCountry='%s',@pToOperator='%s',@pSiteName='%s',@pFactor='%s',@pGranularity='%s',@pDescription='%s',@pDataType='%s',@pTechnology = '%s',@pcsvData='%s'"

        #   Need to convert the data into CSV
        util = ListToCsvUtil()
        csvValues = util.ListToCsv(aScrapeResult.DataPoints)

        formattedSQL = sql % (aScrapeResult.Source ,aScrapeResult.Country,aScrapeResult.Operator ,aScrapeResult.FromCountry ,aScrapeResult.FromOperator ,aScrapeResult.ToCountry ,aScrapeResult.ToOperator ,aScrapeResult.SiteName ,aScrapeResult.Factor ,aScrapeResult.Granularity ,aScrapeResult.Description ,aScrapeResult.DataType ,aScrapeResult.Technology ,csvValues)

        DB = create_engine(self.ConnectionString)
        DB.connect()

        result_proxy = DB.execute(formattedSQL)

        results = result_proxy.fetchall()
        
        import pyodbc

    DATABASES = {
        'default': {
            'ENGINE': 'sql_server.pyodbc',
            'NAME': 'db1',
            'USER': 'admin02',
            'PASSWORD': *****,  
            'HOST': '<host_name>.database.windows.net',
            'PORT': '1433',
            'OPTIONS': {
                'driver': 'ODBC Driver 18 for SQL Server',
            },
        },
    }



    db_settings = DATABASES['default']

    try:
        conn = pyodbc.connect(
            driver=db_settings['OPTIONS']['driver'],
            server=db_settings['HOST'],
            database=db_settings['NAME'],
            user=db_settings['USER'],
            password=db_settings['PASSWORD'],
            port=db_settings['PORT'],
        )

        print("Connected to the database successfully!")

        cursor = conn.cursor()
        cursor.execute("SELECT * FROM Table1")
        rows = cursor.fetchall()
        for row in rows:
            print(row)
            

        cursor.close()
        conn.close()

    except pyodbc.Error as e:
        print("Error connecting to the database:", e)
        

        UPDATE Table 
        SET  Table.col1 = other_table.col1,
             Table.col2 = other_table.col2 
        --select Table.col1, other_table.col,Table.col2,other_table.col2, *   
        FROM     Table 
        INNER JOIN     other_table 
            ON     Table.id = other_table.id 
            
            UPDATE Table1
        INNER JOIN Table2
        ON Table1.id = Table2.id
        SET Table1.col1 = Table2.col1,
            Table1.col2 = Table2.col2



        BEGIN TRY
                DECLARE @age INT = 160
                INSERT INTO TEST_TABLE VALUES ('QZ_TEST', @age + 1)
                INSERT INTO TEST_TABLE VALUES ('QZ_TEST', 'not a number')
                INSERT INTO ANOTHER_TABLE VALUES ('QZ_TEST', @age + 2)
        END TRY
            BEGIN CATCH
                DECLARE @ErrorMessage NVARCHAR(4000)
                DECLARE @ErrorSeverity INT
                DECLARE @ErrorState INT

                SELECT 
                    @ErrorMessage = ERROR_MESSAGE(),
                    @ErrorSeverity = ERROR_SEVERITY(),
                    @ErrorState = ERROR_STATE()

                RAISERROR (@ErrorMessage,
                           @ErrorSeverity, -- Level 16
                           @ErrorState
                           )
            END CATCH
            
            UPDATE Table 
            SET  Table.col1 = other_table.col1,
                 Table.col2 = other_table.col2 
            --select Table.col1, other_table.col,Table.col2,other_table.col2, *   
            FROM     Table 
            INNER JOIN     other_table 
                ON     Table.id = other_table.id 

                
                UPDATE Table1
            INNER JOIN Table2
            ON Table1.id = Table2.id
            SET Table1.col1 = Table2.col1,
                Table1.col2 = Table2.col2
                
                UPDATE x
            SET    x.col1 = x.newCol1,
                   x.col2 = x.newCol2
            FROM   (SELECT t.col1,
                           t2.col1 AS newCol1,
                           t.col2,
                           t2.col2 AS newCol2
                    FROM   [table] t
                           JOIN other_table t2
                             ON t.ID = t2.ID) x
            """



            """UPDATE Table 
            SET  Table.col1 = other_table.col1,
                 Table.col2 = other_table.col2 
            --select Table.col1, other_table.col,Table.col2,other_table.col2, *   
            FROM     Table 
            INNER JOIN     other_table 
                ON     Table.id = other_table.id 
                
                UPDATE Table1
            INNER JOIN Table2
            ON Table1.id = Table2.id
            SET Table1.col1 = Table2.col1,
                Table1.col2 = Table2.col2



            BEGIN TRY
                    DECLARE @age INT = 160
                    INSERT INTO TEST_TABLE VALUES ('QZ_TEST', @age + 1)
                    INSERT INTO TEST_TABLE VALUES ('QZ_TEST', 'not a number')
                    INSERT INTO ANOTHER_TABLE VALUES ('QZ_TEST', @age + 2)
            END TRY
                BEGIN CATCH
                    DECLARE @ErrorMessage NVARCHAR(4000)
                    DECLARE @ErrorSeverity INT
                    DECLARE @ErrorState INT

                    SELECT 
                        @ErrorMessage = ERROR_MESSAGE(),
                        @ErrorSeverity = ERROR_SEVERITY(),
                        @ErrorState = ERROR_STATE()

                    RAISERROR (@ErrorMessage,
                               @ErrorSeverity, -- Level 16
                               @ErrorState
                               )
                END CATCH



            import pandas as pd
            import pyodbc
            from fast_to_sql import fast_to_sql as fts

            # Test Dataframe for insertion
            df = pd.DataFrame(your_dataframe_here)

            # Create a pyodbc connection
            conn = pyodbc.connect(
                """
                Driver={ODBC Driver 17 for SQL Server};
                Server=localhost;
                Database=my_database;
                UID=my_user;
                PWD=my_pass;
                """
            )

            # If a table is created, the generated sql is returned
            create_statement = fts.fast_to_sql(df, "my_great_table", conn, if_exists="replace")

            # Commit upload actions and close connection
            conn.commit()
            conn.close()

            def SaveData(self, aScrapeResult):
                sql = "EXECUTE mc.SaveFundamentalDataCSV @pSource='%s',@pCountry='%s',@pOperator='%s',@pFromCountry='%s',@pFromOperator='%s',@pToCountry='%s',@pToOperator='%s',@pSiteName='%s',@pFactor='%s',@pGranularity='%s',@pDescription='%s',@pDataType='%s',@pTechnology = '%s',@pcsvData='%s'"

                #   Need to convert the data into CSV
                util = ListToCsvUtil()
                csvValues = util.ListToCsv(aScrapeResult.DataPoints)

                formattedSQL = sql % (aScrapeResult.Source ,aScrapeResult.Country,aScrapeResult.Operator ,aScrapeResult.FromCountry ,aScrapeResult.FromOperator ,aScrapeResult.ToCountry ,aScrapeResult.ToOperator ,aScrapeResult.SiteName ,aScrapeResult.Factor ,aScrapeResult.Granularity ,aScrapeResult.Description ,aScrapeResult.DataType ,aScrapeResult.Technology ,csvValues)

                DB = create_engine(self.ConnectionString)
                DB.connect()

                result_proxy = DB.execute(formattedSQL)

                results = result_proxy.fetchall()
                
                import pyodbc
                
                

            DATABASES = {
                'default': {
                    'ENGINE': 'sql_server.pyodbc',
                    'NAME': 'db1',
                    'USER': 'admin02',
                    'PASSWORD': *****,  
                    'HOST': '<host_name>.database.windows.net',
                    'PORT': '1433',
                    'OPTIONS': {
                        'driver': 'ODBC Driver 18 for SQL Server',
                    },
                },
            }



            db_settings = DATABASES['default']

            try:
                conn = pyodbc.connect(
                    driver=db_settings['OPTIONS']['driver'],
                    server=db_settings['HOST'],
                    database=db_settings['NAME'],
                    user=db_settings['USER'],
                    password=db_settings['PASSWORD'],
                    port=db_settings['PORT'],
                )

                print("Connected to the database successfully!")

                cursor = conn.cursor()
                cursor.execute("SELECT * FROM Table1")
                rows = cursor.fetchall()
                for row in rows:
                    print(row)
                cursor.close()
                conn.close()

            except pyodbc.Error as e:
                print("Error connecting to the database:", e)
                
                UPDATE Table 
                SET  Table.col1 = other_table.col1,
                     Table.col2 = other_table.col2 
                --select Table.col1, other_table.col,Table.col2,other_table.col2, *   
                FROM     Table 
                INNER JOIN     other_table 
                    ON     Table.id = other_table.id 
                    
                    UPDATE Table1
                INNER JOIN Table2
                ON Table1.id = Table2.id
                SET Table1.col1 = Table2.col1,
                    Table1.col2 = Table2.col2



                BEGIN TRY
                        DECLARE @age INT = 160
                        INSERT INTO TEST_TABLE VALUES ('QZ_TEST', @age + 1)
                        INSERT INTO TEST_TABLE VALUES ('QZ_TEST', 'not a number')
                        INSERT INTO ANOTHER_TABLE VALUES ('QZ_TEST', @age + 2)
                END TRY
                    BEGIN CATCH
                        DECLARE @ErrorMessage NVARCHAR(4000)
                        DECLARE @ErrorSeverity INT
                        DECLARE @ErrorState INT

                        SELECT 
                            @ErrorMessage = ERROR_MESSAGE(),
                            @ErrorSeverity = ERROR_SEVERITY(),
                            @ErrorState = ERROR_STATE()

                        RAISERROR (@ErrorMessage,
                                   @ErrorSeverity, -- Level 16
                                   @ErrorState
                                   )
                    END CATCH

    import pyodbc

    DATABASES = {
        'default': {
            'ENGINE': 'sql_server.pyodbc',
            'NAME': 'db1',
            'USER': 'admin02',
            'PASSWORD': *****,  
            'HOST': '<host_name>.database.windows.net',
            'PORT': '1433',
            'OPTIONS': {
                'driver': 'ODBC Driver 18 for SQL Server',
            },
        },
    }

    db_settings = DATABASES['default']

    try:
        conn = pyodbc.connect(
            driver=db_settings['OPTIONS']['driver'],
            server=db_settings['HOST'],
            database=db_settings['NAME'],
            user=db_settings['USER'],
            password=db_settings['PASSWORD'],
            port=db_settings['PORT'],
        )

        print("Connected to the database successfully!")

        cursor = conn.cursor()
        cursor.execute("SELECT * FROM Table1")
        rows = cursor.fetchall()
        for row in rows:
            print(row)
            
        connect_str = "Driver={ODBC Driver 17 for SQL Server};" + \
        "Server={server},1433;".format(server='tcp:ipaddress.database.windows.net') + \
        "Database={database};".format(database='mydb') + \
        "uid={uid};".format(uid='myuserid') + \
        "pwd={pwd};".format(pwd='secretpswd') + \
        "Encrypt=yes;TrustServerCertificate=no;"
         cnxn = pyodbc.connect(connect_str)

        cursor.close()
        conn.close()

        except pyodbc.Error as e:
        print("Error connecting to the database:", e)
        
        connect_str = "Driver={ODBC Driver 17 for SQL Server};" + \
        "Server={server},1433;".format(server='tcp:ipaddress.database.windows.net') + \
        "Database={database};".format(database='mydb') + \
        "uid={uid};".format(uid='myuserid') + \
        "pwd={pwd};".format(pwd='secretpswd') + \
        "Encrypt=yes;TrustServerCertificate=no;"
         cnxn = pyodbc.connect(connect_str)

        cursor.close()
        conn.close()

        except pyodbc.Error as e:
        print("Error connecting to the database:", e)


                BEGIN TRY
                        DECLARE @age INT = 160
                        INSERT INTO TEST_TABLE VALUES ('QZ_TEST', @age + 1)
                        INSERT INTO TEST_TABLE VALUES ('QZ_TEST', 'not a number')
                        INSERT INTO ANOTHER_TABLE VALUES ('QZ_TEST', @age + 2)
                END TRY
                    BEGIN CATCH
                        DECLARE @ErrorMessage NVARCHAR(4000)
                        DECLARE @ErrorSeverity INT
                        DECLARE @ErrorState INT

                        SELECT 
                            @ErrorMessage = ERROR_MESSAGE(),
                            @ErrorSeverity = ERROR_SEVERITY(),
                            @ErrorState = ERROR_STATE()

                        RAISERROR (@ErrorMessage,
                                   @ErrorSeverity, -- Level 16
                                   @ErrorState
                                   )
                    END CATCH

    import pyodbc

    DATABASES = {
        'default': {
            'ENGINE': 'sql_server.pyodbc',
            'NAME': 'db1',
            'USER': 'admin02',
            'PASSWORD': *****,  
            'HOST': '<host_name>.database.windows.net',
            'PORT': '1433',
            'OPTIONS': {
                'driver': 'ODBC Driver 18 for SQL Server',
            },
        },
    }

    db_settings = DATABASES['default']

    try:
        conn = pyodbc.connect(
            driver=db_settings['OPTIONS']['driver'],
            server=db_settings['HOST'],
            database=db_settings['NAME'],
            user=db_settings['USER'],
            password=db_settings['PASSWORD'],
            port=db_settings['PORT'],
        )

        print("Connected to the database successfully!")

        cursor = conn.cursor()
        cursor.execute("SELECT * FROM Table1")
        rows = cursor.fetchall()
        for row in rows:
            print(row)
            
        connect_str = "Driver={ODBC Driver 17 for SQL Server};" + \
        "Server={server},1433;".format(server='tcp:ipaddress.database.windows.net') + \
        "Database={database};".format(database='mydb') + \
        "uid={uid};".format(uid='myuserid') + \
        "pwd={pwd};".format(pwd='secretpswd') + \
        "Encrypt=yes;TrustServerCertificate=no;"
         cnxn = pyodbc.connect(connect_str)

        cursor.close()
        conn.close()

        except pyodbc.Error as e:
        print("Error connecting to the database:", e)
        
        connect_str = "Driver={ODBC Driver 17 for SQL Server};" + \
        "Server={server},1433;".format(server='tcp:ipaddress.database.windows.net') + \
        "Database={database};".format(database='mydb') + \
        "uid={uid};".format(uid='myuserid') + \
        "pwd={pwd};".format(pwd='secretpswd') + \
        "Encrypt=yes;TrustServerCertificate=no;"
         cnxn = pyodbc.connect(connect_str)

        cursor.close()
        conn.close()

        except pyodbc.Error as e:
        print("Error connecting to the database:", e)

