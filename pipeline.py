import mysql.connector
from mysql.connector import errorcode
from mysql.connector import Error
import pandas as pd

def get_db_connection():
    """
    2.1 Setup database connection
    In order to make a query against the database table, we need to first connect to it. A connection
    can be established only when the user provides the proper target host, port, and user
    credentials
    """
    connection = None
    try:
        connection = mysql.connector.connect(user='root',password='',host='127.0.0.1',port='3306')
    except Exception as error:
        print("Error while connecting to database for job tracker", error)
    return connection

def initialize_database():
    cnx = get_db_connection()
    cursor = cnx.cursor()
    try:
        cursor.execute("DROP DATABASE {}".format(DB_NAME))
        cursor.execute("CREATE DATABASE {}".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))
        exit(1)
    try:
        cursor.execute("USE {}".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Database {} does not exists.".format(DB_NAME))
        if err.errno == errorcode.ER_BAD_DB_ERROR:
            create_database(cursor)
            print("Database {} created successfully.".format(DB_NAME))
            cnx.database = DB_NAME
        else:
            print(err)
            exit(1)

    TABLES = {}
    TABLES['sales'] = (
        "CREATE TABLE sales ("
        "  ticket_id INT NOT NULL,"
        "  trans_date DATE NOT NULL,"
        "  event_id INT NOT NULL,"
        "  event_name VARCHAR(50) NOT NULL,"
        "  event_date DATE NOT NULL,"
        "  event_type VARCHAR(10) NOT NULL,"
        "  event_city VARCHAR(20) NOT NULL,"
        "  customer_id INT NOT NULL,"
        "  price DECIMAL NOT NULL,"
        "  num_tickets  INT NOT NULL"
        ")")

    for table_name in TABLES:
        table_description = TABLES[table_name]
        try:
            print("Creating table {}: ".format(table_name), end='')
            cursor.execute(table_description)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("already exists.")
            else:
                print(err.msg)
        else:
            print("OK")

    return cnx

def load_third_party(connection, file_path_csv):
    """
    2.2 Load CSV to table
    You’ll find the third party vendor data in the CSV file provided to you. The CSV follows the
    schema of the table. You will need to use the Python connector to insert each record of the CSV
    file into the sales table.
    """
    try:
        cursor = connection.cursor()
        # [Iterate through the CSV file and execute insert statement]
        sdata = pd.read_csv(file_path_csv, index_col=False, delimiter = ',')
        #loop through the data frame
        for i,row in sdata.iterrows():
            #here %S means string values
            sql = "INSERT INTO pipeline.sales VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            cursor.execute(sql, tuple(row))
            print("Record inserted")
            # the connection is not auto committed by default, so we must commit to save our changes
            connection.commit()
    except Error as e:
        print("Error while connecting to MySQL", e)

    connection.commit()
    cursor.close()
    return

def query_popular_tickets(connection):
    """
    3.1 Query the table and get the selected records
    After the data is loaded into the table, you can use this data to provide recommendations to the
    user. For instance, recommending popular events by finding the most top-selling tickets for the
    past month.
    """
    # Get the most popular ticket in the past month
    sql_statement = "WITH top_three AS (SELECT event_name,sum(num_tickets) AS numsold FROM sales group by 1 order by 2 desc,1 asc LIMIT 3) SELECT concat(event_name,'(',numsold) AS top_events from top_three;"
    cursor = connection.cursor()
    cursor.execute(sql_statement)
    records = cursor.fetchall()
    cursor.close()
    return records

def display_the_result(res):
    """3.2 Display the result
    The records you just retrieved are formatted as a list of tuples. You need to convert the format to
    display the on-screen results in a more user-friendly format. Please use this as an example:
    Here are the most popular tickets in the past month:
        - The North American International Auto Show
        - Carlisle Ford Nationals
        - Washington Spirits vs Sky Blue FC
    """
    print('Here are the top three events by number of tickets sold:')
    for i in res:
        print('    -'+i[0]+' tix sold)')

if __name__ == "__main__":
    DB_NAME = 'pipeline'
    cnx = initialize_database()
    load_third_party(cnx,'/home/conner/DataPipelineMiniProject/third_party_sales_full.csv')
    result = query_popular_tickets(cnx)
    display_the_result(result)
