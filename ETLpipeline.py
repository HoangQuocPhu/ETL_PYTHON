# Import libraries required for connecting to mysql
import mysql.connector

# Import libraries required for connecting to PostgreSql
import psycopg2

# Connect to MySQL
connection1 = mysql.connector.connect(user='root', password='1',host='127.0.0.1',database='sales')
cursor1 = connection1.cursor()

# Connect to PostgreSql
dsn_hostname = '127.0.0.1'
dsn_user='postgres'        
dsn_pwd ='1'      
dsn_port ="5432"               
dsn_database ="sales" 
connection2=psycopg2.connect(
            database=dsn_database, 
            user=dsn_user,
            password=dsn_pwd,
            host=dsn_hostname, 
            port= dsn_port
        )
cursor2 = connection2.cursor()
# Find out the last rowid from PostgreSql data warehouse
# The function get_last_rowid must return the last rowid of the table sales_data on PostgreSql.

def get_last_rowid():
    # Query to get the last rowid
    query = "SELECT MAX(rowid) FROM sales_data;"
    cursor2.execute(query)

    # Fetch one record
    last_rowid = cursor2.fetchone()[0]
    return last_rowid
    print("Error while connecting to PostgreSQL", error)


last_row_id = get_last_rowid()
print("Last row id on production datawarehouse = ", last_row_id)

# List out all records in MySQL database with rowid greater than the one on the Data warehouse
# The function get_latest_records must return a list of all records that have a rowid greater than the last_row_id in the sales_data table in the sales database on the MySQL staging data warehouse.

def get_latest_records(rowid):
    query = "SELECT * FROM sales_data WHERE rowid > %s"
    cursor1.execute(query, (rowid,))
    records = cursor1.fetchall()
    return records

    
new_records = get_latest_records(last_row_id)

print("New rows on staging datawarehouse = ", len(new_records))

# Insert the additional records from MySQL into DPostgreSql data warehouse.
# The function insert_records must insert all the records passed to it into the sales_data table in PostgreSql.

def insert_records(records):
    try:
        insert_query = "INSERT INTO sales_data (rowid, product_id, customer_id, quantity) VALUES (%s, %s, %s, %s)"
        for record in records:
            cursor2.execute(insert_query, record)
        connection2.commit()
    except psycopg2.Error as e:
        print("Error while inserting into PostgreSQL", e)
        connection2.rollback()
insert_records(new_records)
print("New rows inserted into production data warehouse =", len(new_records))
try:
    cursor1.close()
    connection1.close()
    cursor2.close()
    connection2.close()
except Exception as e:
    print("Error closing connections", e)

print("End of program")
