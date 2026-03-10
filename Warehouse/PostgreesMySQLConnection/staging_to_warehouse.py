# Import libraries required for connecting to mysql
import mysql.connector

# Import libraries required for connecting to PostgreSql
import psycopg2

# Connect to MySQL
connection = mysql.connector.connect(user='root', password='BXQwlKEHysIcdqsiFt7l1hUa',host='172.21.49.69',database='sales')
# Connect to PostgreSql
# connectction details
dsn_hostname = '172.21.66.53'
dsn_user='postgres'        # e.g. "abc12345"
dsn_pwd ='H3DspHTFPagEC9YBqhgUGYAH'      # e.g. "7dBZ3wWt9XN6$o0J"
dsn_port ="5432"                # e.g. "50000" 
dsn_database ="postgres"           # i.e. "BLUDB"


# create connection

conn = psycopg2.connect(
   database=dsn_database, 
   user=dsn_user,
   password=dsn_pwd,
   host=dsn_hostname, 
   port= dsn_port
)

# Find out the last rowid from or PostgreSql data warehouse
# The function get_last_rowid must return the last rowid of the table sales_data on the PostgreSql.

def get_last_rowid():
    cursor = conn.cursor()
    SQL="SELECT MAX(rowid) FROM sales_data;"
    cursor.execute(SQL)
    result = cursor.fetchone()[0]
    cursor.close()
    return result


last_row_id = get_last_rowid()
print("Last row id on production datawarehouse = ", last_row_id)

# List out all records in MySQL database with rowid greater than the one on the Data warehouse
# The function get_latest_records must return a list of all records that have a rowid greater than the last_row_id in the sales_data table in the sales database on the MySQL staging data warehouse.

def get_latest_records(rowid):
    # query data
    cursor = connection.cursor()
    SQL = "SELECT rowid, product_id, customer_id, quantity FROM sales_data WHERE rowid > %s" 
    cursor.execute(SQL, (rowid,))
    results = cursor.fetchall()
    cursor.close()
    return results


new_records = get_latest_records(last_row_id)

print("New rows on staging datawarehouse = ", len(new_records))

# Insert the additional records from MySQL into PostgreSql data warehouse.
# The function insert_records must insert all the records passed to it into the sales_data table in or PostgreSql.

def insert_records(records):
    cursor = conn.cursor()
    SQL = """ INSERT INTO sales_data (rowid, product_id, customer_id, quantity) 
    VALUES (%s, %s, %s, %s) """
    cursor.executemany(SQL, records)
    conn.commit()
    cursor.close() 

insert_records(new_records)
print("New rows inserted into production datawarehouse = ", len(new_records))

# disconnect from mysql warehouse
connection.close()
# disconnect from PostgreSql data warehouse 
conn.close()

# End of program

