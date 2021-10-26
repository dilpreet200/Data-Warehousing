import configparser
import psycopg2 # postgresql adapter/connector for python programming language

"""" import mysql.connector

conn = mysql.connector.connect(user='scott', password='password',
                              host='127.0.0.1',
                              database='employees')
conn.close() """

from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query) # object "cur" used to call execute method from cursor class of psycopg2 module
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit() # This method sends a COMMIT statement to the postgresql server, committing the current transaction


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values())) # Creating a new database session and returns a new connection object into "conn"
    
    # value() to fetch values from dictionary
    
    cur = conn.cursor() # cursor position
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()