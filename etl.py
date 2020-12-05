import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    This procedure loads data(songs_data and log_data) from S3 to Redshift using SQL queries defined in sql_queries.py
    
    INPUT: 
    cur: cursor variable
    conn: connection variable
    """
    for query in copy_table_queries:
        print("Load table done in progress", query)
        cur.execute(query)
        conn.commit()
        #print("Load table done.")


def insert_tables(cur, conn):
    """
    This procedure inserts data into the data model using the SQL queries defined in sql_queries.py file
    
    INPUT:
    cur: cursor variable
    conn: connection variable
    """
    for query in insert_table_queries:
        print("For Query:", query)
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()