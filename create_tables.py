import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():

    """
    First we establish a connection to the defulte database, Then we create a new database.

    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=postgres password=postgres")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    conn.close()    
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=postgres password=postgres")
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    """

    This method will drop all tables 
    we will call it before the creation of tables.
    Then it will commit after each drop table.

    ** I took the sql statement from the sql_queries.py file **
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):

    """
    This method will create all tables
    we wil call it after the execution of the drop_tables method.
    Then it will commit after each drop table.

    ** I took the sql statement from the sql_queries.py file **
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()