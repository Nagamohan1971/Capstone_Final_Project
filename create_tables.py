import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """
    - Creates and connects to the nagamohan
    @return: cursor and connection to nagamohan
    """
    
    # connect to default database
    conn = psycopg2.connect("host=localhost dbname=postgres user=postgres password=Mudu#1977")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create nagamohan database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS nagamohan")
    cur.execute("CREATE DATABASE nagamohan WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to nagamohan database
    conn = psycopg2.connect("host=localhost dbname=nagamohan user=postgres password=Mudu#1977")
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    @param cur:
    @param conn:
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list.
    @param cur:
    @param conn:
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Drops (if exists) and Creates the nagamohan database. 
    
    - Establishes connection with the nagamohan database and gets
    cursor to it.  
    
    - Drops all the tables.  
    
    - Creates all tables needed. 
    
    - Finally, closes the connection. 
    """
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()