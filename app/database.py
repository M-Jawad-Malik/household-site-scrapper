import os
import psycopg2
from psycopg2 import pool
from dotenv import load_dotenv


from app.logging_config import database_logger

load_dotenv()

class DBManager:
  def __init__(self):
    self.connection_pool = pool.ThreadedConnectionPool(
      1,
      5,
      host= os.getenv('HOST'),
      dbname= os.getenv('DATABASE'),
      user= os.getenv('DATABASE_USER'),
      password= os.getenv('DATABASE_PASSWORD')
    )
    
    try:
      self.create_products_table()
      self.create_jobs_table()
      self.create_collections_table()
      self.create_orders_table()
      self.create_line_items_table()
      self.create_variants_table()
      self.add_image_to_products()
    except Exception as e:
      database_logger.error(e)
  
  def create_collections_table(self):
    conn = self.connection_pool.getconn()
    cursor = conn.cursor()
    cursor.execute("""
      CREATE TABLE IF NOT EXISTS collections(
        id serial PRIMARY KEY,
        name text,
        shopify_collection_id text
      )
    """)
    conn.commit()
    cursor.close()
    self.connection_pool.putconn(conn)

  def add_image_to_products(self):
    conn = self.connection_pool.getconn()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_name = 'products' AND column_name = 'image_url'
            );
        """)
        column_exists = cursor.fetchone()[0]

        if not column_exists:
            cursor.execute("ALTER TABLE products ADD COLUMN image_url text;")
            conn.commit()
            print("Column 'image_url' added to table 'products' successfully.")
        else:
            print("Column 'image_url' already exists in table 'products'. Skipping addition.")

    except psycopg2.Error as e:
        conn.rollback()
        print("Error adding column:", e)
    finally:
        cursor.close()
        self.connection_pool.putconn(conn)

  def create_variants_table(self):
    conn = self.connection_pool.getconn()
    cursor = conn.cursor()
    cursor.execute("""
      CREATE TABLE IF NOT EXISTS variants(
        id serial PRIMARY KEY,
        sku BIGINT,
        variant_shopify_id BIGINT
      )
    """)
    conn.commit()
    cursor.close()
    self.connection_pool.putconn(conn)
  
  def create_products_table(self):
    conn = self.connection_pool.getconn()
    cursor = conn.cursor()
    cursor.execute("""
      CREATE TABLE IF NOT EXISTS products(
        id serial PRIMARY KEY, 
        page_url text,
        product_title text,
        tag text,
        sku text,
        brand text,
        public_price text,
        supplier_price text,
        variants jsonb,
        availability text,
        important_technical_details json,
        product_material_details text[],
        technical_details json,
        category text,
        sub_category text,
        job_id integer,
        created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        synced_at TIMESTAMP WITH TIME ZONE DEFAULT NULL,
        shopify_id text
      )
    """)
    conn.commit()
    cursor.close()
    self.connection_pool.putconn(conn)

  def create_jobs_table(self):
    conn = self.connection_pool.getconn()
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS jobs(
      job_id serial PRIMARY KEY,
      name text,
      completed boolean DEFAULT FALSE,
      meta json,
      message text,
      retry integer,
      created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """)
    conn.commit()
    cursor.close()
    self.connection_pool.putconn(conn)
  
  def create_orders_table(self):
    conn = self.connection_pool.getconn()
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS orders(
      order_id bigint PRIMARY KEY,
      order_splitted boolean DEFAULT FALSE,
      first_name text,
      last_name text,
      email text,
      address1 text,
      address2 text,
      phone text,
      company text,
      city text,
      province text,
      country text,
      zip text,
      created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """)
    conn.commit()
    cursor.close()
    self.connection_pool.putconn(conn)
  
  def create_line_items_table(self):
    conn = self.connection_pool.getconn()
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS line_items(
      order_id bigint,
      sku text,
      quantity integer,
      created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """)
    conn.commit()
    cursor.close()
    self.connection_pool.putconn(conn)

  def execute_query(self, query, parameters=None, fetch_all=False):
    conn = self.connection_pool.getconn()
    cursor = conn.cursor()
    """
      Execute a generic query on the database.

      :param query: The SQL query string.
      :param parameters: Optional parameters to be substituted into the query.
      :param fetch_all: If True, fetch all rows. If False, fetch one row.
      :return: Fetched result(s) or None if no result.
    """
    cursor.execute(query, parameters)
    
    if fetch_all is None:
      conn.commit()
      cursor.close()
      self.connection_pool.putconn(conn)
    elif fetch_all is True:
      records = cursor.fetchall()
      cursor.close()
      self.connection_pool.putconn(conn)
      return records
    else:
      record = cursor.fetchone()
      cursor.close()
      self.connection_pool.putconn(conn)
      return record

  def execute_insert(self, table_name, data):
    insert_query = f"INSERT INTO {table_name} ({', '.join(data.keys())}) VALUES ({', '.join(['%s'] * len(data))})"

    insert_parameters = list(data.values())
    self.execute_query(insert_query, parameters=insert_parameters, fetch_all=None)

  def execute_update(self, table_name, data, unique_column):
    update_query = f"UPDATE {table_name} SET {', '.join([f'{key} = %s' for key in data.keys()])} WHERE {unique_column} = %s"
    update_parameters = [data[key] for key in data.keys()] + [data[unique_column]]
    self.execute_query(update_query, parameters=update_parameters, fetch_all=None)

  def select_from_table(self, table_name, criteria, parameters=None, fetch_all=False):
    query = f"SELECT * FROM {table_name} WHERE {criteria}"
    record =  self.execute_query(query, parameters, fetch_all)

    return record
  
  def close_connection(self):
    self.connection_pool.closeall()
      
      
  