import os
import json

from app.database import DBManager
from main import current_supplier  # noqa: E402

def urls_args_dict(arr):
  return {
    "supplier": arr[0] or  "",
    "allowed_domain": arr[1] or "",
    "start_url": arr[2] or "",
    "products_container": arr[3] or "",
    "host": arr[4] or "",
    "product_path_tag": arr[5] or "",
    "product_availability_tag": arr[6] or "",
    "next_page_url_tag": arr[7] or "",
    "last_page_tag": arr[8] or ""
  }
  
def login_args_dict(arr):
  return {
    "supplier": arr[0] or  "",
    "product_urls": arr[1] or "",
    "allowed_domain": arr[2] or "",
    "start_url": arr[3] or "",
    "username": arr[4] or "",
    "password": arr[5] or "",
    "login_btn_tag": arr[6] or "",
    "login_confirmation_tag": arr[7] or ""
  }

def fetch_last_scrapper_job():
  db_manager = DBManager()
  query = "SELECT * from jobs ORDER BY job_id DESC LIMIT 1"
  result = db_manager.execute_query(query, None, False)
  db_manager.close_connection()
  return result

def fetch_job_data(job_id):
  db_manager = DBManager()
  query = "SELECT * from jobs where job_id = %s ORDER BY job_id DESC LIMIT 1"
  result =  db_manager.execute_query(query, (job_id,), False)
  db_manager.close_connection()
  return result

def get_url_spider_kwargs():
  args_arr = []

  if current_supplier == "Richner":
    args_arr = [  
      "Richner",
      "shop.bmsuisse.ch",
      os.getenv('SHOP_URL'),
      "div.product-item--gallery-view",
      "https://shop.bmsuisse.ch",
      "a.product-item__preview ::attr(href)",
      ".shopping-list-line-items__status ::text",
      "a.oro-pagination__next ::attr(href)",
      "a.remaining__next--btn.pagination__last > span ::text"
    ]
  
  if len(args_arr) > 0:
    return urls_args_dict(args_arr)

  return None

def get_product_spider_kwargs(urls):
  login_args_arr = []

  if current_supplier == "Richner":
    login_args_arr = [
      "Richner",
      urls,
      "shop.bmsuisse.ch",
      os.getenv('SITE_LOGIN_URL'),
      os.environ.get('RICHNER_USERNAME'),
      os.environ.get('RICHNER_PASSWORD'),
      ".btn--full-in-mobile.btn--ultra[type='submit']",
      ".main-menu__item"
    ]
  elif current_supplier == "Reuter":
    login_args_arr = [
      "Reuter",
      urls,
      "reuter-profishop.de",
      os.getenv('SITE_LOGIN_URL'),
      os.environ.get('REUTER_USERNAME'),
      os.environ.get('REUTER_PASSWORD'),
      ".button.expanded-for-small-only[type='submit']",
      ".show-for-large.c-breadcrumb__item"
    ]

  if len(login_args_arr) > 0:
    return login_args_dict(login_args_arr)

  return None

def fetch_product_page_urls(manager, job_id):
  query = "SELECT page_url FROM products where job_id = %s"
  urls = manager.execute_query(query, (job_id,), True)
  records = [r[0] for r in urls]

  return records

def create_job(current_chunk, name):
  db_manager = DBManager()
  
  if name == 'product_detail_scrapper':
    urls = current_chunk
  else:
    urls = [record[1] for record in current_chunk]
 
  last_job = fetch_last_scrapper_job()

  data = {
    'name': name,
    'completed': False,
    'meta': json.dumps({'processing_urls': urls}),
    'retry': 0
  }

  db_manager.execute_insert('jobs', data)
  
  query = "SELECT job_id FROM jobs ORDER BY job_id DESC LIMIT 1"
  result = db_manager.execute_query(query)
  if result:
    job_id = result[0]
  else:
    pass

  db_manager.close_connection()

  return job_id

def get_chunk_from_job(job_id):
  db_manager = DBManager()

  query = "Select * from jobs Where job_id = %s"
  job = db_manager.execute_query(query, (job_id,))
  urls = job[3]['processing_urls']

  query = "Select * from products WHERE page_url = ANY(Array[%s]);"
  products = db_manager.execute_query(query, (urls,), True)
  db_manager.close_connection()
  return products