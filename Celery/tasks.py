import sys  
import os
import json
import threading

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from celery import Celery, chain  # noqa: E402

from app.scrapper.spiders.totalpagesscrapper import total_pages_scrapper
from app.scrapper.spiders import urlscrapingspider, loginspider  # noqa: E402
from app.logging_config import scrapper_logger, scrapper_error_logger, shopify_syncing_logger   # noqa: E402
from app.database import DBManager  # noqa: E402
from app.scrapper.crawl import run_spider  # noqa: E402
from app.shopify_sync.shopify_sync import ShopifySync # noqa: E402
from app.split_shipping.shopify_orders import ShopifyOrders
from app.split_shipping.richner_order_creation import RichnerOrderCreation
from dotenv import load_dotenv

from .utils import get_url_spider_kwargs, get_product_spider_kwargs # noqa: E402
from .utils import fetch_product_page_urls, create_job, get_chunk_from_job  # noqa: E402

load_dotenv()

URL_CHUNK_SIZE = 15
CHUNK_SIZE = 1000
SHOPIFY_CHUNK_SIZE = 100

PROCESS_URL_JOB = True
PROCESS_PRODUCT_JOB = False

job_lock = threading.Lock()

app = Celery(
  'tasks',
  broker='redis://localhost:6379/0',
  timezone='Asia/Karachi',
)
app.conf.worker_concurrency = 12
app.conf.worker_prefetch_multiplier = 1
app.autodiscover_tasks()

@app.task
def scrapper_main_task():
  db_manager = DBManager()
  kwargs = get_url_spider_kwargs()
  total_pages = total_pages_scrapper(kwargs['start_url'])

  try:
    if total_pages > 0:
      chunk_count = total_pages / URL_CHUNK_SIZE
      diff = int(chunk_count) - chunk_count

      if diff < 0:
        chunk_count = int(chunk_count) + 1
      else:
        chunk_count = int(chunk_count)

      for i in range(0, chunk_count):
        index_start = i * URL_CHUNK_SIZE + 1
        index_end = index_start + URL_CHUNK_SIZE - 1

        if index_end > total_pages:
          index_end = total_pages

        url_job_data = {
          'name': 'url_scrapper',
          'completed': False,
          'meta': json.dumps({
            'next_url': None,
            'start_index': index_start,
            'end_index': index_end
          }),
          'retry': 0
        }

        db_manager.execute_insert('jobs', url_job_data)
        
        query = "SELECT job_id FROM jobs ORDER BY job_id DESC LIMIT 1"
        result = db_manager.execute_query(query)
        if result:
          urls_job_id = result[0]
        else:
          pass

        urls_scrpper = scrap_products_urls.s(urls_job_id)
        products_scrapper = scrap_products_details.s()
        chain(urls_scrpper | products_scrapper).apply_async()
      db_manager.close_connection()

  except Exception as err:
    scrapper_error_logger.error(f'error: {err}')
    db_manager.close_connection()

@app.task(bind=True)
def scrap_products_urls(self, url_job_id):
  db_manager = DBManager() 
  scrapper_logger.info(f"Spider started with job id {url_job_id}: {urlscrapingspider.UrlScrapingSpider.name}")

  try:
    kwargs = get_url_spider_kwargs()
    kwargs['url_job_id'] = url_job_id

    criteria = "job_id = %s"
    job = db_manager.select_from_table('jobs',criteria, (url_job_id,) )

  
    meta = job[3] 
    kwargs['start_index'] = meta['start_index']
    kwargs['end_index'] = meta['end_index']

    kwargs['start_url'] = fetch_current_job_start_url(kwargs['start_index'])

    if job and meta['next_url']:
      kwargs['start_url'] = meta['next_url']

    if kwargs is not None and kwargs['start_url'] is not None:
      run_spider(
        urlscrapingspider.UrlScrapingSpider,
        kwargs
      )

    criteria = "job_id = %s"
    job = db_manager.select_from_table('jobs', criteria, (url_job_id,))

    if job and job[3]['next_url']:
      raise Exception('job not completed')
    else:
      query = "Update jobs SET completed = %s where job_id = %s"
      db_manager.execute_query(query, (True, url_job_id), None)
    
  except Exception as err:
    criteria = "job_id = %s"
    job = db_manager.select_from_table('jobs',criteria, (url_job_id,))

    retry = int(job[5]) + 1
    scrapper_error_logger.error(f"Error Occurred while scrapping product urls job having id {url_job_id}: {err}")      
    
    if retry < 4:
      query = "UPDATE jobs SET retry = %s, message = %s WHERE job_id = %s;"
      db_manager.execute_query(query, (retry, f"error occurred: {err}", url_job_id,), None)
      raise self.retry(countdown=10, max_retries=3)

  db_manager.close_connection()
  return {'job_id': url_job_id, 'result': 'URLs scraped successfully'}

def fetch_current_job_start_url(start_index):
  return os.getenv('SHOP_URL')

@app.task
def scrap_products_details(result):
  db_manager = DBManager()
  products_urls = []
  url_job_id = result.get('job_id')
  
  if url_job_id:
    products_urls = fetch_product_page_urls(db_manager, url_job_id)
  
  if len(products_urls) > 0: 
    chunk_count = len(products_urls) / CHUNK_SIZE

    diff = int(chunk_count) - chunk_count

    if diff < 0:
      chunk_count = int(chunk_count) + 1
    else:
      chunk_count = int(chunk_count)

    initial_len = len(products_urls)

    for i in range(0, chunk_count):
      index_start = i * CHUNK_SIZE
      index_end = index_start + CHUNK_SIZE - 1

      if index_end > initial_len:
        index_end = initial_len

      current_chunk = products_urls[index_start: index_end]
      job_id = create_job(current_chunk, 'product_detail_scrapper')
      products_scrapper_spider.s(job_id).apply_async()
  else:
    db_manager.close_connection()
    return { 'result': 'No product urls data from chain process'}
  
  db_manager.close_connection()

@app.task(bind=True)
def products_scrapper_spider(self, job_id):  
  scrapper_logger.info(f"Individual Products detials srapping Spider started with job id {job_id} and spider name: {loginspider.LoginSpider.name}")
  db_manager = DBManager()

  criteria = "job_id = %s"
  job = db_manager.select_from_table('jobs', criteria, (job_id,) )

  try:
    kwargs = get_product_spider_kwargs(job[3]['processing_urls'])
    kwargs['job_id'] = job_id

    if kwargs is not None:
      run_spider(loginspider.LoginSpider, kwargs)

      criteria = "job_id = %s"
      job = db_manager.select_from_table('jobs',criteria, (job_id,) )

      if len(job[3]['processing_urls'] or []) <= 0:
        query = "UPDATE jobs SET completed = %s  WHERE job_id = %s;"
        db_manager.execute_query(query, (True, job_id,), None)
      else:
        raise Exception("Error while scrapping products details data")
  except Exception as err:
    criteria = "job_id = %s"
    job = db_manager.select_from_table('jobs',criteria, (job_id,))
    retry = int(job[5]) + 1
    scrapper_error_logger.error(f"Error Occurred while scrapping product details job having id {job_id}: {err}")

    if retry < 4:
      query = "UPDATE jobs SET retry = %s, message = %s WHERE job_id = %s;"
      db_manager.execute_query(query, (retry, f"Error occurred: {err}", job_id,), None)
      db_manager.close_connection()
      raise self.retry(countdown=10, max_retries=3)

  db_manager.close_connection()

@app.task
def sync_products_to_shopify():
  db_manager = DBManager()
  criteria = "product_title is not Null"
  records = db_manager.select_from_table('products', criteria, None, True)

  chunk_count = len(records) / SHOPIFY_CHUNK_SIZE
  diff = int(chunk_count) - chunk_count
  if diff < 0:
    chunk_count = int(chunk_count) + 1
  else:
    chunk_count = int(chunk_count)

  initial_len = len(records)

  for i in range(0, chunk_count):
    index_start = i * SHOPIFY_CHUNK_SIZE
    index_end = index_start + SHOPIFY_CHUNK_SIZE - 1

    if i == chunk_count - 1:  
      if index_end > initial_len:
        index_end = initial_len    
      else:
        index_end += 1

    current_chunk = records[index_start: index_end]

    job_id = create_job(current_chunk, 'shopify_sync_job')
  
  shopify_sync_job.s(job_id).apply_async()
  
  db_manager.close_connection()

@app.task(bind=True)
def shopify_sync_job(self, job_id):
  db_manager = DBManager()
  current_chunk = get_chunk_from_job(job_id)

  try:
    shopify_syncing_job(current_chunk, job_id)

    criteria = "job_id = %s"

    job = db_manager.select_from_table('jobs',criteria, (job_id,))
    
    if not job[3]['processing_urls'] or len(job[3]['processing_urls'] or []) <= 0:
      query = "UPDATE jobs SET completed = %s  WHERE job_id = %s;"
      db_manager.execute_query(query, (True, job_id,), None)
    else:
      raise Exception("Error while syncing products")
  except Exception as e:
    criteria = "job_id = %s"
    job = db_manager.select_from_table('jobs',criteria, (job_id,))

    retry = int(job[5]) + 1
    shopify_syncing_logger.error(f"Error Occurred while syncing product details job having id {job_id}: {e}")

    if retry < 4:
      query = "UPDATE jobs SET retry = %s WHERE job_id = %s;"
      db_manager.execute_query(query, (retry, job_id,), None)
      db_manager.close_connection()
      raise self.retry(countdown=5, max_retries=3)  

  db_manager.close_connection()  

@app.task
def shopify_syncing_job(chunk, job_id):
  sync_job = ShopifySync(chunk, job_id, job_lock)
  sync_job.sync_products()

@app.task
def split_shipping_job():
  shopify_order = ShopifyOrders()
  shopify_order.fetch_new_orders()

  richner_order_creator = RichnerOrderCreation()
  richner_order_creator.create_orders_on_richner()
