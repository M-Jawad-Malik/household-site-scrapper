from celery import Celery, chain, signals
from celery.schedules import crontab
from dotenv import load_dotenv
from app.logging_config import scrapper_logger

from .tasks import app
from .tasks import scrapper_main_task, sync_products_to_shopify, split_shipping_job

load_dotenv()

def run_worker(): 
  app.worker_main(argv=['worker', '--loglevel=info'])

def run_beat():
  app.start(argv=['beat', '-l', 'info'])

@signals.worker_ready.connect
def starter(sender, **kwargs):  
  # scrapper_main_task.s().apply_async()
  sync_products_to_shopify.s().apply_async()
  # sender.add_periodic_task(
  #   crontab(minute=25, hour=17), 
  # chain(scrapper_main_task.s() | sync_products_to_shopify.s()).apply_async()
  # )
  
