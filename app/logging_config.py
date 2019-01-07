import logging
from logging.handlers import RotatingFileHandler

def log_setup(project_name):
	# Configure logging format and level with RotatingFileHandler
	logger = logging.getLogger(project_name)
	logger.setLevel(logging.INFO)

	formatter = logging.Formatter(f'%(asctime)s - [{project_name}] - %(name)s - %(levelname)s - %(message)s')

	handler = RotatingFileHandler(f'{project_name}.log', maxBytes=1024*1024*10, backupCount=10)  # 10MB file size, maximum 10 files
	handler.setFormatter(formatter)

	logger.addHandler(handler)
	
	return logger

database_logger = log_setup('database_logger')
scrapper_logger = log_setup('scrapper_logger')
scrapper_error_logger = log_setup('scrapper_error_logger')
shopify_syncing_logger = log_setup('shopify_syncing_logger')
split_shipping_logger = log_setup('split_shipping_logger')
