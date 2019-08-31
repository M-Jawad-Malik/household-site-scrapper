# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import psycopg2
import os
import json
from dotenv import load_dotenv
from itemadapter import ItemAdapter
from datetime import datetime, timezone

from .items import Job
from .supplier_product import SupplierProduct
from app.database import DBManager

load_dotenv()

class ScrapperPipeline:

	def __init__(self):
		self.db_manager = DBManager()

	def process_item(self, item, spider):
		adapter = ItemAdapter(item)

		if isinstance(item, SupplierProduct):
			self.process_product_item(adapter, spider)

		return item

	def process_product_item(self, adapter, spider):
		result = self.db_manager.execute_query(
				"select * from products where page_url = %s",
				(adapter['page_url'],)
		)

		if result:
			if len(adapter) > 1:
				result_item = {
					'product_title': result[2],
					'tag': result[3],
					'sku': result[4],
					'brand': result[5],
					'public_price': result[6],
					'supplier_price': result[7],
					'variants': result[8],
					'availability': result[9],
					'important_technical_details': result[10],
					'product_material_details': result[11],
					'technical_details': result[12],
					'category': result[13],
					'sub_category': result[14],
					'image_url': result[15],
				}

				update_item = False

				for key, value in result_item.items():
					if not adapter.get(key, None):
						next
					elif adapter[key] != value:
						update_item = True

				if update_item:
					if adapter.get('important_technical_details', None) and adapter.get('technical_details', None):
						adapter['important_technical_details'] = json.dumps(adapter.get('important_technical_details'))
						adapter['technical_details'] = json.dumps(adapter['technical_details'])
					
					if adapter.get('variants', None):
						adapter['variants'] = json.dumps(adapter['variants'])

					data = {
							"product_title": adapter.get('product_title', None),
							"sku": adapter.get('sku', None),
							"tag": adapter.get('tag', None),
							"brand": adapter.get('brand', None),
							"public_price": adapter.get('public_price', None),
							"supplier_price": adapter.get('supplier_price', None),
							"variants": adapter.get('variants', None),
							"availability": adapter.get('availability', None),
							"important_technical_details": adapter.get('important_technical_details', None),
							"product_material_details": adapter.get('product_material_details', None),
							"technical_details": adapter.get('technical_details', None),
							"category": adapter.get('category', None),
							"sub_category": adapter.get('sub_category', None),
							"updated_at":  datetime.now(timezone.utc),
							"page_url": adapter.get('page_url', None),
							"image_url": adapter.get('image_url', None),
					}

					self.db_manager.execute_update("products", data, "page_url")
					spider.logger.warn("Item updated in database: %s" % adapter['page_url'])
				else:
					spider.logger.warn("Item already existed in database: %s" % adapter['page_url'])
									
			else:
				spider.logger.warn("Url already present in database: %s" % adapter['page_url'])
		else:
			try:
				data = {
					'page_url': adapter['page_url']
				}

				self.db_manager.execute_insert("products", data)
			except psycopg2.Error as e:
				spider.logger.error(f"Error inserting data into PostgreSQL: {e}")
