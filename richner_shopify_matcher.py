import sys
import os
import re
import csv
import json
from shopify import Session, ShopifyResource, CustomCollection, Product, Collect, Variant, Image, ApiVersion, Release
import shopify
from dotenv import load_dotenv
from tqdm import tqdm
from bs4 import BeautifulSoup

sys.path.append('..')
from app.logging_config import split_shipping_logger, shopify_syncing_logger
from app.database import DBManager

load_dotenv()

from store_data import data

class ShopifyRichnerMapper():
    def __init__(self):
        try:
            ApiVersion.define_version(Release(os.getenv('API_VERSION')))
            self.session = Session(os.getenv('SHOP_URL'), os.getenv('API_VERSION'), os.getenv('ACCESS_TOKEN_SHOPIFY'))
            ShopifyResource.activate_session(self.session)

            self.max_threads = 10
            self.db_manager = DBManager()
        except Exception as e:
            shopify_syncing_logger.error(f"An unexpected error occurred while creating shopify session: {e}")

    def fetch_all_products(self):
        split_shipping_logger.info('Fetching all products from the shopify app')
        products = self.get_all_resources(shopify.Product, fields="body_html, variants")
        
        products_data = []
        for product in products:
            try:
                hersteller_art_nr = self.extract_hersteller_art_nr(product.body_html)
                price = product.variants[0].price if product.variants else 0

                if hersteller_art_nr and price:
                    products_data.append((hersteller_art_nr, price))
                    print(f"Product Hersteller Art-Nr: {hersteller_art_nr}, Price: {price}")
            except Exception as e:
                print(e)
                pass

        return products_data

    def get_all_resources(self, resource_type, **kwargs):
        resource_count = resource_type.count(**kwargs)
        resources = []
        
        if resource_count > 0:
            page = resource_type.find(**kwargs)
            resources.extend(page)
            
            i = 0 
            with tqdm(total=resource_count, desc="Fetching Resources") as pbar:
                pbar.update(len(page))

                while page.has_next_page():
                    i += 1
                    page = page.next_page()
                    resources.extend(page)
                    pbar.update(len(page))
        
        return resources

    def extract_hersteller_art_nr(self, html):
      soup = BeautifulSoup(html, 'html.parser')
      table = soup.find('table')
      
      if not table:
          return None
      
      for row in table.find_all('tr'):
          columns = row.find_all('td')
          if len(columns) == 2 and 'Hersteller Art-Nr.' in columns[0].text:
              return columns[1].text.strip()
      
      return None
    
    def id_formatter(self, artikel_number):
        formatted_value = re.sub(r'\W|_', '', artikel_number)
        return re.sub(r'\s+', '', formatted_value)     

        return 
    def extract_artikelnummer_lieferant(self, technical_details):
        if not technical_details:
            return None

        technical_details = json.loads(technical_details)
        if 'Artikelnummer Lieferant' in technical_details.keys():
            return technical_details['Artikelnummer Lieferant']

    def parse_richner_data(self):
        with open('richner-data.csv', mode='r', encoding='utf-8-sig') as file:
            reader = csv.DictReader(file)
            
            richner_products_data = []
            for row in reader:
                artikelnummer_lieferant = self.extract_artikelnummer_lieferant(row['technical_details'])
                price = row['public_price']
                product_name = row['product_title']
                
                if artikelnummer_lieferant and price:
                    richner_products_data.append((self.id_formatter(artikelnummer_lieferant), price, product_name))

            return richner_products_data


if __name__ == '__main__':
    mapper = ShopifyRichnerMapper()
    # shopsify_products = mapper.fetch_all_products()
    richner_products = mapper.parse_richner_data()
    print('Total richner products', len(richner_products))
    print('Total shopify products', len(data))

    richner_product_artikle_ids = [item[0] for item in richner_products]

    stats_data = []

    for item in data:
        artikle_number = re.sub(r'\W|_', '', item[0])
        artikle_number = re.sub(r'\s+', '', artikle_number) 

        for product in richner_products:
            if artikle_number == product[0]:
                stats_data.append((product[2], product[0], item[0], product[1], item[1]))

    file_name = 'prices.csv'

    with open(file_name, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Product Title', 'Artikelnummer Lieferant', 'Product Hersteller Art-Nr', 'Richner Price', 'Shopify Price'])
        writer.writerows(stats_data)

    print(f"CSV file '{file_name}' has been created successfully.")
