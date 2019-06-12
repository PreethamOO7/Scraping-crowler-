import argparse
from datetime import datetime
from functools import partial
from math import ceil
from multiprocessing import Pool
from multiprocessing import cpu_count
from threading import Thread

from tqdm import tqdm

import scripts.logger_util as Logger
from scripts.FileWriterUtil import write_to_file
from scripts.request_util import get_data_from_url
from scripts.session_helper import start_auth_updater, stop_auth_updater
from scripts.utils import *

config = get_configuration()
logger = Logger.get_logger(__name__)

FLAG = None


def _convert_to_pascal(string):
    return "%s%s" % (string[0].upper(), string[1:])


def get_product_items(target_url, start_value):
    items = []
    product_items = get_data_from_url(url=get_api_url(target_url, start_value), headers=get_referer_headers(target_url))
    if product_items:
        if 'listings' in product_items and isinstance(product_items['listings'], list):
            for product_item in product_items['listings']:
                item = dict((_convert_to_pascal(k), v) for k, v in product_item.items())
                items.append(item)
        return items
    return None


def get_items_details(target_url):
    product_url = get_api_url(target_url, 0)
    product_items = get_data_from_url(url=product_url, headers=get_referer_headers(target_url))
    response = []
    if product_items:
        logger.info("Getting data from '{}'".format(product_url))
        items_found = product_items['numFound']
        logger.debug('Items Found - {}'.format(items_found))
        with Pool(processes=config['NUM_OF_WORKER_PROCESS']) as pool:
            results = list(tqdm(pool.imap(partial(get_product_items, target_url), range(0, items_found, 12)),
                                total=ceil(items_found / 12)))
            for result in results:
                if result is not None:
                    response += result
        logger.debug('Item Processed - {}'.format(len(response)))
        return response
    return None


def get_udaan_data(market_type):
    data = []
    market_categories = get_data_from_url(url=get_api_url("/market/v1", 0), headers=get_referer_headers("/market/v1"))
    if market_categories:
        file_write_thread = []
        for market in market_categories['listingUnits']:
            if market['title'] == market_type:
                logger.info("Getting data of '{}'".format(market['title']))
                market_data = {'Market': market['title'], 'Category': []}
                for category in market['l2Units']:
                    if FLAG.include_categories is not None and category['title'] not in FLAG.include_categories:
                        logger.info(
                            "'{}' category is not in categories inclusion list  '{}', Hence ignoring this category"
                                .format(category['title'], ', '.join(FLAG.include_categories)))
                        continue
                    if FLAG.exclude_categories is not None and category['title'] in FLAG.exclude_categories:
                        logger.info(
                            "'{}' category is in categories exclusion list  '{}', Hence ignoring this category"
                                .format(category['title'], ', '.join(FLAG.exclude_categories)))
                        continue
                    logger.info("Getting data of '{}' category under {} market type"
                                .format(category['title'], market['title']))
                    category_data = {'Name': category['title'], 'SubCategory': []}
                    for sub_category in category['l3Units']:
                        sub_category_data = {'Name': sub_category['title'], 'Products': []}
                        logger.info("Getting info of ({} -> {} -> {}) product"
                                    .format(category['title'], sub_category['title'], sub_category['title']))
                        item_details = get_items_details(sub_category['targetUrl'])
                        if item_details is not None:
                            sub_category_data['Products'].append({'Name': sub_category['title'],
                                                                  'Items': item_details})
                        if 'l4Units' in sub_category and isinstance(sub_category['l4Units'], list):
                            for sub_category_products in sub_category['l4Units']:
                                logger.info("Getting info of ({} -> {} -> {}) product"
                                            .format(category['title'], sub_category['title'],
                                                    sub_category_products['title'], ))
                                sub_item_details = get_items_details(sub_category_products['targetUrl'])
                                if sub_item_details is not None:
                                    sub_category_data['Products'].append({'Name': sub_category_products['title'],
                                                                          'Items': sub_item_details})

                        category_data['SubCategory'].append(sub_category_data)

                    market_data['Category'].append(category_data)
                    temp_data = {"Market": market_type, "Category": []}
                    temp_data['Category'].append(category_data)
                    thread = Thread(target=write_to_file,
                                    name=(str(market_data['Market']) + "-" + str(category_data['Name'])),
                                    args=(market_data['Market'], category_data['Name'], temp_data, FLAG.output))
                    thread.start()
                    file_write_thread.append(thread)
                data.append(market_data)
                for thread in file_write_thread:
                    if thread.is_alive():
                        logger.debug("Waiting for file write operation to complete, file name - " + thread.getName())
                        thread.join()
        return data
    return None


def start(market):
    logger.info("Download started for '{}' data at {} time".format(market, datetime.now()))
    crawl_json_data = get_udaan_data(market)
    if crawl_json_data is not None and len(crawl_json_data) > 0:
        logger.info("Download of '{}' data completed at {} time".format(market, datetime.now()))
        write_to_file(market, market, crawl_json_data, FLAG.output)
        logger.info(
            "Successfully writes consolidated '{}' data, writes completed at {} time".format(market, datetime.now()))
    else:
        logger.info('No data found for given market {}.'.format(market))


def main():
    start_time = datetime.now()
    logger.info('Crawling triggered at {} time'.format(start_time))

    start_auth_updater()
    start(FLAG.market)
    stop_auth_updater()
    end_time = datetime.now()
    logger.info('Crawling successfully completed at {} time'.format(end_time))
    logger.info('Total time {}'.format(end_time - start_time))


def str2bool(v):
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Data Crawler')

    parser.add_argument('-o', '--output', type=str, help='Output format', default='json',
                        choices=['json', 'csv', 'excel'])
    parser.add_argument('--folder-loc', type=str, help='Folder location where files will get store',
                        default='data')
    parser.add_argument("--proxy", type=str2bool, nargs='?',
                        const=True, default=False,
                        help="Activate proxy.")
    parser.add_argument('-e', '--exclude-categories', nargs='*', type=str,
                        help='Categories to Exclude', required=False, default=None)
    parser.add_argument('-i', '--include-categories', nargs='*', type=str,
                        help='Categories to Include', required=False, default=None)

    requiredNamed = parser.add_argument_group('required arguments')
    requiredNamed.add_argument('--market', type=str, help='Market to crawl', required=True)

    FLAG = parser.parse_args()
    print(FLAG)
    config = get_configuration()
    config['NUM_OF_WORKER_PROCESS'] = cpu_count() - 1
    config['PROXY'] = True if FLAG.proxy else False
    config['DOWNLOAD_LOCATION'] = FLAG.folder_loc if FLAG.folder_loc.endswith("/") else FLAG.folder_loc + "/"
    update_configuration(config)

    main()
