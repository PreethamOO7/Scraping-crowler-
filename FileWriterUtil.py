import json
import os
import re
from datetime import datetime

from pandas import ExcelWriter
from pandas.io.json import json_normalize

import scripts.logger_util as Logger
from scripts.utils import get_configuration

logger = Logger.get_logger(__name__)
config = get_configuration()


def write_to_file(market_type, category, data, output_format):
    start_time = datetime.now()
    logger.info(
        "Saving {} -> {} data in {} format, started at {}".format(market_type, category, output_format, start_time))
    file_dir = config['DOWNLOAD_LOCATION'] + market_type
    _create_directory(file_dir)

    category_with_out_special_chars = re.sub('[^a-zA-Z0-9]+', ' ', category)
    base_file_name = file_dir + '/' + category_with_out_special_chars

    if output_format == 'json':
        file_name = base_file_name + '.json'
        _write_json_data(file_name, data)
    elif output_format == 'csv':
        file_name = base_file_name + '.json'
        _write_csv_data(file_name, data)
    else:
        file_name = base_file_name + ".xlsx"
        _write_excel_data(file_name, category_with_out_special_chars, data)

    completed_time = datetime.now()
    logger.info("Successfully saved {} -> {} data into {} file, completed at {}, total time - {}"
                .format(market_type, category, file_name, completed_time, completed_time - start_time))


def _create_directory(dir_name):
    if not os.path.exists(dir_name):
        logger.info('Creating Directory: {}'.format(dir_name))
        os.makedirs(dir_name)


def _get_data_frame(data):
    df = json_normalize(data, meta=['Market', ['Category', 'Name'], ['Category', 'SubCategory', 'Name'],
                                    ['Category', 'SubCategory', 'Products', 'Name']],
                        record_path=['Category', 'SubCategory', 'Products', 'Items'])
    df = df.rename(columns={'Category.Name': 'CategoryName', 'Category.SubCategory.Name': 'SubCategoryName',
                            'Category.SubCategory.Products.Name': 'ProductName'})
    columns = list(df)
    initial_order = ['Market', 'CategoryName', 'SubCategoryName', 'ProductName']
    last_order = [x for x in columns if x not in initial_order]
    final_order = initial_order + last_order
    return df[final_order]


def _write_json_data(file_name, data):
    start_time = datetime.now()
    logger.info("Writing json file  {}, started at {}".format(file_name, start_time))
    with open(file_name, 'w') as file:
        json.dump(data, file)

    completed_time = datetime.now()
    logger.info("Successfully writes json file  {}, completed at {}, total time - {}".format(file_name, completed_time,
                                                                                             completed_time - start_time))


def _write_csv_data(file_name, data):
    start_time = datetime.now()
    logger.info("Writing csv file  {}, started at {}".format(file_name, start_time))
    df = _get_data_frame(data)
    df.to_csv(file_name, index=None, header=True, sep='\t', encoding='utf-8')
    completed_time = datetime.now()
    logger.info("Successfully writes csv file  {}, completed at {}, total time - {}".format(file_name, completed_time,
                                                                                            completed_time - start_time))


def _write_excel_data(file_name, sheet_name, data):
    start_time = datetime.now()
    logger.info("Writing excel file  {}, started at {}".format(file_name, start_time))
    df = _get_data_frame(data)
    writer = ExcelWriter(file_name)
    df.to_excel(writer, sheet_name)
    writer.save()
    completed_time = datetime.now()
    logger.info("Successfully writes excel file  {}, completed at {}, total time - {}".format(file_name, completed_time,
                                                                                              completed_time - start_time))
