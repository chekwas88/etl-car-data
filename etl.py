import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

log_file = "log_file.txt"
target_file = "transformed_data.csv"


def drop_empty_columns(df_list):
    clean_list = [df.dropna(axis=1, how='all') for df in df_list]
    return clean_list


def extract_from_csv(file):
    dataframe = pd.read_csv(file)
    return dataframe


def extract_from_json(file):
    dataframe = pd.read_json(file, lines=True)
    return dataframe


def extract_from_xml(file):
    dataframe = pd.DataFrame(columns=["car_model", "year_of_manufacture", "price", "fuel"])
    tree = ET.parse(file)
    root = tree.getroot()
    for car in root:
        car_model = car.find("car_model").text
        year_of_manufacture = car.find("year_of_manufacture").text
        price = float(car.find("price").text)
        fuel = car.find("fuel").text
        df_list = [dataframe, pd.DataFrame(
            [{"car_model": car_model, "year_of_manufacture": year_of_manufacture, "price": price, "fuel": fuel}])]
        clean_dfs = drop_empty_columns(df_list)
        dataframe = pd.concat(clean_dfs, ignore_index=True)

    return dataframe


def extract():
    extracted_data = pd.DataFrame(columns=["car_model", "year_of_manufacture", "price", "fuel"])

    # extract csv files
    for csv_file in glob.glob("/absolute_path_to_data_source/*.csv"):
        df_list = [extracted_data, pd.DataFrame(extract_from_csv(csv_file))]
        clean_dfs = drop_empty_columns(df_list)
        extracted_data = pd.concat(clean_dfs, ignore_index=True)
    # extract json files
    for json_file in glob.glob("/absolute_path_to_data_source/*.json"):
        df_list = [extracted_data, pd.DataFrame(extract_from_json(json_file))]
        clean_dfs = drop_empty_columns(df_list)
        extracted_data = pd.concat(clean_dfs, ignore_index=True)

    # extract xml files
    for xml_file in glob.glob("/absolute_path_to_data_source/*.xml"):
        df_list = [extracted_data, pd.DataFrame(extract_from_xml(xml_file))]
        clean_dfs = drop_empty_columns(df_list)
        extracted_data = pd.concat(clean_dfs, ignore_index=True)

    return extracted_data


# transform data
def transform(data):
    '''
    Round off price to two decimals
     '''
    data['price'] = round(data.price, 2)

    return data


# load data

def load_data(to_file, transformed_data):
    transformed_data.to_csv(to_file)


def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'  # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now()  # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(timestamp + ',' + message + '\n')


# Log the initialization of the ETL process
log_progress("ETL Job Started")

# Log the beginning of the Extraction process
log_progress("Extract phase Started")
extracted_data = extract()

# Log the completion of the Extraction process
log_progress("Extract phase Ended")

# Log the beginning of the Transformation process
log_progress("Transform phase Started")
transformed_data = transform(extracted_data)
print("Transformed Data")
print(transformed_data)

# Log the completion of the Transformation process
log_progress("Transform phase Ended")

# Log the beginning of the Loading process
log_progress("Load phase Started")
load_data(target_file, transformed_data)

# Log the completion of the Loading process
log_progress("Load phase Ended")

# Log the completion of the ETL process
log_progress("ETL Job Ended")
