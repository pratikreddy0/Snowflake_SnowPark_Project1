import os
from snowflake.snowpark import Session
import logging
import sys

# Initiate logging at info level
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%I:%M:%S'
)


# Snowpark session
def get_snowpark_session() -> Session:
    connection_parameters = {
        "ACCOUNT": "ETIFMIB-AUTO_DATA_UNLIMITED",
        "USER": "SNOWPARK_USER",
        "PASSWORD": "Pratikreddy01@",
        "ROLE": "SYSADMIN",
        "DATABASE": "sales_dwh",
        "SCHEMA": "source"
    }
    # Creating Snowflake session object
    return Session.builder.configs(connection_parameters).create()


def traverse_directory(directory, file_extension) -> list:
    local_file_path = []
    file_name = []  # List to store file names
    partition_dir = []

    logging.info(f"Traversing directory: {directory}")
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith(file_extension):
                file_path = os.path.join(root, file)
                file_name.append(file)
                partition_dir.append(root.replace(directory, ""))  # Relative path
                local_file_path.append(file_path)

    return file_name, partition_dir, local_file_path


def upload_files(file_names, partition_dirs, local_file_paths, stage_location, session):
    for index, file_name in enumerate(file_names):
        try:
            put_result = session.file.put(
                local_file_paths[index],
                stage_location + "/" + partition_dirs[index],
                auto_compress=False,
                overwrite=True,
                parallel=10
            )
            logging.info(f"Uploaded {file_name} => {put_result[0].status}")
        except Exception as e:
            logging.error(f"Failed to upload {file_name}: {e}")


def main():
    # Specify the directory path to traverse
    directory_path = '/home/hadoop/Downloads/sales/'
    stage_location = '@sales_dwh.source.my_internal_stg'

    # Traverse for different file types
    csv_file_name, csv_partition_dir, csv_local_file_path = traverse_directory(directory_path, '.csv')
    parquet_file_name, parquet_partition_dir, parquet_local_file_path = traverse_directory(directory_path, '.parquet')
    json_file_name, json_partition_dir, json_local_file_path = traverse_directory(directory_path, '.json')

    # Get Snowpark session
    session = get_snowpark_session()

    # Upload files
    upload_files(csv_file_name, csv_partition_dir, csv_local_file_path, stage_location, session)
    upload_files(parquet_file_name, parquet_partition_dir, parquet_local_file_path, stage_location, session)
    upload_files(json_file_name, json_partition_dir, json_local_file_path, stage_location, session)


if __name__ == '__main__':
    main()
