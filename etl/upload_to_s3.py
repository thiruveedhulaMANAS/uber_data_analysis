import boto3
import os
import pathlib
import configparser

# Create a ConfigParser object to read configuration settings
parser = configparser.ConfigParser()

# Determine the directory path of the current script
path = os.path.dirname(pathlib.Path(__file__).parent.resolve())

# Specify the relative path to the configuration file
add_path = 'config/configuration.config'

# Read the configuration settings from the specified file
parser.read(f"{path}/{add_path}")

# Retrieve the value of the 'bucket' key from the 'AWS' section in the configuration file
bucket = parser.get('AWS', 'bucket')

try:
    s3 = boto3.resource('s3',
                    aws_access_key_id=parser.get('AWS','aws_access_key_id'),
                    aws_secret_access_key=parser.get('AWS','aws_secret_access_key'))
    print("successfully connected to s3")
except Exception as e:
    print(f"failed to connect with s3 bucket and exception arises: {e}")

def main():
    s3.Bucket(bucket).upload_file('/opt/airflow/transform_data/passenger_count_dim.csv', 'passenger_count_dim/passenger_count_dim.csv')
    s3.Bucket(bucket).upload_file('/opt/airflow/transform_data/pickup_location_dim.csv', 'pickup_location_dim/pickup_location_dim.csv')
    s3.Bucket(bucket).upload_file('/opt/airflow/transform_data/trip_distance_dim.csv', 'trip_distance_dim/trip_distance_dim.csv')
    s3.Bucket(bucket).upload_file('/opt/airflow/transform_data/dropoff_location_dim.csv', 'dropoff_location_dim/dropoff_location_dim.csv')
    s3.Bucket(bucket).upload_file('/opt/airflow/transform_data/payment_type_dim.csv', 'payment_type_dim/payment_type_dim.csv')
    s3.Bucket(bucket).upload_file('/opt/airflow/transform_data/rate_code_dim.csv', 'rate_code_dim/rate_code_dim.csv')
    s3.Bucket(bucket).upload_file('/opt/airflow/transform_data/datetime_dim.csv', 'datetime_dim/datetime_dim.csv')
    s3.Bucket(bucket).upload_file('/opt/airflow/transform_data/fact_table.csv', 'fact_table/fact_table.csv')
