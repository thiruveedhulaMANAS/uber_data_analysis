import redshift_connector
import os
import pathlib 
import configparser

parser = configparser.ConfigParser()
path = os.path.dirname(pathlib.Path(__file__).parent.resolve())
print(path)
add_path = 'config/configuration.config'
parser.read(f"{path}/{add_path}")

bucket = parser.get('AWS', 'bucket')
iam_role_arn = parser.get('Redshift', 'iam_role_arn')

def redshift_main():
    conn = sql_conection()
    sql_queries(conn)

def sql_conection():
    try:
      conn = redshift_connector.connect(host=parser.get('Redshift','host_name'),
                                        port=parser.get('Redshift','port'),
                                        database=parser.get('Redshift','database'),
                                        user=parser.get('Redshift','user'),
                                        password=parser.get('Redshift','password')
                                        )
      conn.autocommit=True
      print("successfully connected to redshift")
      return conn
    except Exception as e:
       print(f"failed to connect and exception occurs: {e}")

def sql_queries(conn):
  try:
    cur = conn.cursor()
    cur.execute("""CREATE TABLE "fact_table" (
"index" INTEGER,
  "VendorID" INTEGER,
  "datetime_id" INTEGER,
  "passenger_count_id" INTEGER,
  "trip_distance_id" INTEGER,
  "rate_code_id" INTEGER,
  "store_and_fwd_flag" TEXT,
  "pickup_location_id" INTEGER,
  "dropoff_location_id" INTEGER,
  "payment_type_id" INTEGER,
  "fare_amount" REAL,
  "extra" REAL,
  "mta_tax" REAL,
  "tip_amount" REAL,
  "tolls_amount" REAL,
  "improvement_surcharge" REAL,
  "total_amount" REAL
);""")
    cur.execute("""CREATE TABLE IF NOT EXISTS "passenger_count_dim" (
    "passenger_count_id" INTEGER,
      "passenger_count" INTEGER
    );""")
    cur.execute("""CREATE TABLE IF NOT EXISTS "trip_distance_dim" (
    "trip_distance_id" INTEGER,
      "trip_distance" REAL
    );""")
    cur.execute("""CREATE TABLE IF NOT EXISTS "pickup_location_dim" (
    "pickup_location_id" INTEGER,
      "pickup_latitude" REAL,
      "pickup_longitude" REAL
    );""")
    cur.execute("""CREATE TABLE "dropoff_location_dim" (
    "dropoff_location_id" INTEGER,
      "dropoff_latitude" REAL,
      "dropoff_longitude" REAL
    );""")
    cur.execute("""CREATE TABLE IF NOT EXISTS "payment_type_dim" (
    "payment_type_id" INTEGER,
      "payment_type" INTEGER,
      "payment_type_name" TEXT
    );""")
    cur.execute("""CREATE TABLE IF NOT EXISTS "rate_code_dim" (
    "rate_code_id" INTEGER,
      "RatecodeID" INTEGER,
      "rate_code_name" TEXT
    );""")
    cur.execute("""CREATE TABLE IF NOT EXISTS "datetime_dim" (
    "datetime_id" INTEGER,
      "tpep_pickup_datetime" TIMESTAMP,
      "pick_hour" INTEGER,
      "pick_day" INTEGER,
      "pick_month" INTEGER,
      "pick_year" INTEGER,
      "pick_weekday" INTEGER,
      "tpep_dropoff_datetime" TIMESTAMP,
      "drop_hour" INTEGER,
      "drop_day" INTEGER,
      "drop_month" INTEGER,
      "drop_year" INTEGER,
      "drop_weekday" INTEGER
    );""")
    cur.execute(f"copy fact_table from 's3://{bucket}/fact_table/fact_table.csv' iam_role '{iam_role_arn}' IGNOREHEADER 1 REGION 'ap-south-1' DELIMITER ',' csv;")
    cur.execute(f"copy datetime_dim from 's3://{bucket}/datetime_dim/datetime_dim.csv' iam_role '{iam_role_arn}' IGNOREHEADER 1 REGION 'ap-south-1' DELIMITER ',' csv;")
    cur.execute(f"copy dropoff_location_dim from 's3://{bucket}/dropoff_location_dim/dropoff_location_dim.csv' iam_role '{iam_role_arn}' IGNOREHEADER 1 REGION 'ap-south-1' DELIMITER ',' csv;")
    cur.execute(f"copy passenger_count_dim from 's3://{bucket}/passenger_count_dim/passenger_count_dim.csv' iam_role '{iam_role_arn}' IGNOREHEADER 1 REGION 'ap-south-1' DELIMITER ',' csv;")
    cur.execute(f"copy payment_type_dim from 's3://{bucket}/payment_type_dim/payment_type_dim.csv' iam_role '{iam_role_arn}' IGNOREHEADER 1 REGION 'ap-south-1' DELIMITER ',' csv;")
    cur.execute(f"copy pickup_location_dim from 's3://{bucket}/pickup_location_dim/pickup_location_dim.csv' iam_role '{iam_role_arn}' IGNOREHEADER 1 REGION 'ap-south-1' DELIMITER ',' csv;")
    cur.execute(f"copy rate_code_dim from 's3://{bucket}/rate_code_dim/rate_code_dim.csv' iam_role '{iam_role_arn}' IGNOREHEADER 1 REGION 'ap-south-1' DELIMITER ',' csv;")
    cur.execute(f"copy trip_distance_dim from 's3://{bucket}/trip_distance_dim/trip_distance_dim.csv' iam_role '{iam_role_arn}' IGNOREHEADER 1 REGION 'ap-south-1' DELIMITER ',' csv;")
    cur.fetchall()
    cur.close()
    print("successfully Executed")
  except Exception as e:
     print(f'Exception arises: {e}')
