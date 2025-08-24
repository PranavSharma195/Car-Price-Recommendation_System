import sys
import time
import os
import psycopg2
from pyspark.sql import SparkSession
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utility.utility import setup_logging, format_time


def create_spark_session(app_name="CarsDataLoad"):
    return SparkSession.builder.appName(app_name).getOrCreate()


def create_postgres_tables(logger, pg_un, pg_pw, host, db, port):
    """Create cars table aligned with transform output schema"""
    try:
        conn = psycopg2.connect(dbname=db, user=pg_un, password=pg_pw, host=host, port=port)
        cursor = conn.cursor()
        create_table_query = """
        CREATE TABLE IF NOT EXISTS cars (
            vin TEXT PRIMARY KEY,
            stocknum TEXT,
            firstseen DATE,
            lastseen DATE,
            msrp FLOAT,
            price FLOAT,
            mileage BIGINT,
            days_on_market INT,
            predicted_price FLOAT
        );
        """
        cursor.execute(create_table_query)
        conn.commit()
        cursor.close()
        conn.close()
        logger.info("Postgres cars table created / verified")
    except Exception as e:
        logger.error(f"Error creating Postgres table: {e}")
        raise


def load_to_postgres(logger, spark, input_dir, pg_un, pg_pw, host, db, port):
    """Load parquet final_cars into Postgres"""
    parquet_path = os.path.join(input_dir, "stage2/final_cars")
    df = spark.read.parquet(parquet_path)

    # Align column names with Postgres table schema
    df = (df
          .withColumnRenamed("askPrice", "price")
          .withColumnRenamed("stockNum", "stocknum")
          .withColumnRenamed("firstSeen", "firstseen")
          .withColumnRenamed("lastSeen", "lastseen"))

    row_count = df.count()
    logger.info(f"Loading cars: {row_count} rows")

    jdbc_url = f"jdbc:postgresql://{host}:{port}/{db}"
    connection_props = {
        "user": pg_un,
        "password": pg_pw,
        "driver": "org.postgresql.Driver"
    }

    try:
        # Write to Postgres (truncate avoids full drop)
        (df.write
           .mode("overwrite")
           .option("truncate", "true")
           .jdbc(url=jdbc_url, table="cars", properties=connection_props))
        logger.info("Loaded cars into Postgres")
    except Exception as e:
        logger.error(f"Error loading data to Postgres: {e}")
        raise


if __name__ == "__main__":
    logger = setup_logging("load.log")
    if len(sys.argv) != 7:
        print("Usage: python load/execute.py <input_dir> <pg_un> <pg_pw> <host> <db> <port>")
        sys.exit(1)

    input_dir, pg_un, pg_pw, host, db, port = sys.argv[1:]
    logger.info("Load stage started")
    start = time.time()

    spark = create_spark_session()
    create_postgres_tables(logger, pg_un, pg_pw, host, db, port)
    load_to_postgres(logger, spark, input_dir, pg_un, pg_pw, host, db, port)

    end = time.time()
    logger.info("Load completed")
    logger.info(f"Total time taken {format_time(end-start)}")
