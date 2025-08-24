import sys
import os
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline, PipelineModel

# Import utilities
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utility.utility import setup_logging, format_time


def create_spark_session(logger):
    """Initialize Spark session"""
    logger.info("Stage 0: Creating Spark session")
    return (SparkSession.builder
            .appName("CarPriceTransform")
            .config("spark.driver.memory", "2g")
            .config("spark.executor.memory", "4g")
            .getOrCreate())


def load_and_clean(logger, spark, input_dir, output_dir):
    """Stage 1: Load car dataset, clean and preprocess"""
    logger.info("Stage 1: Loading and cleaning data")

    schema = T.StructType([
        T.StructField("vin", T.StringType(), False),
        T.StructField("stockNum", T.StringType(), True),
        T.StructField("firstSeen", T.StringType(), True),
        T.StructField("lastSeen", T.StringType(), True),
        T.StructField("msrp", T.DoubleType(), True),
        T.StructField("askPrice", T.DoubleType(), True),
        T.StructField("mileage", T.DoubleType(), True),
    ])

    df = spark.read.csv(os.path.join(input_dir, "CIS_Automotive_Kaggle_Sample.csv"),
                        header=True, schema=schema)

    # Drop duplicates and nulls
    df = df.dropDuplicates(["vin"]).dropna(subset=["askPrice", "mileage", "msrp"])

    # Convert dates
    df = df.withColumn("firstSeen", F.to_date("firstSeen")) \
           .withColumn("lastSeen", F.to_date("lastSeen"))

    # Feature: days on market
    df = df.withColumn("days_on_market", F.datediff("lastSeen", "firstSeen"))

    # Save cleaned data
    df.write.mode("overwrite").parquet(os.path.join(output_dir, "stage1", "cleaned_data"))
    logger.info("Stage 1 complete: Cleaned data saved")

    return df


def train_and_evaluate(logger, df, output_dir):
    """Stage 2: Train Random Forest model and save dataset with predictions"""
    logger.info("Stage 2: Training models")

    # Features and label
    features = ["mileage", "msrp", "days_on_market"]
    assembler = VectorAssembler(inputCols=features, outputCol="features")

    rf = RandomForestRegressor(featuresCol="features", labelCol="askPrice")

    # Pipeline (assembler + model)
    pipeline = Pipeline(stages=[assembler, rf])

    # Train-test split
    train, test = df.randomSplit([0.8, 0.2], seed=42)

    model = pipeline.fit(train)
    predictions = model.transform(test)

    # Evaluation
    evaluator = RegressionEvaluator(labelCol="askPrice", predictionCol="prediction", metricName="rmse")
    rmse = evaluator.evaluate(predictions)
    logger.info(f"Random Forest Test RMSE: {rmse}")

    # Save predictions on full dataset
    full_predictions = model.transform(df) \
        .withColumnRenamed("prediction", "predicted_price") \
        .drop("features")

    full_predictions.write.mode("overwrite").parquet(os.path.join(output_dir, "stage2", "final_cars"))
    logger.info("Stage 2 complete: Predictions saved")

    # Save model for later use (user input predictions)
    model.write().overwrite().save(os.path.join(output_dir, "stage2", "rf_pipeline_model"))
    logger.info("Random Forest pipeline model saved")


def create_query_tables(logger, df, output_dir):
    """Stage 3: Create query-friendly tables"""
    logger.info("Stage 3: Creating query tables")

    stats = df.groupBy("stockNum").agg(
        F.avg("askPrice").alias("avg_price"),
        F.avg("mileage").alias("avg_mileage"),
        F.count("vin").alias("num_listings")
    )

    stats.write.mode("overwrite").parquet(os.path.join(output_dir, "stage3", "stock_summary"))

    logger.info("Stage 3 complete: Query tables saved")


if __name__ == "__main__":
    logger = setup_logging("transform.log")
    start = time.time()

    if len(sys.argv) != 3:
        logger.info("Usage: python execute.py <input_dir> <output_dir>")
        sys.exit(1)

    input_dir = sys.argv[1]
    output_dir = sys.argv[2]

    spark = create_spark_session(logger)

    df = load_and_clean(logger, spark, input_dir, output_dir)
    train_and_evaluate(logger, df, output_dir)
    create_query_tables(logger, df, output_dir)

    end = time.time()
    logger.info("Transformation pipeline completed")
    logger.info(f"Total time taken {format_time(end - start)}")
