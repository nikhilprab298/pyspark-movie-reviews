
# ============================================================
# HOMEWORK 1.2
# IMDb Movie Review Dataset Analysis using PySpark
# ============================================================

# STEP 1: Import required libraries

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, trim, lower

# ============================================================
# STEP 2: Initialize Spark Session
# ============================================================

spark = SparkSession.builder \
    .appName("IMDb Movie Review Analysis") \
    .master("local[*]") \
    .getOrCreate()

print("Spark Session Created Successfully!")

# ============================================================
# STEP 3: Load the IMDb Dataset
# ============================================================

file_path = "IMDB Dataset.csv"

df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("multiLine", True) \
    .option("escape", '"') \
    .csv(file_path)

print("\nOriginal Dataset Loaded Successfully!")

# ============================================================
# STEP 4: Explore the Dataset
# ============================================================

print("\nDATASET SCHEMA:")
df.printSchema()

print("\nFIRST 5 ROWS:")
df.show(5, truncate=False)

print("\nORIGINAL TOTAL NUMBER OF ROWS:")
print(df.count())

# ============================================================
# STEP 5: Clean the Dataset
# Remove null and empty reviews
# ============================================================

cleaned_df = df.filter(
    col("review").isNotNull() &
    (length(trim(col("review"))) > 0) &
    col("sentiment").isNotNull() &
    (length(trim(col("sentiment"))) > 0)
)

print("\nDATA CLEANING COMPLETED!")

print("Number of rows after cleaning:")
print(cleaned_df.count())

# ============================================================
# STEP 6: Analyze the Dataset
# ============================================================

total_reviews = cleaned_df.count()

print("\nTOTAL NUMBER OF REVIEWS:")
print(total_reviews)

print("\nPOSITIVE AND NEGATIVE REVIEW COUNTS:")

sentiment_counts = cleaned_df.groupBy(
    lower(trim(col("sentiment"))).alias("sentiment")
).count()

sentiment_counts.show()

# ============================================================
# STEP 7: Transform the Dataset
# Create a column containing review length
# ============================================================

transformed_df = cleaned_df.withColumn(
    "review_length",
    length(col("review"))
)

print("\nDATASET WITH REVIEW LENGTH:")
transformed_df.select(
    "review",
    "sentiment",
    "review_length"
).show(5, truncate=False)

# ============================================================
# STEP 8: Filter Reviews Longer Than 500 Characters
# ============================================================

long_reviews = transformed_df.filter(
    col("review_length") > 500
)

print("\nREVIEWS WITH MORE THAN 500 CHARACTERS:")

long_reviews.select(
    "sentiment",
    "review_length",
    "review"
).show(10, truncate=False)

print("\nNUMBER OF REVIEWS LONGER THAN 500 CHARACTERS:")
print(long_reviews.count())

# ============================================================
# STEP 9: Save Cleaned and Transformed Dataset
# ============================================================

output_path = "output/cleaned_movie_reviews"

transformed_df.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(output_path)

print("\nCLEANED AND TRANSFORMED DATASET SAVED SUCCESSFULLY!")

# ============================================================
# STEP 10: Stop Spark Session
# ============================================================

spark.stop()

print("\nSPARK SESSION TERMINATED.")
print("HOMEWORK COMPLETED SUCCESSFULLY!")
