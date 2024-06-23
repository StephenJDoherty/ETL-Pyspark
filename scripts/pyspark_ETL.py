from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

# Create a SparkSession
spark = SparkSession.builder \
    .appName("MedicalRecordsETL") \
    .config("spark.jars", "/path/to/postgresql-42.2.18.jar") \
    .getOrCreate()

# Define the schema for the CSV file
schema = StructType([
    StructField("PatientID", IntegerType(), True),
    StructField("Name", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("Gender", StringType(), True),
    StructField("City", StringType(), True),
    StructField("Disease", StringType(), True),
    StructField("AdmissionDate", DateType(), True),
    StructField("DischargeDate", DateType(), True),
    StructField("StayDays", IntegerType(), True),
    StructField("TreatmentCost", DoubleType(), True)
])

# Extract and read the CSV file into a DataFrame
df = spark.read.format("csv") \
    .option("header", "true") \
    .schema(schema) \
    .load("medical_records.csv")

# Transform the data
# Convert the TreatmentCost column to a string and add a currency symbol
df = df.withColumn("TreatmentCost", lit("$") + col("TreatmentCost").cast(StringType()))

# Create a new column "TreatmentCategory" based on the Disease column
df = df.withColumn("TreatmentCategory", when(col("Disease").contains("Diabetes"), lit("Diabetes Management"))
                   .when(col("Disease").contains("Cardiovascular"), lit("Cardiovascular Treatment"))
                   .otherwise(lit("Other")))

# Load the transformed data into PostgreSQL
# Replace with your actual PostgreSQL connection details
df.write.format("jdbc") \
    .option("url", "jdbc:postgresql://your_host:your_port/your_database") \
    .option("driver", "org.postgresql.Driver") \
    .option("user", "your_username") \
    .option("password", "your_password") \
    .option("dbtable", "medical_records") \
    .mode("append") \
    .save()

# Stop the SparkSession
spark.stop()
