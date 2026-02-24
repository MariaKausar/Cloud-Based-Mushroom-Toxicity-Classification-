-- Databricks notebook source
-- MAGIC %md 
-- MAGIC # Spark SQL

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Import necessary libraries
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC from pyspark.sql.functions import split
-- MAGIC import pandas as pd
-- MAGIC from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # List the files and directories in the "/FileStore/tables/" directory
-- MAGIC dbutils.fs.ls("/FileStore/tables/")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #from pyspark.sql import SparkSession
-- MAGIC #from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
-- MAGIC
-- MAGIC # Create SparkSession
-- MAGIC spark = SparkSession.builder \
-- MAGIC     .appName("DataFrame Cleaning") \
-- MAGIC     .getOrCreate()
-- MAGIC
-- MAGIC # Define schema for DataFrame
-- MAGIC mySchema = StructType([
-- MAGIC     StructField("Id", StringType(), True),
-- MAGIC     StructField("Study Title", StringType(), True),
-- MAGIC     StructField("Acronym", StringType(), True),
-- MAGIC     StructField("Status", StringType(), True),
-- MAGIC     StructField("Conditions", StringType(), True),
-- MAGIC     StructField("Interventions", StringType(), True),
-- MAGIC     StructField("Sponsor", StringType(), True),
-- MAGIC     StructField("Collaborators", StringType(), True),
-- MAGIC     StructField("Enrollment", StringType(), True),
-- MAGIC     StructField("Funder Type", StringType(), True),
-- MAGIC     StructField("Type", StringType(), True),
-- MAGIC     StructField("Study Design", StringType(), True),
-- MAGIC     StructField("Start", StringType(), True),
-- MAGIC     StructField("Completion", StringType(), True)
-- MAGIC ])

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Read the CSV file into a DataFrame using the specified schema and options
-- MAGIC myDF = (spark.read.format("csv")
-- MAGIC     .option("delimiter","\t")
-- MAGIC     .option("quote","")
-- MAGIC     .option("header","true")
-- MAGIC     .schema(mySchema)
-- MAGIC     .load("/FileStore/tables/clinicaltrial_2023.csv"))
-- MAGIC
-- MAGIC # Display DataFrame
-- MAGIC #display(myDF)

-- COMMAND ----------

-- MAGIC %md Cleaning Done

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # First, we want to create a temporary table (view) 
-- MAGIC myDF.createOrReplaceTempView("clinicaltrial_2023")

-- COMMAND ----------

-- Show the list of available databases
SHOW DATABASES

-- COMMAND ----------

-- Show the list of available tables
SHOW TABLES

-- COMMAND ----------

-- Select and display the first 10 rows from the table "clinicaltrial_2023"
SELECT * FROM clinicaltrial_2023 LIMIT 10

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import regexp_replace
-- MAGIC from pyspark.sql.functions import regexp_replace, col
-- MAGIC
-- MAGIC # Assuming you have already loaded clinicaltrial_2023 DataFrame
-- MAGIC
-- MAGIC # Remove double quotes and commas from each column
-- MAGIC for Completion in myDF.columns:
-- MAGIC     myDF = myDF.withColumn(Completion, regexp_replace(col(Completion), '"', ''))
-- MAGIC     myDF = myDF.withColumn(Completion, regexp_replace(col(Completion), ',', ''))
-- MAGIC
-- MAGIC # Show the modified DataFrame
-- MAGIC #myDF.show()
-- MAGIC #display(myDF)
-- MAGIC

-- COMMAND ----------

-- Create New Databse   
 
CREATE DATABASE IF NOT EXISTS clinicaltrial_2023

-- COMMAND ----------

-- MAGIC %md try to create pharma temporary table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC
-- MAGIC # Create SparkSession
-- MAGIC spark = SparkSession.builder \
-- MAGIC     .appName("DataFrame Cleaning") \
-- MAGIC     .getOrCreate()
-- MAGIC
-- MAGIC # Define schema for DataFrame
-- MAGIC pharmaSchema = StructType([
-- MAGIC     StructField("Company", StringType(), True),
-- MAGIC     StructField("Parent_Company", StringType(), True),
-- MAGIC     StructField("Penalty_Amount", StringType(), True),
-- MAGIC     StructField("Subtraction_From_Penalty", StringType(), True),
-- MAGIC     StructField("Penalty_Amount_Adjusted_For_Eliminating_Multiple_Counting", StringType(), True),
-- MAGIC     StructField("Penalty_Year", StringType(), True),
-- MAGIC     StructField("Penalty_Date", StringType(), True),
-- MAGIC     StructField("Offense_Group", StringType(), True),
-- MAGIC     StructField("Primary_Offense", StringType(), True),
-- MAGIC     StructField("Secondary_Offense", StringType(), True),
-- MAGIC     StructField("Description", StringType(), True),
-- MAGIC     StructField("Level_of_Government", StringType(), True),
-- MAGIC     StructField("Action_Type", StringType(), True),
-- MAGIC     StructField("Agency", StringType(), True),
-- MAGIC     StructField("Civil/Criminal", StringType(), True),
-- MAGIC     StructField("Prosecution_Agreement", StringType(), True),
-- MAGIC     StructField("Court", StringType(), True),
-- MAGIC     StructField("Case_ID", StringType(), True),
-- MAGIC     StructField("Private_Litigation_Case_Title", StringType(), True),
-- MAGIC     StructField("Lawsuit_Resolution", StringType(), True),
-- MAGIC     StructField("Facility_State", StringType(), True),
-- MAGIC     StructField("City", StringType(), True),
-- MAGIC     StructField("Address", StringType(), True),
-- MAGIC     StructField("Zip", StringType(), True),
-- MAGIC     StructField("NAICS_Code", StringType(), True),
-- MAGIC     StructField("NAICS_Translation", StringType(), True),
-- MAGIC     StructField("HQ_Country_of_Parent", StringType(), True),
-- MAGIC     StructField("HQ_State_of_Parent", StringType(), True),
-- MAGIC     StructField("Ownership_Structure", StringType(), True),
-- MAGIC     StructField("Parent_Company_Stock_Ticker", StringType(), True),
-- MAGIC     StructField("Major_Industry_of_Parent", StringType(), True),
-- MAGIC     StructField("Specific_Industry_of_Parent", StringType(), True),
-- MAGIC     StructField("Info_Source", StringType(), True),
-- MAGIC     StructField("Notes", StringType(), True)
-- MAGIC ])
-- MAGIC
-- MAGIC # Load CSV file into DataFrame with defined schema
-- MAGIC pharma_df = spark.read.csv("/FileStore/tables/pharma.csv", header=True, schema=pharmaSchema)
-- MAGIC
-- MAGIC # Show the first few rows of the DataFrame
-- MAGIC #display(pharma_df)
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # First, we want to create a temporary table (view) 
-- MAGIC pharma_df.createOrReplaceTempView("pharma")
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- Show the list of available databases
SHOW DATABASES

-- COMMAND ----------

-- Show the list of available tables
SHOW TABLES

-- COMMAND ----------

-- MAGIC %md pharma table end

-- COMMAND ----------

SELECT * FROM pharma LIMIT 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Problem 1

-- COMMAND ----------

SELECT COUNT(*) FROM clinicaltrial_2023

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #Problem 2

-- COMMAND ----------

SELECT Type FROM clinicaltrial_2023

-- COMMAND ----------

-- Register the DataFrame as a temporary view

SELECT Type, COUNT(*) AS Frequency
FROM clinicaltrial_2023
GROUP BY Type
ORDER BY Frequency DESC

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #Problem 3

-- COMMAND ----------

SELECT Conditions FROM clinicaltrial_2023

-- COMMAND ----------

-- Write SQL query to count frequency of each condition and find the top 5
SELECT Conditions, COUNT(*) AS Frequency
FROM clinicaltrial_2023
GROUP BY Conditions
ORDER BY Frequency DESC
LIMIT 5

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #Problem 4

-- COMMAND ----------

SELECT Sponsor FROM clinicaltrial_2023


-- COMMAND ----------

SELECT Parent_Company FROM pharma

-- COMMAND ----------

-- Write SQL query to find top 10 sponsors with their frequency that exist only in clinicaltrial_2023
SELECT c.Sponsor, COUNT(*) AS Frequency
FROM clinicaltrial_2023 c
LEFT JOIN pharma p ON c.Sponsor = p.Parent_Company
WHERE p.Parent_Company IS NULL
GROUP BY c.Sponsor
ORDER BY Frequency DESC
LIMIT 10


-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #Problem 5

-- COMMAND ----------

SELECT Completion FROM clinicaltrial_2023


-- COMMAND ----------

SELECT Status FROM clinicaltrial_2023

-- COMMAND ----------

SELECT Status, Completion FROM clinicaltrial_2023

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import regexp_replace
-- MAGIC from pyspark.sql.functions import regexp_replace, col
-- MAGIC
-- MAGIC # Assuming you have already loaded clinicaltrial_2023 DataFrame
-- MAGIC
-- MAGIC # Remove double quotes and commas from each column
-- MAGIC for Completion in myDF.columns:
-- MAGIC     myDF = myDF.withColumn(Completion, regexp_replace(col(Completion), '"', ''))
-- MAGIC     myDF = myDF.withColumn(Completion, regexp_replace(col(Completion), ',', ''))
-- MAGIC
-- MAGIC # Show the modified DataFrame
-- MAGIC #myDF.show()
-- MAGIC display(myDF)
-- MAGIC

-- COMMAND ----------

SELECT Status, Completion FROM clinicaltrial_2023

-- COMMAND ----------

SELECT Status, 
       SUBSTRING(Completion, 1, 7) AS Completion
  FROM clinicaltrial_2023


-- COMMAND ----------

/*SELECT Status, 
       SUBSTRING(Completion, 1, 7) AS Completion
  FROM clinicaltrial_2023
 WHERE YEAR(Completion) = 2023*/
 SELECT Status, 
       SUBSTRING(Completion, 1, 7) AS Completion
  FROM clinicaltrial_2023
 WHERE SUBSTRING(Completion, 1, 4) = '2023'


-- COMMAND ----------

SELECT 
    Status,
    SUBSTRING(Completion, 1, 7) AS Completion
FROM 
    clinicaltrial_2023
WHERE 
    SUBSTRING(Completion, 1, 4) = '2023' AND
    Status = 'COMPLETED';


-- COMMAND ----------

WITH MonthlyCompletion AS (
    SELECT 
        Status,
        SUBSTRING(Completion, 1, 7) AS Completion
    FROM 
        clinicaltrial_2023
    WHERE 
        SUBSTRING(Completion, 1, 4) = '2023' AND
        Status = 'COMPLETED'
)

SELECT 
    MONTH(Completion) AS Month,
    COUNT(*) AS Frequency
FROM 
    MonthlyCompletion
GROUP BY 
    MONTH(Completion)
ORDER BY 
    Month;


-- COMMAND ----------

-- MAGIC %md 
-- MAGIC # Further Analysis 3

-- COMMAND ----------

-- Top 10 most frequent primary offense categories 
 
SELECT DISTINCT(Parent_Company), 
       Primary_Offense, 
       COUNT(*) AS Offense_Count
FROM pharma
GROUP BY Parent_Company, Primary_Offense
ORDER BY Offense_Count DESC
LIMIT 10;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC  
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC  
-- MAGIC data = spark.sql("SELECT DISTINCT(Parent_Company), Primary_Offense, COUNT(*) AS Offense_Count FROM pharma GROUP BY Parent_Company, Primary_Offense ORDER BY Offense_Count DESC LIMIT 10;").toPandas()
-- MAGIC  
-- MAGIC  
-- MAGIC # Create bar plot
-- MAGIC plt.bar(data['Primary_Offense'], data['Offense_Count'])
-- MAGIC  
-- MAGIC # Set plot title and axis labels
-- MAGIC plt.title('Top 10 Primary Offense Categories')
-- MAGIC plt.xlabel('Primary Offense Category')
-- MAGIC plt.ylabel('Number of Offenses')
-- MAGIC  
-- MAGIC # Display the plot
-- MAGIC plt.show()

-- COMMAND ----------


