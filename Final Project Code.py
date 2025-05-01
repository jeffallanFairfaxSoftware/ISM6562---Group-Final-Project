# Databricks notebook source
import pandas as pd

# Obtaining File Path From Excel Upload
file_path = "dbfs:/FileStore/tables/API_ST_INT_ARVL_DS2_en_excel_v2_20308.xlsx"

# Load Sheet at A4
df_raw = spark.read.format("com.crealytics.spark.excel") \
    .option("dataAddress", "'Data'!A4") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(file_path)

# Select relevant columns
df_selected = df_raw.select(
    "Country Name",
    "Country Code",
    "2015", "2016", "2017", "2018", "2019", "2020"
)

# Convert to Pandas for reshaping
df_pandas = df_selected.toPandas()

# Melt to long format
df_long = df_pandas.melt(
    id_vars=["Country Name", "Country Code"],
    value_vars=["2015", "2016", "2017", "2018", "2019", "2020"],
    var_name="Year",
    value_name="Arrivals"
)

#  Clean Arrivals values
df_long["Arrivals"] = (
    df_long["Arrivals"]
    .astype(str)
    .str.replace(",", "", regex=False)
    .str.replace("\xa0", "", regex=False)
    .str.strip()
)
df_long["Arrivals"] = pd.to_numeric(df_long["Arrivals"], errors="coerce")

# Standardize Country Names and Create Index for join
df_long["Country Name"] = df_long["Country Name"].str.strip().str.title()
df_long["Index"] = df_long["Country Name"] + "-" + df_long["Year"]

# Convert to Spark DF
df_final_arrivals = spark.createDataFrame(
    df_long[["Country Name", "Year", "Arrivals", "Index"]]
)


display(df_final_arrivals)


# COMMAND ----------

# Obtaining File Path From Excel Upload
file_path = "dbfs:/FileStore/tables/API_ST_INT_RCPT_CD_DS2_en_excel_v2_15429-1.xlsx"
# Load Sheet at A4
df_data = spark.read.format("com.crealytics.spark.excel") \
    .option("dataAddress", "'Data'!A4") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(file_path)

# Get Columns
df_data_filtered = df_data.select(
    "Country Name",
    "Country Code",
    "2015", "2016", "2017", "2018", "2019", "2020"
)

# Convert to pandas
df_pandas = df_data_filtered.toPandas()

# Melting
df_long = df_pandas.melt(
    id_vars=["Country Name", "Country Code"],
    value_vars=["2015", "2016", "2017", "2018", "2019", "2020"],
    var_name="Year",
    value_name="Receipts"
)

# Clean Receipts column
df_long["Receipts"] = (
    df_long["Receipts"]
    .astype(str)
    .str.replace(",", "", regex=False)
    .str.replace("\xa0", "", regex=False)
    .str.strip()
)

df_long["Receipts"] = pd.to_numeric(df_long["Receipts"], errors="coerce")

# Add Index column
df_long["Country Name"] = df_long["Country Name"].str.strip().str.title()
df_long["Index"] = df_long["Country Name"] + "-" + df_long["Year"]

# Spark DF
df_final_receipts = spark.createDataFrame(df_long[["Country Name", "Year", "Receipts", "Index"]])


display(df_final_receipts)


# COMMAND ----------

# Load Excel
new_file_path = "dbfs:/FileStore/tables/API_ST_INT_XPND_CD_DS2_en_excel_v2_15541.xlsx"

df_data_2 = spark.read.format("com.crealytics.spark.excel") \
    .option("dataAddress", "'Data'!A4") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(new_file_path)

# Select Columns
df_data_2_filtered = df_data_2.select(
    "Country Name",
    "Country Code",
    "2015", "2016", "2017", "2018", "2019", "2020"
)

# Pandas
df_pandas = df_data_2_filtered.toPandas()

# melt
df_long = df_pandas.melt(
    id_vars=["Country Name", "Country Code"],
    value_vars=["2015", "2016", "2017", "2018", "2019", "2020"],
    var_name="Year",
    value_name="Expenditures"
)

# Clean Expenditures
df_long["Expenditures"] = (
    df_long["Expenditures"]
    .astype(str)
    .str.replace(",", "", regex=False)
    .str.replace("\xa0", "", regex=False)
    .str.strip()
)

df_long["Expenditures"] = pd.to_numeric(df_long["Expenditures"], errors="coerce")

# Index 
df_long["Country Name"] = df_long["Country Name"].str.strip().str.title()
df_long["Index"] = df_long["Country Name"] + "-" + df_long["Year"]

# Spark DF
df_final_expenditure = spark.createDataFrame(df_long[["Country Name", "Year", "Expenditures", "Index"]])


display(df_final_expenditure)


# COMMAND ----------

# Excel
file_path = "dbfs:/FileStore/tables/API_SP_POP_TOTL_DS2_en_csv_v2_19373.xlsx"

# Selecting cell A5 for data
df_raw = spark.read.format("com.crealytics.spark.excel") \
    .option("dataAddress", "'API_SP.POP.TOTL_DS2_en_csv_v2_1'!A5") \
    .option("header", "false") \
    .option("inferSchema", "true") \
    .load(file_path)

# Selecting Columns
df_selected = df_raw.select(
    "_c0", "_c1", "_c59", "_c60", "_c61", "_c62", "_c63", "_c64"
)

# renamed
df_renamed = df_selected.toDF(
    "Country Name", "Country Code", "2015", "2016", "2017", "2018", "2019", "2020"
)

# pandas reshaped
df_pandas = df_renamed.toPandas()

# melting 
df_long = df_pandas.melt(
    id_vars=["Country Name", "Country Code"],
    value_vars=["2015", "2016", "2017", "2018", "2019", "2020"],
    var_name="Year",
    value_name="Population"
)

# Cleaning population
df_long["Population"] = (
    df_long["Population"]
    .astype(str)
    .str.replace(",", "", regex=False)
    .str.replace("\xa0", "", regex=False)
    .str.strip()
)
df_long["Population"] = pd.to_numeric(df_long["Population"], errors="coerce")
df_long["Country Name"] = df_long["Country Name"].str.strip().str.title()
df_long["Index"] = df_long["Country Name"] + "-" + df_long["Year"]

# Spark Dataframe
df_final_population = spark.createDataFrame(
    df_long[["Country Name", "Year", "Population", "Index"]]
)

display(df_final_population)


# COMMAND ----------

# Replacing Space with underscore
df_final_arrivals = df_final_arrivals.withColumnRenamed("Country Name", "Country_Name")
df_final_receipts = df_final_receipts.withColumnRenamed("Country Name", "Country_Name")
df_final_expenditure = df_final_expenditure.withColumnRenamed("Country Name", "Country_Name")
df_final_population = df_final_population.withColumnRenamed("Country Name", "Country_Name")

# Join Arrivals to Receipts
joined_df = df_final_arrivals.join(
    df_final_receipts.drop("Country_Name", "Year"),
    on="Index",
    how="inner"
)

# Join above to Expenditure
joined_df = joined_df.join(
    df_final_expenditure.drop("Country_Name", "Year"),
    on="Index",
    how="inner"
)

# join above to population
joined_df = joined_df.join(
    df_final_population.drop("Country_Name", "Year"),
    on="Index",
    how="inner"
)

# Reordering columns and choosing joined columns
ordered_columns = ["Country_Name", "Year", "Index"] + [col for col in joined_df.columns if col not in ["Country_Name", "Year", "Index"]]
joined_df = joined_df.select(*ordered_columns)

display(joined_df)


# COMMAND ----------

from pyspark.sql.functions import col, count, when, avg, lit, round
from pyspark.sql import Window

# identifier and numerical columns 
identifier_cols = ["Country_Name", "Year", "Index"]
metric_cols = [c for c in joined_df.columns if c not in identifier_cols]

# Obtaining Null Count for each row
joined_df = joined_df.withColumn(
    "NullCount",
    sum(when(col(c).isNull(), 1).otherwise(0) for c in metric_cols)
)

# Equal to or more than 2 nulls per row means badrow
bad_rows_per_country = joined_df \
    .filter(col("NullCount") >= 2) \
    .groupBy("Country_Name") \
    .count() \
    .withColumnRenamed("count", "BadRowCount")

# Join this information to the current table. 
joined_df = joined_df.drop("BadRowCount") \
    .join(bad_rows_per_country, on="Country_Name", how="left") \
    .fillna({"BadRowCount": 0})

# Countries with a bad row count of 5 are removed
cleaned_df = joined_df.filter(col("BadRowCount") < 4)

# rows with 2 or more nulls are removed
cleaned_df = cleaned_df.filter(col("NullCount") < 2)

# rest are filled with average based on country metrics if there are any
window_spec = Window.partitionBy("Country_Name")
for c in metric_cols:
    cleaned_df = cleaned_df.withColumn(
        f"{c}_filled",
        when(col(c).isNull(), avg(c).over(window_spec)).otherwise(col(c))
    ).drop(c).withColumnRenamed(f"{c}_filled", c)

# everything else is a 0
cleaned_df = cleaned_df.fillna({c: 0 for c in metric_cols})

# round to hundreth
for c in metric_cols:
    cleaned_df = cleaned_df.withColumn(c, round(col(c), 2))

# Viet nam to Vietnam
cleaned_df = cleaned_df.withColumn(
    "Country_Name",
    when(col("Country_Name") == "Viet Nam", "Vietnam").otherwise(col("Country_Name"))
)

# balance column= receipt - expenditures
cleaned_df = cleaned_df.withColumn("Balance", round(col("Receipts") - col("Expenditures"), 2))


#NullCount and badrow count are removed
cleaned_df = cleaned_df.drop("NullCount", "BadRowCount")


#Getting list of countries, and selecting rows that have these as a country. everything else will not be counted
actual_countries = [
    "Afghanistan", "Albania", "Algeria", "Andorra", "Angola", "Antigua And Barbuda", "Argentina", "Armenia",
    "Australia", "Austria", "Azerbaijan", "Bahamas, The", "Bahrain", "Bangladesh", "Barbados", "Belarus",
    "Belgium", "Belize", "Benin", "Bhutan", "Bolivia", "Bosnia And Herzegovina", "Botswana", "Brazil",
    "Brunei Darussalam", "Bulgaria", "Burkina Faso", "Burundi", "Cabo Verde", "Cambodia", "Cameroon",
    "Canada", "Chile", "China", "Colombia", "Comoros", "Congo, Dem. Rep.", "Congo, Rep.", "Costa Rica",
    "Cote D'Ivoire", "Croatia", "Cuba", "Cyprus", "Czechia", "Denmark", "Djibouti", "Dominica",
    "Dominican Republic", "Ecuador", "Egypt, Arab Rep.", "El Salvador", "Equatorial Guinea", "Eritrea",
    "Estonia", "Eswatini", "Ethiopia", "Fiji", "Finland", "France", "Gabon", "Gambia, The", "Georgia",
    "Germany", "Ghana", "Greece", "Grenada", "Guatemala", "Guinea", "Guinea-Bissau", "Guyana", "Haiti",
    "Honduras", "Hungary", "Iceland", "India", "Indonesia", "Iran, Islamic Rep.", "Iraq", "Ireland", "Israel",
    "Italy", "Jamaica", "Japan", "Jordan", "Kazakhstan", "Kenya", "Kiribati", "Korea, Rep.", "Kuwait",
    "Kyrgyz Republic", "Lao Pdr", "Latvia", "Lebanon", "Lesotho", "Liberia", "Libya", "Liechtenstein",
    "Lithuania", "Luxembourg", "Madagascar", "Malawi", "Malaysia", "Maldives", "Mali", "Malta",
    "Marshall Islands", "Mauritania", "Mauritius", "Mexico", "Micronesia, Fed. Sts.", "Moldova", "Monaco",
    "Mongolia", "Montenegro", "Morocco", "Mozambique", "Myanmar", "Namibia", "Nauru", "Nepal", "Netherlands",
    "New Zealand", "Nicaragua", "Niger", "Nigeria", "North Macedonia", "Norway", "Oman", "Pakistan", "Palau",
    "Panama", "Papua New Guinea", "Paraguay", "Peru", "Philippines", "Poland", "Portugal", "Qatar", "Romania",
    "Russian Federation", "Rwanda", "Samoa", "San Marino", "Sao Tome And Principe", "Saudi Arabia", "Senegal",
    "Serbia", "Seychelles", "Sierra Leone", "Singapore", "Slovak Republic", "Slovenia", "Solomon Islands",
    "Somalia", "South Africa", "South Sudan", "Spain", "Sri Lanka", "St. Kitts And Nevis", "St. Lucia",
    "St. Vincent And The Grenadines", "Sudan", "Suriname", "Sweden", "Switzerland", "Syrian Arab Republic",
    "Tajikistan", "Tanzania", "Thailand", "Timor-Leste", "Togo", "Tonga", "Trinidad And Tobago", "Tunisia",
    "Turkiye", "Turkmenistan", "Tuvalu", "Uganda", "Ukraine", "United Arab Emirates", "United Kingdom",
    "United States", "Uruguay", "Uzbekistan", "Vanuatu", "Venezuela, Rb", "Vietnam", "Yemen", "Zambia", "Zimbabwe"
]

cleaned_df = cleaned_df.filter(col("Country_Name").isin(actual_countries))


display(cleaned_df)


# COMMAND ----------

from pyspark.sql.functions import col, mean as _mean, stddev as _stddev, round as _round

# Numerical columns to convert into z scores
metrics_to_standardize = ["Arrivals", "Receipts", "Expenditures", "Population", "Balance"]

#mean and std dev for each column
stats = cleaned_df.select(
    *[ _mean(c).alias(f"{c}_mean") for c in metrics_to_standardize ],
    *[ _stddev(c).alias(f"{c}_stddev") for c in metrics_to_standardize ]
).collect()[0]

# Z Scores as another column
for c in metrics_to_standardize:
    mean_val = stats[f"{c}_mean"]
    std_val = stats[f"{c}_stddev"]
    
    if std_val != 0:
        cleaned_df = cleaned_df.withColumn(
            f"{c}_zscore",
            _round((col(c) - mean_val) / std_val, 4)
        )
    else:
        cleaned_df = cleaned_df.withColumn(f"{c}_zscore", lit(0.0))


display(cleaned_df)


# COMMAND ----------

# Define catalog/schema/table
catalog_name = "hive_metastore"  # or your custom catalog
schema_name = "tourism_analytics"
table_name = "tourism_final_with_zscores"

# Ensure the schema exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")

# Overwrite the table with new schema
cleaned_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.{schema_name}.{table_name}")


# COMMAND ----------

finaldf_csv = spark.table("hive_metastore.tourism_analytics.tourism_final_with_zscores")
finaldf_csv.createOrReplaceTempView("finaldf_csv")
import matplotlib.pyplot as plt
#Original SQL Query
top_countries_df = spark.sql("""
SELECT Country_Name,
       ROUND(AVG(Receipts / Expenditures), 2) AS AvgReceiptsToExpenditures
FROM finaldf_csv
WHERE Expenditures > 0
GROUP BY Country_Name
ORDER BY AvgReceiptsToExpenditures DESC
LIMIT 10
""")

#Convert to DataFrame
top_countries_pd = top_countries_df.toPandas()


#Sort the Vals for plot
top_countries_pd = top_countries_pd.sort_values(by="AvgReceiptsToExpenditures", ascending=True)

plt.figure(figsize=(10, 6))
plt.barh(top_countries_pd['Country_Name'], top_countries_pd['AvgReceiptsToExpenditures'], color='skyblue')
plt.xlabel('Avg Receipts to Expenditures Ratio')
plt.title('Top 10 Countries by Avg Tourist Receipts-to-Expenditures Ratio')
plt.grid(axis='x', linestyle='--', alpha=0.7)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --List of Countries with Population Decline Over Consecutive Years, ordered by the total change
# MAGIC SELECT a.Country_Name, 
# MAGIC        a.Year, 
# MAGIC        a.Population, 
# MAGIC        b.Year AS PrevYear, 
# MAGIC        b.Population AS PrevPopulation,
# MAGIC        b.Population - a.Population AS Total_Change
# MAGIC FROM finaldf_csv a
# MAGIC JOIN finaldf_csv b
# MAGIC   ON a.Country_Name = b.Country_Name
# MAGIC  AND a.Year = b.Year + 1
# MAGIC WHERE a.Population < b.Population
# MAGIC ORDER by Total_Change DESC;

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import lag, col, round as spark_round

# Define the window specification
window_spec = Window.partitionBy("Country_Name").orderBy("Year")

# Add lag columns for Receipts and Population
df_with_lags = finaldf_csv.withColumn("prev_receipts", lag("Receipts").over(window_spec)) \
                            .withColumn("prev_population", lag("Population").over(window_spec))

# Calculate growth percentages and filter out nulls for United States (or desired country)
result_df = df_with_lags \
    .withColumn("receipts_growth_pct", 
                spark_round(((col("Receipts") - col("prev_receipts")) / col("prev_receipts")) * 100, 2)) \
    .withColumn("population_growth_pct", 
                spark_round(((col("Population") - col("prev_population")) / col("prev_population")) * 100, 2)) \
    .filter(col("Country_Name") == 'United States') \
    .filter(col("prev_receipts").isNotNull() & col("prev_population").isNotNull()) \
    .select("Country_Name", "Year", "receipts_growth_pct", "population_growth_pct") \
    .orderBy("Year")

# Convert to Pandas DataFrame for plotting
pandas_df = result_df.toPandas()

import matplotlib.pyplot as plt
import seaborn as sns

# Plotting both growth percentages on the same line chart
plt.figure(figsize=(12, 6))

# Plot Receipts Growth Percentage
sns.lineplot(data=pandas_df, x="Year", y="receipts_growth_pct", label="Receipts Growth (%)", marker='o')

# Plot Population Growth Percentage
sns.lineplot(data=pandas_df, x="Year", y="population_growth_pct", label="Population Growth (%)", marker='o')

# Customize chart
plt.title("Receipts and Population Growth (%) Over Time (United States)")
plt.xlabel("Year")
plt.ylabel("Growth (%)")
plt.legend(title="Growth Types")
plt.grid(True)
plt.tight_layout()

# Show plot
plt.show()


# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns
# top 10 countries by arrivals per capita
top_10_countries = spark.sql("""
    SELECT 
        Country_Name,
        Year,
        ROUND(Arrivals / Population, 4) AS arrivals_per_capita
    FROM finaldf_csv
    WHERE 
        Year = 2019 -- We can change this to any year in our data. Be nice if this was user-editable on the chart
        AND Arrivals IS NOT NULL --Ignore nulls
        AND Population IS NOT NULL --Ignore nulls
        AND Population > 0
    ORDER BY arrivals_per_capita DESC
    LIMIT 10
""")
# Convert the result to a Pandas DataFrame for easy plotting
top_10_df = top_10_countries.toPandas()

# Set figure size
plt.figure(figsize=(10, 6))

# Plot bar chart
sns.barplot(x="arrivals_per_capita", y="Country_Name", data=top_10_df, palette="viridis")
plt.title("Top 10 Countries by Arrivals per Capita (2019)")#Need to alter if changing the year above
plt.xlabel("Arrivals per Capita")
plt.ylabel("Country Name")
plt.tight_layout()

# Show chart
plt.show()


# COMMAND ----------

# Receipts Share Percentage by Country 
receipts_share_df = spark.sql("""
WITH top_countries AS (
    SELECT Country_Name
    FROM   finaldf_csv
    WHERE  Receipts IS NOT NULL
    GROUP BY Country_Name
    ORDER BY  SUM(Receipts) DESC
    LIMIT 5
),
total_receipts AS (
    SELECT 
        Year,
        SUM(Receipts) AS total_receipts
    FROM finaldf_csv
    WHERE  Receipts IS NOT NULL
    GROUP BY   Year
)
SELECT 
    f.Country_Name,
    f.Year,
    f.Receipts,
    ROUND(f.Receipts / t.total_receipts * 100, 2) AS receipts_share_pct
FROM finaldf_csv f
JOIN total_receipts t
ON 
    f.Year = t.Year
WHERE     f.Country_Name IN (SELECT Country_Name FROM top_countries)
    AND f.Receipts IS NOT NULL
    AND f.Year BETWEEN 2018 AND 2022 --This can be adjusted for other years
ORDER BY 
    f.Year, f.Country_Name
""")
# Convert to a Pandas DataFrame
receipts_share_pandas = receipts_share_df.toPandas()
import plotly.express as px

# Create table 
pivot_df = receipts_share_pandas.pivot_table(
    index="Year", 
    columns="Country_Name", 
    values="receipts_share_pct", 
    aggfunc="sum", 
    fill_value=0  # Replace NaN with 0 for missing data
)

# Plot the chart
fig = px.area(
    pivot_df, 
    x=pivot_df.index, 
    y=pivot_df.columns, 
    title="Receipts Share Percentage by Country (2018-2022)", 
    labels={"value": "Receipts Share (%)", "Year": "Year"},
    template="plotly",
)
fig.update_layout(
    xaxis_title="Year",
    yaxis_title="Receipts Share (%)",
    legend_title="Country Name",
    margin={"r": 0, "t": 40, "l": 0, "b": 0},
)

# Show it
fig.show()



# COMMAND ----------

# ==========================================================
# Tourism Prediction using Machine Learning Models
# Authors: Claudia (primary code generator) and Derek (final reviewer/editor)
# Date: [4.26.25]
# Description:
#   This script loads the tourism dataset, trains five machine
#   learning models, evaluates their performance, and identifies
#   the best-performing model based on accuracy.
# ==========================================================

# -----------------------------------------
# Step 1: Load Data
# -----------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import when
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import (
    LogisticRegression,
    RandomForestClassifier,
    GBTClassifier,
    LinearSVC,
    MultilayerPerceptronClassifier
)
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

spark = SparkSession.builder.appName("TourismPrediction").getOrCreate()

# Load Delta table
finaldf_csv = spark.table("hive_metastore.tourism_analytics.tourism_final_with_zscores")

# Binary label: 1 if Receipts > Expenditures, else 0
finaldf_csv = finaldf_csv.withColumn(
    "label",
    when(finaldf_csv["Receipts"] > finaldf_csv["Expenditures"], 1).otherwise(0)
)

# -----------------------------------------
# Step 2: Prepare Data
# -----------------------------------------
# Feature columns (excluding non-numerics or IDs)
feature_columns = [c for c in finaldf_csv.columns if c not in ["label", "Country_Name", "Index", "Year"]]

assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel")

data = labelIndexer.fit(finaldf_csv).transform(finaldf_csv)
data = assembler.transform(data)

final_data = data.select("features", "indexedLabel")

train_data, test_data = final_data.randomSplit([0.7, 0.3], seed=42)

# -----------------------------------------
# Step 3: Initialize Models
# -----------------------------------------
lr = LogisticRegression(labelCol="indexedLabel", featuresCol="features")
rf = RandomForestClassifier(labelCol="indexedLabel", featuresCol="features")
gbt = GBTClassifier(labelCol="indexedLabel", featuresCol="features")
svc = LinearSVC(labelCol="indexedLabel", featuresCol="features")
mlp = MultilayerPerceptronClassifier(
    labelCol="indexedLabel", featuresCol="features",
    layers=[len(feature_columns), 5, 2], maxIter=100
)

# -----------------------------------------
# First Block: TEST Accuracy for All 5 Models
# -----------------------------------------
evaluator = MulticlassClassificationEvaluator(labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy")

# Logistic Regression
lr_model = lr.fit(train_data)
lr_test_pred = lr_model.transform(test_data)
lr_test_acc = evaluator.evaluate(lr_test_pred)
print(f"Logistic Regression Test Accuracy: {lr_test_acc:.4f}")

# Random Forest
rf_model = rf.fit(train_data)
rf_test_pred = rf_model.transform(test_data)
rf_test_acc = evaluator.evaluate(rf_test_pred)
print(f"Random Forest Test Accuracy: {rf_test_acc:.4f}")

# Gradient Boosted Trees
gbt_model = gbt.fit(train_data)
gbt_test_pred = gbt_model.transform(test_data)
gbt_test_acc = evaluator.evaluate(gbt_test_pred)
print(f"GBT Test Accuracy: {gbt_test_acc:.4f}")

# Linear SVC
svc_model = svc.fit(train_data)
svc_test_pred = svc_model.transform(test_data)
svc_test_acc = evaluator.evaluate(svc_test_pred)
print(f"Linear SVC Test Accuracy: {svc_test_acc:.4f}")

# MLP
mlp_model = mlp.fit(train_data)
mlp_test_pred = mlp_model.transform(test_data)
mlp_test_acc = evaluator.evaluate(mlp_test_pred)
print(f"MLP Test Accuracy: {mlp_test_acc:.4f}")

# -----------------------------------------
# Second Block: TRAINING Accuracy for Select Models
# -----------------------------------------

# Logistic Regression
lr_train_pred = lr_model.transform(train_data)
lr_train_acc = evaluator.evaluate(lr_train_pred)
print(f"Logistic Regression Train Accuracy: {lr_train_acc:.4f}")

# Random Forest
rf_train_pred = rf_model.transform(train_data)
rf_train_acc = evaluator.evaluate(rf_train_pred)
print(f"Random Forest Train Accuracy: {rf_train_acc:.4f}")

# Gradient Boosted Trees – Optional
print("Gradient Boosted Trees Train Accuracy: N/A")

# Linear SVC – Optional
print("Linear SVC Train Accuracy: N/A")

# MLP
mlp_train_pred = mlp_model.transform(train_data)
mlp_train_acc = evaluator.evaluate(mlp_train_pred)
print(f"MLP Train Accuracy: {mlp_train_acc:.4f}")
