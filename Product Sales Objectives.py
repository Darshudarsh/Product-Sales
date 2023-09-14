# Databricks notebook source
# MAGIC %md
# MAGIC ### Problem Statement:
# MAGIC
# MAGIC ### Given 3 datasets related to online products sales in the year 2019 for months Jan, Feb and March for a given store. 
# MAGIC Need to do data engineering using PySpark or Spark(Scala/Java) on these datasets to obtain the following objectives
# MAGIC
# MAGIC 1. Cleanse the data removing blank rows 
# MAGIC 2. Get the date on which max sales was done by product in these 3 months
# MAGIC 3. Get the date on which max sales was done for all products in these 3 months
# MAGIC 4. Get the average sales value for each product in these 3 months
# MAGIC 5. Create a combined dataset merging all these 3 datasets with order by date in desc order and add a new column which is “salesdiff” where this column will contain the difference of the sales in the current row (current date of that row) and the next row (previous date of that row, as the date columns are sorted by desc) grouped on the product
# MAGIC For the last row, next row will be blank so consider the sales as 0
# MAGIC 6. Get the orderId and purchase address details who made max sales in all the 3 months
# MAGIC 7. Extract city from the purchase address column which is 2nd element in , delimited separated string and determine the city from where more orders came in all these 3 months
# MAGIC 8. Get the total order count details for each city in all the 3 months
# MAGIC
# MAGIC ### Note: 
# MAGIC 1. Sales value calculated by qty * price
# MAGIC 2. orders count can be determined based on orderId(one orderId means 1 order)
# MAGIC

# COMMAND ----------

client_id = dbutils.secrets.get(scope='sales-scope', key='sales-app-client-id')
tenant_id =  dbutils.secrets.get(scope='sales-scope', key='sales-app-tenant-id')
client_secret =  dbutils.secrets.get(scope='sales-scope', key='sales-app-client-secret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@formula1nasa.dfs.core.windows.net/",
  mount_point = "/mnt/formula1nasa/demo",
  extra_configs = configs)

# COMMAND ----------

# DBTITLE 1,Azure Data Lake Storage Mounted
# Uploaded files to ADLS Gen2 and is mounted in databricks to read the data files 
demo_folder_path = '/mnt/formula1nasa/demo'

# COMMAND ----------

# DBTITLE 1,Reading all the source files from ADLS 
# January 2019 src file read
df_sales_jan = spark.read.format('csv')\
.option("header", True) \
.option("inferSchema", True) \
.csv(f"{demo_folder_path}/sales/Sales_January_2019.csv")

# Febuaray 2019 src file read
df_sales_feb = spark.read.format('csv')\
.option("header", True) \
.option("inferSchema", True) \
.csv(f"{demo_folder_path}/sales/Sales_February_2019.csv")

# March 2019 src file read
df_sales_mar = spark.read.format('csv')\
.option("header", True) \
.option("inferSchema", True) \
.csv(f"{demo_folder_path}/sales/Sales_March_2019.csv")

print(df_sales_jan.count())
print(df_sales_feb.count())
print(df_sales_mar.count())

# COMMAND ----------

from pyspark.sql.functions import col,to_date,row_number,sum,desc,month,avg,lead,split,count
from pyspark.sql.window import Window

# Combined all the 3 src files to one dataframe - df_sales
df_temp = df_sales_jan.union(df_sales_feb)
df_sales = df_temp.union(df_sales_mar)

# Additional transformations required

# Replacing Spaces from column names to '_' (underscore)
df_sales = df_sales.select([col(c).alias(c.replace(' ', '_')) for c in df_sales.columns])

# Refomarting date by ignore minutes and hours
df_sales = df_sales.withColumn("Order_Date_Formatted", to_date(df_sales["Order_Date"], "MM/dd/yy HH:mm"))

# Calculating sales where sales = Quantity * Price per unit
df_sales = df_sales.withColumn("sales",df_sales.Quantity_Ordered*df_sales.Price_Each)

# Data for April (4th) month is also present hence filtering it as per requirement
df_sales = df_sales.filter(month("Order_Date_Formatted").isin(1,2,3))

print(df_sales.count())

# COMMAND ----------

# DBTITLE 1,1. Cleanse the data removing blank rows
df_sales = df_sales.dropna()
print(df_sales.count())

# COMMAND ----------

# DBTITLE 1,2. Get the date on which max sales was done by product in these 3 months 
# Calculating the sum of sales for each product and order date
total_sales_df = df_sales.groupBy("product", "Order_Date_Formatted") \
                   .agg(sum("sales").alias("total_sales"))

# Partitioning by Product and ordering by total sales in descending Order
window_spec = Window.partitionBy("product").orderBy(desc("total_sales"))

# Applying row_number() to get the max sales
result_df = total_sales_df.withColumn("rnk", row_number().over(window_spec))

# Extracting the max date for max sales for each product and dropping unused columns
result_df = result_df.filter('rnk = 1').select("Product",col("Order_Date_Formatted").alias("Max_Order_Date")).drop("rnk")

display(result_df)

# COMMAND ----------

# DBTITLE 1,3. Get the date on which max sales was done for all products in these 3 months
# Calculating the sum of sales for each order date
total_sales_df = df_sales.groupBy("Order_Date_Formatted") \
                   .agg(sum("sales").alias("total_sales"))

# Window specification by Ordering by total sales in descending order
window_spec = Window.orderBy(desc("total_sales"))

# Applying row_number() to get the max sales
result_df = total_sales_df.withColumn("rnk", row_number().over(window_spec))

# Extracting the max date for max sales for all products and dropping unused columns
result_df = result_df.filter('rnk = 1').select(col("Order_Date_Formatted").alias("Max_Order_Date")).drop("rnk")

display(result_df)

# COMMAND ----------

# DBTITLE 1,4. Get the average sales value for each product in these 3 months
# Calculating the average of sales for each product
total_sales_df = df_sales.groupBy("product") \
                   .agg(avg("sales").alias("total_sales"))

display(total_sales_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5. Create a combined dataset merging all these 3 datasets with order by date in desc order and add a new column which is “salesdiff” where this column will contain the difference of the sales in the current row (current date of that row) and the next row (previous date of that row, as the date columns are sorted by desc) grouped on the product For the last row, next row will be blank so consider the sales as 0

# COMMAND ----------

# Partitioning by Product and Ordering by Order Data in descending Order
window_spec = Window.partitionBy("product").orderBy(desc("Order_Date"))

# Used lead function to calulcate the sales difference between current and previous order date sales, 3rd argument in lead functions specifies the default value
df_sales_difference = df_sales.withColumn("salesdiff", col("sales") - lead("sales",1,0).over(window_spec)).drop("Order_Date_Formatted")

display(df_sales_difference)

# COMMAND ----------

# DBTITLE 1,6. Get the orderId and purchase address details who made max sales in all the 3 months
# Caluclating the total sales based on Order ID and Purchase Address
total_sales_df = df_sales.groupBy("Order_ID","Purchase_Address") \
                   .agg(sum("sales").alias("total_sales"))

# Window specification by Ordering by total sales in descending Order
window_spec = Window.orderBy(desc("total_sales"))

# Applying row_number() to get the max sales 
result_df = total_sales_df.withColumn("rnk", row_number().over(window_spec))

# Extracting the Order Id & Purchase Address for max sales for all products and dropping unused columns
result_df = result_df.filter('rnk = 1').select("Order_ID","Purchase_Address").drop("rnk")

display(result_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 7. Extract city from the purchase address column which is 2nd element in , delimited separated string and determine the city from where more orders came in all these 3 months

# COMMAND ----------

# Splitting the Purchase Address by ',' and extracting the city using index as 1
df_city_sales = df_sales.withColumn("City", split("Purchase_Address", ",")[1])

# Grouping by extracted city and counting the orders using Order ID
max_orders_df = df_city_sales.groupBy("City") \
                   .agg(count("Order_ID").alias("count_of_orders"))

# Window Specification to Order by the count of orders from each city
window_spec = Window.orderBy(desc("count_of_orders"))

# Applying Row_number to get the max orders 
result_df = max_orders_df.withColumn("rnk", row_number().over(window_spec))

# Extracting the City for max purchase orders and dropping unused columns
result_df = result_df.filter('rnk = 1').select("City").drop("rnk")

display(result_df)

# COMMAND ----------

# DBTITLE 1,8. Get the total order count details for each city in all the 3 months
# Since already the City with Count of orders were calculated in cmd 13 I have reused same df 
display(max_orders_df)
