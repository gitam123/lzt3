# Databricks notebook source
# MAGIC %md
# MAGIC # Reading CSV files

# COMMAND ----------

df=spark.read.format('CSV').option('inferSchmea',True).option("header",True).option("sep",',').load('dbfs:/FileStore/Daily_Household_Transactions.csv')
display(df)

# COMMAND ----------

df=spark.read.format("CSV").options(inferSchmea='True',header='True',sep=',').load("dbfs:/FileStore/Daily_Household_Transactions.csv")
display(df)


# COMMAND ----------

df=spark.read.format("CSV").options(inferSchmea='True',header='True',sep=',').load(["dbfs:/FileStore/Daily_Household_Transactions.csv","dbfs:/FileStore/weather_by_cities.csv"])
display(df)

# COMMAND ----------

df=spark.read.format("CSV").options(inferSchmea='True',header='True',sep=',').load("dbfs:/FileStore/tables/archive (1)/")
display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,FloatType,DateType

# COMMAND ----------

schema_def=StructType([StructField('year',IntegerType(),True),
                       StructField('make',StringType(),True),
                       StructField('model',StringType(),True),
                       StructField('trim',StringType(),True),
                       StructField('transmission',StringType(),True),
                       StructField('vin',StringType(),True),
                       StructField('state',StringType(),True),
                       StructField('condition',StringType(),True),
                       StructField('odometer',StringType(),True),
                       StructField('color',StringType(),True),
                       StructField('interior',StringType(),True),
                       StructField('seller',StringType(),True),
                       StructField('mmr',StringType(),True),
                       StructField('sellingprice',FloatType(),True),
                       StructField('saledate',DateType(),True),
                       ])

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df=spark.read.format("CSV").options(header='True',sep=',').schema(schema_def).load("dbfs:/FileStore/tables/archive (1)/car_prices.csv")
display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

schema_alt='year integer,make string,model string,trim string,transmission string,vin string,state string,condition string,odometer string,color string,interior string,seller string,mmr string,sellingprice float,saledate string '

# COMMAND ----------

df=spark.read.format("CSV").options(header='True',sep=',').schema(schema_alt).load("dbfs:/FileStore/tables/archive (1)/car_prices.csv")
display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # Filter

# COMMAND ----------

employee=[(1,'Naveen','M',23, 2),
          (2,'sai','M',26, 4),
          (3,'Pavan','M',25, 3),
          (4,'Raji','F',28, 5),
          (5,'Mamatha','F',30, 10),
          (6,'Balaji','M',32, 10),
          ]
schema_tab='Serial_No INTEGER, Name STRING, Gender STRING, Age INTEGER, Experince INTEGER'

# COMMAND ----------

data=spark.createDataFrame(data=employee, schema=schema_tab)
display(data)

# COMMAND ----------

display(data.filter(data.Gender=='M'))

# COMMAND ----------

display(data.filter(data.Gender=='F'))

# COMMAND ----------

schema_alt='year integer,make string,model string,trim string,transmission string,vin string,state string,condition string,odometer string,color string,interior string,seller string,mmr string,sellingprice float,saledate string '
car_df=spark.read.format("CSV").options(header='True',sep=',').schema(schema_alt).load("dbfs:/FileStore/tables/archive (1)/car_prices.csv")
display(car_df)

# COMMAND ----------

car_df.printSchema()

# COMMAND ----------

display(car_df.filter(car_df.year==2020))

# COMMAND ----------

display( car_df.filter((car_df.model == 'Sorento') & (car_df.make == 'Kia')))

# COMMAND ----------

display(car_df.filter((car_df.year==2014) & (car_df.model=='Sorento')))

# COMMAND ----------

columns_list = car_df.columns
print(columns_list)

# COMMAND ----------

# DBTITLE 1,Null Values
from pyspark.sql.functions import col
fields = []
# Assuming columns_list contains the list of column names in car_df
for col_name in columns_list:
    # Filter the DataFrame for rows where the column is null
    filtered_df = car_df.filter(col(col_name).isNull())
    if filtered_df.count()>0:
        print(f"Null values in column '{col_name}':{filtered_df.count()}")

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import col

# Initialize an empty list to store the data tuples
data_t = []

# Assuming columns_list contains the list of column names in car_df
for col_name in columns_list:
    # Filter the DataFrame for rows where the column is null
    filtered_df = car_df.filter(col(col_name).isNull())
    
    # Append a tuple containing the column name and the count of null values
    data_t.append(Row(column_name=col_name, null_count=filtered_df.count()))

# Create a DataFrame using the collected data tuples
data_1 = spark.createDataFrame(data=data_t)

# Display the DataFrame
display(data_1)


# COMMAND ----------

# DBTITLE 1,Starts With
display(car_df.filter(car_df.make.startswith('B')))

# COMMAND ----------

# DBTITLE 1,Endswith
display(car_df.filter(car_df.make.endswith('w')))

# COMMAND ----------

# DBTITLE 1,Contains
display(car_df.filter(car_df.make.contains('Acura')))

# COMMAND ----------

# DBTITLE 1,isin
display(car_df.filter(car_df.year.isin(2014)))

# COMMAND ----------

display(car_df.filter(car_df.make.isin('BMW')))

# COMMAND ----------

display(car_df.filter(~car_df.make.isin('Kia','BMW')))

# COMMAND ----------

display(car_df.filter(car_df.make.isin('Kia','BMW')))

# COMMAND ----------

# DBTITLE 1,Like
display(car_df.filter(car_df.make.like('B%')))

# COMMAND ----------

display(car_df.filter(car_df.make.like('%a')))

# COMMAND ----------

display(car_df.filter(car_df.make.like('%a%')))

# COMMAND ----------

# MAGIC %md
# MAGIC # Add, Rename and drop columns

# COMMAND ----------

display(data)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Add column

# COMMAND ----------

from pyspark.sql.functions import lit         #lit: is used to fill column with constant value.
data = data.withColumn('DOJ_Year', lit(2000))
display(data)

# COMMAND ----------

# for performing the arithematic operations.
from pyspark.sql.functions import col
constant_factor = 2024
data = data.withColumn('DOJ_Year',constant_factor-col('Experince'))
display(data)

# COMMAND ----------

# for performing the arithematic operations.
constant_factor = 2024
data = data.withColumn('DOB_Year',constant_factor-col('Age'))
display(data)

# COMMAND ----------

#instead of col we can use dataframe stored variable by using dot notation.
constant_factor = 2024
data = data.withColumn('DOJ_Age_in_Year',data.DOJ_Year-data.DOB_Year)
display(data)

# COMMAND ----------

# MAGIC %md
# MAGIC #### we can use concat function to merge two cloumns.
# MAGIC from pyspark.sql.functions import concat.
# MAGIC df=data.withcolumn('Name',concat('first_name',lit(" "),'last_name'))
# MAGIC lit(" ") will mention the space.
# MAGIC it will merge the first name and last name and creates the new column Name.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Rename column

# COMMAND ----------

data.withColumnRenamed('Experince','Experience')
display(data)

# COMMAND ----------

display(data)

# COMMAND ----------

data=data.withColumnRenamed('Experince','Exp').withColumnRenamed('DOJ_Year','Joining_Year')

# COMMAND ----------

display(data)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Drop column

# COMMAND ----------

data=data.drop('DOB_Year')

# COMMAND ----------

display(data)

# COMMAND ----------

data=data.drop('DOJ_Age_in_Year')
display(data)

# COMMAND ----------


