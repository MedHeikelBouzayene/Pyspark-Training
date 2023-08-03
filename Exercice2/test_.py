from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType, LongType
from pyspark.sql.functions import col, year, upper, sum, count
import pytest
from Exercice2 import *
from datetime import date
from chispa import assert_df_equality

def test_date_type(spark):
    df_paths = ['C:/Users/mhbouzayenne/Desktop/UDEMY COURSE/Pyspark-Training/Exercice2/df1.csv']   
    df1 = DF(spark, df_paths[0])
    df1_changed_type = df1.changeColumnType("date", DateType)

    input_data = [[1, date(2021, 1, 1), 100, 'Paris'], 
                  [3, date(2021, 3, 10), 200, 'New York'],
                  [2, date(2021, 2, 15), 50, 'London'],
                  [4, date(2021, 4, 5), 30, 'Paris']
                 ]
    input_schema: StructType = StructType(
                [
                    StructField('id', IntegerType()), 
                    StructField('date', DateType()),
                    StructField('amount', IntegerType()),
                    StructField('city', StringType())
                ]
    )

    expected_result = spark.createDataFrame(data = input_data, schema = input_schema)

    assert_df_equality(expected_result, df1_changed_type.__get_df__(), ignore_row_order=True, ignore_schema=False)
    #assert isinstance(df1_changed_type.schema()['date'].dataType, DateType)

def test_column_name_changed(spark):
    df_paths = ['C:/Users/mhbouzayenne/Desktop/UDEMY COURSE/Pyspark-Training/Exercice2/df1.csv']
    df1 = DF(spark, df_paths[0])

    df1_changed_name = df1.changeColumnName('amount', 'total_amount')
    
    
    input_data = [[1, date(2021, 1, 1), 100, 'Paris'], 
                  [3, date(2021, 3, 10), 200, 'New York'],
                  [2, date(2021, 2, 15), 50, 'London'],
                  [4, date(2021, 4, 5), 30, 'Paris']
                 ]
    input_schema: StructType = StructType(
                [
                    StructField('id', IntegerType()), 
                    StructField('date', DateType()),
                    StructField('total_amount', IntegerType()),
                    StructField('city', StringType())
                ]
    )

    expected_result = spark.createDataFrame(data = input_data, schema = input_schema)
    assert_df_equality(expected_result, df1_changed_name.__get_df__(), ignore_row_order=True, ignore_schema=False)

    #assert ('total_amount' in df1_changed_name.columns() and 'amount' not in df1_changed_name.columns()) 

def test_filter(spark):
    df_paths = ['C:/Users/mhbouzayenne/Desktop/UDEMY COURSE/Pyspark-Training/Exercice2/df1.csv']
    df1 = DF(spark, df_paths[0])
    df1_filtered = df1.filter_sup('amount', 50)

    input_data = [[1, date(2021, 1, 1), 100, 'Paris'], [3, date(2021, 3, 10), 200, 'New York']]
    input_schema: StructType = StructType(
                [
                    StructField('id', IntegerType()), 
                    StructField('date', DateType()),
                    StructField('amount', IntegerType()),
                    StructField('city', StringType())
                ]
    )

    expected_result = spark.createDataFrame(data = input_data, schema = input_schema)
    assert_df_equality(expected_result, df1_filtered.__get_df__(), ignore_row_order=True, ignore_schema=False)

def test_add_year(spark):
    df_paths = ['C:/Users/mhbouzayenne/Desktop/UDEMY COURSE/Pyspark-Training/Exercice2/df1.csv']
    df1 = DF(spark, df_paths[0])
    df1_with_year_column = df1.addYearColumn('year', 'date')

    input_data = [[1, date(2021, 1, 1), 100, 'Paris', 2021], 
                  [3, date(2021, 3, 10), 200, 'New York', 2021],
                  [2, date(2021, 2, 15), 50, 'London', 2021],
                  [4, date(2021, 4, 5), 30, 'Paris', 2021]
                 ]
    input_schema: StructType = StructType(
                [
                    StructField('id', IntegerType()), 
                    StructField('date', DateType()),
                    StructField('amount', IntegerType()),
                    StructField('city', StringType()),
                    StructField('year', IntegerType())
                ]
    )

    expected_result = spark.createDataFrame(data = input_data, schema = input_schema)
    assert_df_equality(expected_result, df1_with_year_column.__get_df__(), ignore_row_order=True, ignore_schema=False)

def test_city_upper(spark):
    df_paths = ['C:/Users/mhbouzayenne/Desktop/UDEMY COURSE/Pyspark-Training/Exercice2/df1.csv']
    df1 = DF(spark, df_paths[0])
    df1_city_upper = df1.changeToUpper('city')

    input_data = [[1, date(2021, 1, 1), 100, 'PARIS'], 
                  [3, date(2021, 3, 10), 200, 'NEW YORK'],
                  [2, date(2021, 2, 15), 50, 'LONDON'],
                  [4, date(2021, 4, 5), 30, 'PARIS']
                 ]
    input_schema: StructType = StructType(
                [
                    StructField('id', IntegerType()), 
                    StructField('date', DateType()),
                    StructField('amount', IntegerType()),
                    StructField('city', StringType())
                ]
    )

    expected_result = spark.createDataFrame(data = input_data, schema = input_schema)
    assert_df_equality(expected_result, df1_city_upper.__get_df__(), ignore_row_order=True, ignore_schema=False)


def test_orders_join_customers_select(spark):
    df_paths = ['C:/Users/mhbouzayenne/Desktop/UDEMY COURSE/Pyspark-Training/Exercice2/df_orders.csv',
               'C:/Users/mhbouzayenne/Desktop/UDEMY COURSE/Pyspark-Training/Exercice2/df_customers.csv']
    df_orders = DF(spark, df_paths[0])
    df_customers = DF(spark, df_paths[1])
    dfOrders_join_dfCustomers = df_orders.left_join(df_customers, ['customer_id'])
    dfOrders_join_dfCustomers = dfOrders_join_dfCustomers.select(['customer_name', 'customer_country'])

    input_data = [['Alice', 'USA'], 
                  ['Bob', 'UK'],
                  ['Alice', 'USA'],
                  ['Charlie', 'France']
                 ]
    input_schema: StructType = StructType(
                [
                    StructField('customer_name', StringType()), 
                    StructField('customer_country', StringType()),
                ]
    )

    expected_result = spark.createDataFrame(data = input_data, schema = input_schema)
    assert_df_equality(expected_result, dfOrders_join_dfCustomers.__get_df__(), ignore_row_order=True, ignore_schema=False)


def test_total_amount_per_country(spark):
    df_paths = ['C:/Users/mhbouzayenne/Desktop/UDEMY COURSE/Pyspark-Training/Exercice2/df1.csv']
    df1 = DF(spark, df_paths[0])
    amount_of_orders_by_country = df1.groupBy_and_sum(['city'], 'amount', 'total_amount_per_country')

    input_data = [['Paris', 130], 
                    ['New York', 200],
                    ['London', 50],
                 ]
    input_schema: StructType = StructType(
                [
                    StructField('city', StringType()),
                    StructField('total_amount_per_country', LongType()),
                ]
    )

    expected_result = spark.createDataFrame(data = input_data, schema = input_schema)
    assert_df_equality(expected_result, amount_of_orders_by_country.__get_df__(), ignore_row_order=True, ignore_schema=False)



def test_number_of_orders_by_client(spark):
    df_paths = ['C:/Users/mhbouzayenne/Desktop/UDEMY COURSE/Pyspark-Training/Exercice2/df_orders.csv']
    df_orders = DF(spark, df_paths[0])
    number_of_orders_by_client_greater_than_1 = df_orders.groupBy_condition(['customer_id'], 'total_orders', 1)
    input_data = [[1001, 2]]
    input_schema: StructType = StructType(
                [
                    StructField('customer_id', IntegerType()), 
                    StructField('total_orders', LongType(), False),
                ]
    )

    expected_result = spark.createDataFrame(data = input_data, schema = input_schema)
    assert_df_equality(expected_result, number_of_orders_by_client_greater_than_1.__get_df__(), ignore_row_order=True, ignore_schema=False)