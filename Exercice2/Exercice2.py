from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType, LongType
from pyspark.sql.functions import col, year, upper, sum, count

class DF:
    def __init__(self, spark = None, path = None, schema = None, dataFrame = None):
        if spark:    
            if schema:
                self.df = spark.read.options(header = 'True', inferSchema = 'False').schema(schema).csv(path)
            else:
                self.df = spark.read.options(header = 'True', inferSchema = 'True').csv(path)
        else:
            self.df = dataFrame
        
        

    def show(self):
        self.df.show()
        self.df.cache()

    def __get_column__(self, column_name):
        return self.df[column_name]
    
    def __get_df__(self):
        return self.df
    
    def count(self):
        self.df.count()
        self.df.cache()
    
    def printSchema(self):
        self.df.printSchema()
    
    def schema(self):
        return self.df.schema
    
    def columns(self):
        return self.df.columns

    def changeColumnType(self, column_name, newType):
        print(type(self.df))
        new_df = DF(dataFrame = self.df.withColumn(column_name, col(column_name).cast(newType())))
        return new_df

    def changeColumnName(self, column_name, new_column_name):
        new_df = DF(dataFrame = self.df.withColumnRenamed(column_name, new_column_name))
        return new_df
    
    def filter_sup(self, column, val):
        new_df = DF(dataFrame = self.df.filter(col(column) > val))
        return new_df

    def addYearColumn(self, new_column_name, date_column):
        new_df = DF(dataFrame = self.df.withColumn(new_column_name, year(col(date_column))))
        return new_df
    
    def changeToUpper(self, column_name):
        new_df = DF(dataFrame = self.df.withColumn(column_name, upper(col(column_name))))
        return new_df

    def left_join(self, other_df, join_key):
        new_df = DF(dataFrame = self.df.join(other_df.df, join_key, 'left'))
        return new_df
    
    def select(self, columns_selected):
        new_df = DF(dataFrame = self.df.select(*columns_selected))
        return new_df
    
    def groupBy_and_sum(self, columns_to_groupBy, column_to_sum, sum_column_name):
        new_df = DF(dataFrame = self.df.groupBy(columns_to_groupBy).agg(sum(column_to_sum).alias(sum_column_name)))
        return new_df

    def groupBy_condition(self, columns_to_groupBy, condition_column_name, condition):
        new_df = DF(
                    dataFrame = self.df.groupBy(columns_to_groupBy) \
                    .agg(count('*').alias(condition_column_name)) \
                    .where(col(condition_column_name) > condition)
                   )
        return new_df

    def write(self, path, mode = 'error'):
        self.df.write.mode(mode).options(header = 'True').csv(path)

if __name__ == "__main__":
    spark = (SparkSession.builder \
        .appName("Exercice 1") \
        .getOrCreate())
    
    df_paths = [
                'C:/Users/mhbouzayenne/Desktop/UDEMY COURSE/Pyspark-Training/Exercice2/df1.csv', 
                'C:/Users/mhbouzayenne/Desktop/UDEMY COURSE/Pyspark-Training/Exercice2/df_orders.csv',
                'C:/Users/mhbouzayenne/Desktop/UDEMY COURSE/Pyspark-Training/Exercice2/df_customers.csv'
               ]
    
    #Question 1
    df1 = DF(spark, df_paths[0])
    df_orders = DF(spark, df_paths[1])
    df_customers = DF(spark, df_paths[2])
    
    #Question 2
    df1_changed_type = df1.changeColumnType("date", DateType)
    
    df1_changed_name = df1.changeColumnName('amount', 'total_amount')

    df1_filtered = df1.filter_sup('amount', 50)

    df1_with_year_column = df1.addYearColumn('year', 'date')

    df1_city_upper = df1.changeToUpper('city')

    df1.show()
    df_orders.show()
    df_customers.show()
    df1_changed_type.show()
    df1_changed_name.show()
    df1_filtered.show()
    df1_with_year_column.show()
    df1_city_upper.show()
    
    #Question 3
    dfOrders_join_dfCustomers = df_orders.left_join(df_customers, ['customer_id'])
    dfOrders_join_dfCustomers = dfOrders_join_dfCustomers.select(['customer_name', 'customer_country'])
    dfOrders_join_dfCustomers.show()

    amount_of_orders_by_country = df1.groupBy_and_sum(['city'], 'amount', 'total_amount_per_country')
    amount_of_orders_by_country.show()

    number_of_orders_by_client_greater_than_1 = df_orders.groupBy_condition(['customer_id'], 'total_orders', 1)
    number_of_orders_by_client_greater_than_1.show()
    number_of_orders_by_client_greater_than_1.write('C:/Users/mhbouzayenne/Desktop/UDEMY COURSE/Pyspark-Training/Exercice2/output', 
                                                    'overwrite'
                                                   )
    
    
    spark.stop()