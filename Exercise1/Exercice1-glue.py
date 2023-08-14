from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType, LongType
# COMMAND ----------


# COMMAND ----------

class DF:
    def __init__(self, spark, path, schema = None):
        if schema:
            self.df = spark.read.options(header = 'True', inferSchema = 'False').schema(schema).parquet(path)
        else:
            self.df = spark.read.options(header = 'True', inferSchema = 'True').parquet(path)
    
    def show(self):
        self.df.show()
        self.df.cache()
    
    def count(self):
        self.df.count()
        self.df.cache()

    def columns_intersection(self, other_df):
        return [x for x in self.df.columns if x in other_df.df.columns]
    
    def left_join(self, other_df, join_key):
        self.df = self.df.join(other_df.df, join_key, 'left')
    
    def column_rename(self, old_name, new_name):
        self.df = self.df.withColumnRenamed(old_name, new_name)
    
    def print_schema(self):
        self.df.printSchema()
    
    #def display(self):
    #    display(self.df)

    def drop(self, columns):
        self.df = self.df.drop(*columns)
    
    def write(self, path, mode = 'error'):
        self.df.write.mode(mode).options(header = 'True').parquet(path)
    
    def take(self, n = None):
        if n:
            return self.df.take(n)
        else:
            return self.df.take()

# COMMAND ----------

class DataSet:
    def __init__(self, dfs, keys):
        self.df = dfs[0]
        for i in range(1, len(dfs)):
            #save common columns that are not keys to remove them after the join operation
            columns_intersection = self.df.columns_intersection(dfs[i])
            columns_intersection_and_not_keys = [dfs[i].df[x] for x in columns_intersection if (x not in keys[i - 1])]
            self.df.left_join(dfs[i], keys[i - 1])
            if len(columns_intersection_and_not_keys):
                self.df.drop(tuple(columns_intersection_and_not_keys))
    
    def show(self):
         self.df.show()
    
    def write(self, path, mode = 'error'):
        self.df.write(path, mode)
    def take(self, n):
        return self.df.take(n)
    
# COMMAND ----------

DATASET_SCHEMA: StructType = StructType(
    [
        StructField('N_APPLIC_INFQ', IntegerType()),
        StructField('C_PART_REFER', StringType()),
        StructField('C_MAST_AGREM_REFER', StringType()),
        StructField("C_ACT_TYPE", StringType()),
        StructField("D_CRDT_COMMITTEE_APRV", DateType()),
        StructField("CENMES", IntegerType()),
        StructField("L_MAST_AGREM_NAME", StringType()),
        StructField("C_ACT_MNG_STG", StringType()),
        StructField("N_MAST_AGREM_VALI_PER", IntegerType()),
        StructField("L_ACT_TYPE", StringType()),
        StructField('C_ROLE', StringType()),
        StructField('D_STR_ACTR_AGREM', DateType()),
        StructField('D_END_ACTR_AGREM', DateType()),
        StructField('EVENTDATE',DateType()),
        StructField("C_M", StringType()),
        StructField("M_ORIG", LongType()), #if we use FloatType, we have an error cuz it is registered as INT64 in the parquet file metadata.
        StructField("M_ORIG_SHAR", LongType()),#if we use FloatType, we have an error cuz it is registered as INT64 in the parquet file metadata.
        StructField("L_THIR_PART_NAME", StringType()),
    ]   
)

MAAG_REPA_RROL_LINKED_SCHEMA: StructType = StructType(
    [
        StructField('N_APPLIC_INFQ', IntegerType()),
        StructField('C_MAST_AGREM_REFER', StringType()),
        StructField('C_PART_REFER', StringType()),
        StructField('C_ROLE', StringType()),
        StructField('D_STR_ACTR_AGREM', DateType()),
        StructField('D_END_ACTR_AGREM', DateType()),
        StructField('EVENTDATE',DateType()),
    ]
)
MAAG_MASTER_AGREEMENT_SCHEMA: StructType = StructType(
    [
        StructField("C_ACT_TYPE", StringType()),
        StructField("D_CRDT_COMMITTEE_APRV", DateType()),  #if we use StringType, we have an error cuz it is registered as Date in the parquet file metadata.
        StructField("CENMES", IntegerType()),
        StructField("L_MAST_AGREM_NAME", StringType()),
        StructField("C_ACT_MNG_STG", StringType()),
        StructField("C_MAST_AGREM_REFER", StringType()),
        StructField("N_APPLIC_INFQ", IntegerType()),
        StructField("N_MAST_AGREM_VALI_PER", IntegerType()),
    ]
)

RTPA_REF_THIRD_PARTY_SCHEMA: StructType = StructType(
    [
        StructField("C_THIR_PART_REFER", StringType()),
        StructField("L_THIR_PART_NAME", StringType()),
        StructField("N_APPLIC_INFQ", IntegerType()),
    ]
)

MAAG_RATY_LINKED_SCHEMA: StructType = StructType(
    [
        StructField("C_M", StringType()),
        StructField("M_ORIG", LongType()), #if we use FloatType, we have an error cuz it is registered as INT64 in the parquet file metadata.
        StructField("M_ORIG_SHAR", LongType()),#if we use FloatType, we have an error cuz it is registered as INT64 in the parquet file metadata.
        StructField("C_MAST_AGREM_REFER", StringType()),
        StructField("N_APPLIC_INFQ", IntegerType()),
    
      
    ]
)

REAC_REF_ACT_TYPE_SCHEMA: StructType = StructType(
    [
        StructField("C_ACT_TYPE", StringType()),
        StructField("L_ACT_TYPE", StringType()),
        StructField("N_APPLIC_INFQ", IntegerType()),
    ]
)

# COMMAND ----------

if __name__ == "__main__":
    
    print("test jenkins9")
    spark = (SparkSession.builder \
        .appName("Exercice 1-Read-Write-From-S3") \
        .getOrCreate())
    
    df_paths = [ 
                's3a://exercice1-pyspark/tables/maag_master_agrem',
                's3a://exercice1-pyspark/tables/reac_ref_act_type', 
                's3a://exercice1-pyspark/tables/maag_repa_rrol_linked', 
                's3a://exercice1-pyspark/tables/maag_raty_linked', 
                's3a://exercice1-pyspark/tables/rtpa_ref_third_party'
               ]

    #Partie 1
    
    maag_master_agremDF = DF(spark, df_paths[0], MAAG_MASTER_AGREEMENT_SCHEMA)
    reac_ref_act_typeDF = DF(spark, df_paths[1], REAC_REF_ACT_TYPE_SCHEMA)
    maag_repa_rrol_linkedDF = DF(spark, df_paths[2], MAAG_REPA_RROL_LINKED_SCHEMA)
    maag_raty_linkedDF = DF(spark, df_paths[3],  MAAG_RATY_LINKED_SCHEMA)
    rtpa_ref_third_partyDF = DF(spark, df_paths[4], RTPA_REF_THIRD_PARTY_SCHEMA)

    rtpa_ref_third_partyDF.column_rename('C_THIR_PART_REFER', 'C_PART_REFER')
    
    dataset = DataSet(
                        [maag_master_agremDF, reac_ref_act_typeDF, maag_repa_rrol_linkedDF, maag_raty_linkedDF, rtpa_ref_third_partyDF], 
                        [
                            ['C_ACT_TYPE', 'N_APPLIC_INFQ'], 
                            ['N_APPLIC_INFQ', 'C_MAST_AGREM_REFER'], 
                            ['C_MAST_AGREM_REFER'], 
                            ['N_APPLIC_INFQ', 'C_PART_REFER']
                        ]
                     )
    
    dataset.show()
    

    dataset.write('s3a://exercise1-output-pyspark/tables/output', 'overwrite')
    
    #spark.stop()


# COMMAND ----------

    outputDF = DF(spark, 's3a://exercise1-output-pyspark/tables/output')
    outputDF.show()

# COMMAND ----------
    spark.stop()





spark.stop()


#withCredentials([[$class: 'AmazonWebServicesCredentialsBinding', accessKeyVariable:'AWS_ACCESS_KEY_ID', 
                        #credentialsId:'deploytos3-heikel', secretKeyVariable: 'AWS_SECRET_ACCESS_KEY']])