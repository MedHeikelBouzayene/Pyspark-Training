from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType, LongType
import boto3

if __name__ == '__main__':
    
    sts_client = boto3.client('sts',aws_access_key_id = "AKIAS6LVODHULRGNXUR4", aws_secret_access_key = "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" ,region_name="us-east-1")

    assumed_role_object =sts_client.assume_role(RoleArn="arn:aws:iam::202646624744:role/role_to_access_S3",RoleSessionName="S3access", DurationSeconds = 900)
    
    creds = assumed_role_object["Credentials"]
    
    AWS_ACCESS_KEY_ID = "AKIAS6LVODHULRGNXUR4"#creds['AccessKeyId']
    AWS_SECRET_ACCESS_KEY = "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"#creds['SecretAccessKey']
    SessionToken = creds["SessionToken"]
    
    print(AWS_ACCESS_KEY_ID)
    print(AWS_SECRET_ACCESS_KEY)
    print(SessionToken)
    
    spark = SparkSession.builder.appName("Read S3 Files").getOrCreate()
    spark.conf.set("spark.hadoop.fs.s3a.access.key", "AKIAS6LVODHULRGNXUR4")
    spark.conf.set("spark.hadoop.fs.s3a.secret.key","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")
    
    df_paths = [ 
                f's3a://exercice1-pyspark/tables/maag_master_agrem',
                's3://exercice1-pyspark/tables/reac_ref_act_type', 
                's3://exercice1-pyspark/tables/maag_repa_rrol_linked', 
                's3://exercice1-pyspark/tables/maag_raty_linked', 
                's3://exercice1-pyspark/tables/rtpa_ref_third_party'
               ]

    df = spark.read.options(header = 'True', inferSchema = 'True').parquet(df_paths[0])
    df.show()
    spark.stop()