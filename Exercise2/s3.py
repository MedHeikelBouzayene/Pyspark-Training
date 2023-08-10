from pyspark import SparkConf
from pyspark.sql import SparkSession

# Créer une session Spark
AWS_ACCESS_KEY_ID = "ASIAXV3TV65U72WWRGQK"
AWS_SECRET_ACCESS_KEY = "+qBzw4y17ySEq1FfnThB+M+LD1yleovm7yWbfzq8"
SessionToken = "FwoGZXIvYXdzENz//////////wEaDBYV4OqfOl+olivBzyL8AZ6x5acCJjVdoh+DAtTUR7yf8mGUJRed9IXX2/+rtcg5ESdTF3kt1Tf1YrNmwtsG0OxHoMiDcKFuoVG1h9iG+SXXjAMqmQGgT7iehM8VoWGv2+i5gX1QuKsXsTAXVpgh2ZyyjQGwjTjb6QUL+H9zw0Ar+A0L3kOv/a48n2sf3QBT1IOCYiiNTx13XujYgcNEwPFu02JC7q2IIzO8HlNIDffVdja9ub5X8DNECaPRhnfntSH5ql3oQOTbZMGOhURkKnD3izqNe92wlccGRcAvHeINQNqo/bV1mR+yCwNuoF+kJ2Z+rLFzE1ORaa3A7Otb0LpDYZN1ELqL2IN+eCimmsOmBjIrojOzcUNeSwSNisejCEbCZVnUhqwRfsMyqxEpDtzt4LN9UYeyI7iv8sZvcw=="


full_path = "s3a://aws-sto-s3s-dcp-r-trusted/CORPORATE/COMPTOIR_FINANCERISK/MOU/APIS/InstrumentCredit/transaction_risk_reason/eventdate=2023-05-31/SIG=ALL/part-00000-c7f050e0-a468-45fd-a884-762f42f6ab7a.c000.csv"

spark = (

    SparkSession.builder.appName("Lire une table depuis AWS S3")

    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")

    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)

    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)

    .config("spark.hadoop.fs.s3a.session.token", SessionToken)

    .config("spark.hadoop.fs.s3a.endpoint", "https://s3-eu-west-3.amazonaws.com")

    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    .getOrCreate()

)


#spark = SparkSession.builder.appName("s3 test").getOrCreate()

df = spark.read.csv(full_path)
spark.stop()