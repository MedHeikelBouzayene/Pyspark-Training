import boto3
import json

ACCOUNT_ID = "202646624744"

sts_client = boto3.client('sts',aws_access_key_id = "AKIAS6LVODHULRGNXUR4", aws_secret_access_key = "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" ,region_name="us-east-1")

assumed_role_object =sts_client.assume_role(RoleArn="arn:aws:iam::202646624744:role/role_to_access_S3",RoleSessionName="S3access", DurationSeconds = 900)

print("assumed role: ", json.dumps(assumed_role_object,indent=4,default=str))

creds = assumed_role_object["Credentials"]

print("creds: ", json.dumps(creds,indent=4,default=str))

s3client = boto3.client("s3",aws_access_key_id=creds["AccessKeyId"],aws_secret_access_key=creds["SecretAccessKey"],aws_session_token=creds["SessionToken"],region_name="us-east-1")

result = s3client.list_buckets()

print("buckets: ", json.dumps(result,indent=4,default=str))