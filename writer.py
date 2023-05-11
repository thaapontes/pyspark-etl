import boto3

# Tables in spark are transient, so we need to write tables before finishing the session with spark.stop()
# Reference: https://www.stackvidhya.com/write-a-file-to-s3-using-boto3/
# Creating Session With Boto3.
session = boto3.Session(
    aws_access_key_id='AKIAQNW56D3ZIESDXSEK',
    aws_secret_access_key='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'
)

# Creating S3 Resource From the Session.
s3 = session.resource('s3')

result = s3.Bucket('pysparkdlstorage').upload_file(
    'written_files/cars_grouped/part-00000-d955ce34-729b-4d78-bd77-70e377f9bbb5-c000.csv', 'cars_grouped.csv')
