import boto3

access_key = ""
secret_key = ""


s3 = boto3.client(
    's3',
    endpoint_url='https://storage.yandexcloud.net',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key
)

def upload_file(bucket_name, file_name, object_name):
    try:
        s3.upload_file(file_name, bucket_name, object_name)
        print(f"File {file_name} uploaded to bucket {bucket_name} as {object_name}")
    except Exception as e:
        print(f"Error uploading file: {e}")

if __name__ == "__main__":
    bucket_name = "mlops-backet-1"
    file_name = "test.txt"
    object_name = "uploaded_test.txt"
    
    upload_file(bucket_name, file_name, object_name)
