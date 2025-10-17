import boto3

def get_aws_client(service_name, credentials):

    if isinstance(credentials, dict):
        creds = credentials.get("credentials", credentials)
        access_key = creds.get("AccessKeyId")
        secret_key = creds.get("SecretAccessKey")
        token = creds.get("SessionToken")
    else:
         access_key = getattr(credentials, "access_key", None)
         secret_key = getattr(credentials, "secret_key", None)
         token = getattr(credentials, "token", None)

    return boto3.client(service_name,
                        region_name = "us-east-1",
                        aws_access_key_id = access_key,
                        aws_secret_access_key = secret_key,
                        aws_session_token = token)