import array
import os
import boto3
import pandas as pd

bucket = 's3-server-access-logs'
dict_of_buckets_to_files = {}

def check_for_anonymous_access():
    log_data = []
    for child_bucket in dict_of_buckets_to_files:
        for log_key in dict_of_buckets_to_files[child_bucket]:
            log_data.append(pd.read_csv('s3://' + bucket + '/' + child_bucket + "/" + log_key, sep=" ",
                                        names=['Bucket_Owner', 'Bucket', 'Time', 'Time_Offset', 'Remote_IP',
                                               'Requester_ARN/Canonical_ID',
                                               'Request_ID',
                                               'Operation', 'Key', 'Request_URI', 'HTTP_status', 'Error_Code', 'Bytes_Sent',
                                               'Object_Size',
                                               'Total_Time',
                                               'Turn_Around_Time', 'Referrer', 'User_Agent', 'Version_Id', 'Host_Id',
                                               'Signature_Version',
                                               'Cipher_Suite',
                                               'Authentication_Type', 'Host_Header', 'TLS_version'],
                                        usecols=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
                                                 21, 22, 23, 24]))

        df = pd.concat(log_data)
        print(df.to_csv(child_bucket + ".csv"))
        # df[(df['Operation'] == 'REST.GET.OBJECT')]['Key'].value_counts()
        # print(df[(df['Requester_ARN/Canonical_ID'] == '-')]['Key'].info())
        # print(df[(df['Requester_ARN/Canonical_ID'] == '-')]['Time'].info())




def create_dict(child_bucket, log_file):
    if child_bucket not in dict_of_buckets_to_files:
        list_of_files = [log_file]
        dict_of_buckets_to_files[child_bucket] = list_of_files
    else:
        list_from_dict = dict_of_buckets_to_files[child_bucket]
        list_from_dict.append(log_file)



def analyze():
    s3_client = boto3.client('s3')

    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket)
    for page in page_iterator:
        for obj in page['Contents']:
            key = obj['Key']
            child_bucket = key.split("/")[0]
            log_file = key.split("/")[1]
            create_dict(child_bucket, log_file)

    check_for_anonymous_access()


if __name__ == '__main__':
    analyze()