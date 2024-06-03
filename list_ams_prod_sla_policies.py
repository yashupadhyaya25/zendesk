import requests
import boto3
from datetime import datetime as dt
import json
from configparser import ConfigParser

ENV = 'production'

config = ConfigParser()
config.read('config.ini')

aws_access_id = config.get(ENV,'s3_access_id')
aws_secret_access_key = config.get(ENV,'s3_secret_access_key')
zendesk_api_token = config.get(ENV,'zendesk_api_token')
aws_region_name = config.get(ENV,'aws_region_name')
comapny_zendesk_org_name = config.get(ENV,'comapny_zendesk_org_name')

s3_client = boto3.client('s3',
    region_name=aws_region_name,
    aws_access_key_id=aws_access_id,
    aws_secret_access_key=aws_secret_access_key
)

s3_bucket_name = "zendeskdemo"
s3_folder_path = 'slas_policies/' ### With path till /
s3_file_name = s3_folder_path+'policies_data.json'
url = "https://"+comapny_zendesk_org_name+".zendesk.com/api/v2/slas/policies"
REQUEST_HEADER =  {
  'Authorization': 'Basic {0}'.format(zendesk_api_token)
}

def main():
    ### Check If File Exist in S3
    try :
        status = s3_client.get_object(Key = s3_file_name,Bucket = s3_bucket_name
                                    #   ,ObjectAttributes=['ETag','Checksum','ObjectParts','StorageClass','ObjectSize']
                                    )
        file_exist = status.get('ResponseMetadata').get('HTTPStatusCode') 
    except :
        file_exist = 0
        
    if file_exist == 200 :
        return {
            'Status_Code' : 400,
            'Message' : 'File Exist'
        }
    res = slas_policies_fetch()
    return res

def slas_policies_fetch():
    try :
        api_data = []
        next_page_url = ''
        while True :
            if next_page_url == '' :
                ### Make a GET request to the API endpoint with the headers
                response = requests.get(url, headers=REQUEST_HEADER)
                ### Convert the response to JSON format
                json_data = response.json()
                fetch_data = json_data.get('sla_policies')
                for data in fetch_data :
                    api_data.append(data)
                next_page_url = json_data.get('next_page') 
                print('API Status Code : '+ str(response.status_code))
                if next_page_url is None :
                    break
                if response.status_code != 200 :
                    return {
                        'Status_Code' : 403,
                        'Message' : 'API Error : '+response.text
                    }
            ### Make a GET request to the API endpoint with the headers
            response = requests.get(next_page_url, headers=REQUEST_HEADER)
            json_data = response.json()
            fetch_data = json_data.get('sla_policies')
            for data in fetch_data :
                    api_data.append(data)
            next_page_url = json_data.get('next_page')
            if next_page_url is None :
                break
            print('API Status Code : '+ str(response.status_code))
            if response.status_code != 200 :
                return {
                    'Status_Code' : 403,
                    'Message' : 'API Error : '+response.text
                }
            
        ## Put File In S3
        s3_client.put_object(Body = json.dumps(api_data),Key = s3_file_name,Bucket = s3_bucket_name)
        return {
                'Status_Code' : 200,
                'Message' : 'Data Loaded Into S3 Successfully'
            }
            
    except Exception as e:
        return {
                'Status_Code' : 401,
                'Message' : 'Encountered An Error : '+str(e)
        }
    

if __name__ == '__main__' :
    print('API SLAs Policy Started On : '+str(dt.now()))
    res = main()
    print(res)
    print('API SLAs Policy Completed On : '+str(dt.now()))