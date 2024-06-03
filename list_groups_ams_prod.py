import json
import requests
import boto3
from datetime import datetime as dt
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
s3_folder_path = 'group/' ### Folder path till /
s3_file_name = s3_folder_path+'group_data.json'
url = "https://"+comapny_zendesk_org_name+".zendesk.com/api/v2/groups?page[size]=100"
REQUEST_HEADER =  {
  'Authorization': 'Basic {0}'.format(zendesk_api_token)
}

def main():
    ### Check If File Exist in S3
    try :
        status = s3_client.get_object(Key = s3_file_name,
                                      Bucket = s3_bucket_name,
                                    #   ObjectAttributes=['ETag','Checksum','ObjectParts','StorageClass','ObjectSize']
                                      )
        file_exist = status.get('ResponseMetadata').get('HTTPStatusCode') 
    except :
        file_exist = 0
        
    if file_exist == 200 :
        return {
            'Status_Code' : 400,
            'Message' : 'File Exist'
        }
    res = group_fetch()
    return res

def group_fetch():
    try :
        api_data = []
        loop_flag = True
        next_page_url = ''
        while loop_flag :
            ### Make a GET request to the API endpoint with the headers
            if next_page_url != '' :
                response = requests.get(next_page_url, headers=REQUEST_HEADER)
            else :
                response = requests.get(url, headers=REQUEST_HEADER)
            print('API Status Code : '+ str(response.status_code))
            
            if response.status_code != 200 :
                return {
                    'Status_Code' : 403,
                    'Message' : 'API Error : '+response.text
                }
               
            ### Convert the response to JSON format
            json_data = response.json().get('groups')
            has_more = response.json().get('meta').get('has_more')  
            next_page_url = response.json().get('links').get('next')

            if json_data != [] :
                for data in json_data :
                    api_data.append(data)

            if not(has_more) :
                loop_flag = False
            
        ### Put File In S3
        if json_data != []  :
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
    print('API Group Started On : '+str(dt.now()))
    res = main()
    print(res)
    print('API Group Completed On : '+str(dt.now()))