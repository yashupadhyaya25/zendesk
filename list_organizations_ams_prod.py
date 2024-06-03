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
s3_folder_path = 'organizations/' ### Folder path till /
s3_file_name = s3_folder_path+'organizations_data.json'
url = "https://"+comapny_zendesk_org_name+".zendesk.com/api/v2/organizations?page[size]=1"
next_url_page_file = s3_folder_path+"organizations_cursor.txt"
REQUEST_HEADER =  {
  'Authorization': 'Basic {0}'.format(zendesk_api_token)
}

def main():
    ### Check If File Exist in S3
    try :
        status = s3_client.get_object_attributes(Key = s3_file_name,Bucket = s3_bucket_name,ObjectAttributes=['ETag','Checksum','ObjectParts','StorageClass','ObjectSize'])
        file_exist = status.get('ResponseMetadata').get('HTTPStatusCode') 
    except :
        file_exist = 0
        
    if file_exist == 200 :
        return {
            'Status_Code' : 400,
            'Message' : 'File Exist'
        }
    res = org_fetch()
    return res

def org_fetch():
    try :
        api_data = []
        loop_flag = True
        next_page_url = ''
        ### Check for incremental file
        # try :
        #     status = s3_client.get_object_attributes(Key = next_url_page_file,Bucket = s3_bucket_name,ObjectAttributes=['ETag','Checksum','ObjectParts','StorageClass','ObjectSize'])
        #     file_exist = status.get('ResponseMetadata').get('HTTPStatusCode') 
        # except :
        #     file_exist = 0

        # if file_exist == 200 :
        #     next_page_url_obj = s3_client.get_object(Key = next_url_page_file,Bucket = s3_bucket_name)
        #     next_page_url = next_page_url_obj.get('Body').read().decode('utf-8')
        # else :
        #     next_page_url = ''


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
            json_data = response.json().get('organizations')
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
            ### Store Incremental Link
            # s3_client.put_object(Body = str(next_page_url),Key = next_url_page_file,Bucket = s3_bucket_name)
        
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
    print('API Organiztion Started On : '+str(dt.now()))
    res = main()
    print(res)
    print('API Organiztion Completed On : '+str(dt.now()))