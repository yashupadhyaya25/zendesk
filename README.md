# ZENDESK

# How to setup on local
**Step 1 :** First of all download the zip or clone the repo.

**Step 2 :** Unzip the file that you have downloaded from above step

**Step 3 :** Add the local path under the ‘ **config.ini** ’ file like this if file not present make ‘ **config.ini** ’ file:

        [give name as you like]
                zendesk_api_token = <zendesk basic auth token>        
                s3_access_id = <s3 access id>
                s3_secret_access_key = <s3 secret access key>
                aws_region_name = <s3 region name>
                comapny_zendesk_org_name =  <In my case its adaasa912 https://adaasa912.zendesk.com>

**Step 4 :** Relpace ENV variable value that you have given in above step in all python file.