# osm-borders

Required C libraries: GDAL, GEOS

On Ubuntu:
apt install g++ gcc libgdal-dev python3.6-dev


Requires: python 3.6

pip install -r requirements.txt

This script fetches administrative boundary definitions from EMUiA repository and converts it to OSM XML.

It is possible to fetch:
1. Municipality (admin_level=7) borders by providing 4-digit PRG code
2. Settlement (admin_level=8,9 and 10) borders by providing 7-digit PRG code



# Amazon architecture
Because AWS API Gateway calls are limited to 60 seconds and the process may take more than 60 seconds,
SNS queues are used to allow longer Lambda calls. Clients 

```
 package-rest.zip
+-------------------+     2. Submit fetch task
| /<type>/<prg>.osm +-------------+
+-+-----------------+             |
  |                               |
  | 1. Check cache         +------+-----+
  |                        | Amazon SNS |
  | 5. Fetch from cache    +------+-----+
  |                               |
  |                               |
  |                       +-------+-------------+  3. Fetch  +-----------+
  |                       | package-fetcher.zip +------------+ Geoportal |
  |                       +--+------------------+     data   +-----------+
+-+--------+    4. Put into  |
| DynamoDB +-----------------+
+----------+       cache
```

1. Amazon API Gateway is exposing Amazon Lambda `amazon.rest_endpoint` function at `/<prg>.osm`. 
2. If there is sentinel object in DynamoDB and it's not stale goto 4
3. If stale or no data is found in DynamoDB, puts a sentinel object into DynamoDB and a message in Amazon SNS queue `queue_name`
4. Sleep for 30 seconds
5. Check if result is ready. If response is ready, returns the response, if not returns redirect to itself (goto 1).

Message schema:
```json
{
    "terc": "123331",
    "type": "all",
    "request_time": 1515263002
}
```

`amazon.fetcher` listens on Amazon SNS queue 

1. Checks if there is sentinel object in DynamoDB and matches request_time, if not exit
2. Checks if there is fresh data in DynamoDB. If yes, then exit
3. Invokes `get_<type>_borders`. Results are put into DynamoDB table `table_name`

# Building Amazon environment

## Requirements
Install docker:
`apt install docker.io`

[Install Terraform](https://www.terraform.io/intro/getting-started/install.html)

Assuming you will have `terraform` in your path, `amazon/` folder contains Terraform scripts to create AWS Lambda / DynamoDB deployment. To deploy on your Amazon environment:
```bash
export AWS_ACCESS_KEY_ID="YOUR_AWS_KEY_ID"
export AWS_SECRET_ACCESS_KEY="YOUR_AWS_ACCESS_KEY"
export AWS_DEFAULT_REGION="DEPLOYMENT_REGION"

$ cd amazon
$ terraform init
```

And then, to deploy:
```bash
$ ( cd .. && build_package.sh )
$ terraform apply
[...]
Outputs:

url = https://xxx.execute-api.eu-west-1.amazonaws.com/api
```
Where the last line presents URL of your deployment 
