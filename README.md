# osm-borders

Required C libraries: GDAL, GEOS

On Ubuntu:
apt install g++ gcc libgdal-dev python3.6-dev


Requires: python 3.6

pip install -r requirements.txt

This script fetches administrative boundary definitions from EMUiA repository and converts it to OSM XML


`amazon/` folder contains Terraform scripts to create AWS Lambda / DynamoDB deployment