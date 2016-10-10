# CSV2Redshift

This script will take a local CSV file, upload it to S3, create a matching table in Redshift, then copy the contents from S3 to Redshift.

Before running, do this:

`pip3 install -r requirements.txt`

Usage looks like this:

`python3.4 filename.csv schema_name table_name`
