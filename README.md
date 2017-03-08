# CSV2Redshift

This Python 3 script will take a local CSV file, upload it to S3, create a matching table in Redshift, then copy the contents from S3 to Redshift.

Before running, do this:

`pip install -r requirements.txt`

Then copy `settings.py.example` to `settings.py` and fill in your Redshift and S3 credentials.

Usage looks like this:

`python csv2redshift.py --file filename.csv --schema schema_name --table table_name`

To see all options:

`python csv2redshift.py --help`
