import csv
from redshift import rsm
import sys
import settings
from slugify import Slugify

# Set up custom_slugify
custom_slugify = Slugify(to_lower=True)
custom_slugify.separator = '_'

filename = sys.argv[1]
schema = 'stafftemp' if len(sys.argv) < 3 else custom_slugify(sys.argv[2])
table = custom_slugify(filename.split('.')[0] if len(sys.argv) < 4 else sys.argv[3])
# TODO: distinguish between filename and filepath
grant = True if len(sys.argv) < 5 or sys.argv[4] != 'no-grant' else False

# TODO: make sure schema exists
# e.g. SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'ngpvan';
# e.g. CREATE SCHEMA ngpvan
# schema_exists_query = """SELECT table_schema
# FROM information_schema.tables
# WHERE table_schema = '%s'
# LIMIT 1""" % schema
#
# schema_exists_result = rsm.db_query(schema_exists_query)
# print schema_exists_result

# TODO: grant usage if schema being created and grant = TRUE
# e.g. GRANT USAGE ON SCHEMA ngpvan TO staff

print 'Uploading file %s to S3 ...' % filename

# TODO: use boto3 to upload the file
# See: https://github.com/flyingsparx/FlaskDirectUploader/blob/master/application.py

print 'Creating Redshift table %s.%s ...' % (schema, table)

# Figure out the columns

rows = []
with open(filename, 'rb') as csvfile:
    csvreader = csv.reader(csvfile, delimiter=',', quotechar='"')
    for row in csvreader:
        rows.append(row)
columns = rows[0]

# TODO: Make sure column names are valid
# TODO: Build create table statement

column_statements = []
for column in columns:
    column_statements.append("%s VARCHAR(255)" % column)
column_sql = """,
    """.join(column_statements)
create_table_sql = """CREATE TABLE %s.%s (
    %s
)""" % (schema, table, column_sql)

print create_table_sql

# TODO: Run create table in redshift

print 'Importing file %s into Redshift table %s.%s ...' % (filename, schema, table)

# TODO: Run import table in redshift
# e.g. COPY hustle.actions
# (actions_created_date,groups_name,leads_external_id,leads_external_id_type,leads_first_name,leads_last_name,leads_phone_number,leads_email,goals_name,actions_value_str)
# FROM 's3://hustle-integrations-export-bjtioat6at/actions.gzip'
# CREDENTIALS 'aws_access_key_id=xxxxx;aws_secret_access_key=xxx'
# csv gzip dateformat 'YYYY-MM-DD' region 'us-east-1' ignoreheader 1;

copy_sql = """COPY %s.%s
(%s)
FROM 's3://%s/%s'
CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s'
csv ignoreheader 1 acceptinvchars""" % (schema, table, ", ".join(columns), settings.s3_bucket, filename, settings.aws_access_key, settings.aws_secret_key)

if settings.s3_bucket_region != None:
    copy_sql = copy_sql + " region '%s'" % settings.s3_bucket_region

print copy_sql

if grant:
    print 'Granting staff SELECT access to Redshift table %s.%s ...' % (schema, table)

    grant_sql = """GRANT SELECT ON %s.%s TO staff""" % (schema, table)

    print grant_sql

    # TODO: Run GRANT in redshift
