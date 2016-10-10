import boto3
import csv
import psycopg2
import settings
from slugify import Slugify
import sys

redshift = psycopg2.connect(
    host=settings.DB_HOST,
    port=settings.DB_PORT,
    user=settings.DB_USER,
    password=settings.DB_PASS,
    database=settings.DB_NAME
)
redshift_cursor = redshift.cursor()

# Set up custom_slugify
custom_slugify = Slugify(to_lower=True)
custom_slugify.separator = '_'

filename = sys.argv[1]
schema = 'stafftemp' if len(sys.argv) < 3 else custom_slugify(sys.argv[2])
table = custom_slugify(filename.split('.')[0] if len(sys.argv) < 4 else sys.argv[3])
# TODO: distinguish between filename and filepath
grant = True if len(sys.argv) < 5 or sys.argv[4] != 'no-grant' else False

schema_exists_query = """SELECT table_schema
FROM information_schema.tables
WHERE table_schema = '%s'
LIMIT 1""" % schema

redshift_cursor.execute(schema_exists_query)
schema_exists_results = list(redshift_cursor.fetchall())

if len(schema_exists_results) == 0:
    print("Creating schema %s and granting usage to staff...." % schema)
    schema_create_query = "CREATE SCHEMA %s" % schema
    redshift_cursor.execute(schema_create_query)
    redshift.commit()
    schema_grant_query = "GRANT USAGE ON SCHEMA %s to staff" % schema
    redshift_cursor.execute(schema_grant_query)
    redshift.commit()

print('Uploading file %s to S3 ...' % filename)

# TODO: get S3 upload working.

aws_session = boto3.session.Session(
  aws_access_key_id=settings.AWS['UPLOAD']['ACCESS_KEY'],
  aws_secret_access_key=settings.AWS['UPLOAD']['SECRET_KEY'],
  region_name=settings.AWS['UPLOAD']['REGION']
)
s3 = aws_session.client('s3')
s3.upload_file(filename, settings.AWS['UPLOAD']['S3_BUCKET'], filename)

print('Creating Redshift table %s.%s ...' % (schema, table))

# Figure out the columns

rows = []
with open(filename, 'rt') as csvfile:
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

print(create_table_sql)

redshift_cursor.execute(create_table_sql)
redshift.commit()

print('Importing file %s into Redshift table %s.%s ...' % (filename, schema, table))

copy_sql = """COPY %s.%s
(%s)
FROM 's3://%s/%s'
CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s'
csv ignoreheader 1 acceptinvchars""" % (schema, table, ", ".join(columns), settings.AWS['COPY']['S3_BUCKET'], filename, settings.AWS['COPY']['ACCESS_KEY'], settings.AWS['COPY']['SECRET_KEY'])

if settings.AWS['COPY']['REGION'] != None:
    copy_sql = copy_sql + " region '%s'" % settings.AWS['COPY']['REGION']

print(copy_sql)

redshift_cursor.execute(copy_sql)
redshift.commit()

if grant:
    print('Granting staff SELECT access to Redshift table %s.%s ...' % (schema, table))

    grant_sql = """GRANT SELECT ON %s.%s TO staff""" % (schema, table)

    print(grant_sql)

    redshift_cursor.execute(grant_sql)
    redshift.commit()
