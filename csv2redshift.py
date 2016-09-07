import csv
import sys

filename = sys.argv[1]
database = 'stafftemp' if len(sys.argv) < 3 else sys.argv[2]
table = filename.split('.')[0] if len(sys.argv) < 4 else sys.argv[3]
# TODO: distinguish between filename and filepath
grant = True if len(sys.argv) < 5 or sys.argv[4] != 'no-grant' else False

# TODO: make sure table name is valid
# TODO: make sure database name is valid
# TODO: make sure database exists

print 'Uploading file %s to S3 ...' % filename

# TODO: use boto3 to upload the file
# See: https://github.com/flyingsparx/FlaskDirectUploader/blob/master/application.py

print 'Creating Redshift table %s.%s ...' % (database, table)

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
create_table = """CREATE TABLE %s.%s (
    %s
)""" % (database, table, column_sql)

print create_table

# TODO: Run create table in redshift

print 'Importing file %s into Redshift table %s.%s ...' % (filename, database, table)

# TODO: Run import table in redshift

if grant:
    print 'Granting staff SELECT access to Redshift table %s.%s ...' % (database, table)

    # TODO: Run GRANT in redshift
