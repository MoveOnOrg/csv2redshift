import boto3
import csv
import psycopg2
from slugify import Slugify

class CSV2Reshift:

    def __init__(self, settings):
        self.settings = settings
        self.redshift = psycopg2.connect(
            host=self.settings.DB_HOST,
            port=self.settings.DB_PORT,
            user=self.settings.DB_USER,
            password=self.settings.DB_PASS,
            database=self.settings.DB_NAME
        )
        self.redshift_cursor = self.redshift.cursor()
        self.custom_slugify = Slugify(to_lower=True)
        self.custom_slugify.separator = '_'

    def schema_exists(self, schema_name):

        schema_exists_query = """SELECT table_schema
        FROM information_schema.tables
        WHERE table_schema = '%s'
        LIMIT 1""" % schema_name

        self.redshift_cursor.execute(schema_exists_query)
        schema_exists_results = list(self.redshift_cursor.fetchall())

        if len(schema_exists_results) == 0:
            return False
        else:
            return True

    def create_schema(self, schema_name, print_sql=False):
        schema_create_query = "CREATE SCHEMA %s" % schema_name
        if print_sql:
            print(schema_create_query)
        self.redshift_cursor.execute(schema_create_query)
        self.redshift.commit()

    def grant_usage(self, schema_name, user_name, print_sql=False):
        schema_grant_query = "GRANT USAGE ON SCHEMA %s to %s" % (schema_name, user_name)
        if print_sql:
            print(schema_grant_query)
        self.redshift_cursor.execute(schema_grant_query)
        self.redshift.commit()

    def upload_file_to_s3(self, filename):
        aws_session = boto3.session.Session(
          aws_access_key_id=self.settings.AWS['UPLOAD']['ACCESS_KEY'],
          aws_secret_access_key=self.settings.AWS['UPLOAD']['SECRET_KEY'],
          region_name=self.settings.AWS['UPLOAD']['REGION']
        )
        s3 = aws_session.client('s3')
        s3.upload_file(filename, self.settings.AWS['UPLOAD']['S3_BUCKET'], filename)

    def get_column_headers_from_csv(self, filename, tsv=False):
        with open(filename, 'rt') as csvfile:
            if tsv:
                csvreader = csv.reader(csvfile, dialect='excel-tab')
            else:
                csvreader = csv.reader(csvfile, delimiter=',', quotechar='"')
            columns = [self.custom_slugify(column) for column in next(csvreader)]
            columns = [
                'x' + column if column[0].isdigit()
                else column
                for column in columns
            ]
        return columns

    def table_exists(self, schema_name, table_name):
        table_exists_query = """SELECT table_schema
        FROM information_schema.tables
        WHERE table_schema = '%s'
        AND table_name = '%s'
        LIMIT 1""" % (schema_name, table_name)

        self.redshift_cursor.execute(table_exists_query)
        table_exists_results = list(self.redshift_cursor.fetchall())

        if len(table_exists_results) == 0:
            return False
        else:
            return True

    def get_create_table_sql(self, schema_name, table_name, columns):
        column_statements = []
        for column in columns:
            column_statements.append("%s VARCHAR(255)" % column)
        column_sql = """,
            """.join(column_statements)
        return """CREATE TABLE %s.%s (
            %s
        )""" % (schema_name, table_name, column_sql)

    def create_table(self, schema_name, table_name, columns, print_sql=False):
        create_table_sql = self.get_create_table_sql(schema_name, table_name, columns)
        if print_sql:
            print(create_table_sql)
        self.redshift_cursor.execute(create_table_sql)
        self.redshift.commit()

    def get_copy_from_s3_sql(self, schema_name, table_name, columns, filename):
        copy_sql = """COPY %s.%s
        (%s)
        FROM 's3://%s/%s'
        CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s'
        csv ignoreheader 1 acceptinvchars""" % (schema_name, table_name, ", ".join(columns), self.settings.AWS['COPY']['S3_BUCKET'], filename, self.settings.AWS['COPY']['ACCESS_KEY'], self.settings.AWS['COPY']['SECRET_KEY'])

        if self.settings.AWS['COPY']['REGION'] != None:
            copy_sql = copy_sql + " region '%s'" % self.settings.AWS['COPY']['REGION']

        return copy_sql

    def copy_from_s3(self, schema_name, table_name, columns, filename, print_sql=False):
        copy_sql = self.get_copy_from_s3_sql(schema_name, table_name, columns, filename)
        if print_sql:
            print(copy_sql)
        self.redshift_cursor.execute(copy_sql)
        self.redshift.commit()

    def grant_select_on_table(self, schema_name, table_name, user_name, print_sql=False):
        grant_sql = """GRANT SELECT ON %s.%s TO %s""" % (schema_name, table_name, user_name)
        if print_sql:
            print(grant_sql)
        self.redshift_cursor.execute(grant_sql)
        self.redshift.commit()

if __name__ == '__main__':

    import argparse
    import settings

    parser = argparse.ArgumentParser(description='Import CSV files into Redshift.')
    parser.add_argument('--file', dest='filename', help='file name')
    parser.add_argument('--schema', dest='schema', help='database schema')
    parser.add_argument('--table', dest='table', help='database table')
    parser.add_argument('--nogrant', dest='nogrant', help='skip table grant', action='store_const', const=True, default=False)
    parser.add_argument('--nocreate', dest='nocreate', help='skip table creation', action='store_const', const=True, default=False)
    parser.add_argument('--nos3', dest='nos3', help='skip S3 upload', action='store_const', const=True, default=False)
    parser.add_argument('--nocopy', dest='nocopy', help='skip data COPY', action='store_const', const=True, default=False)
    parser.add_argument('--tsv', dest='tsv', help='parse as TSV instead of CSV', action='store_const', const=True, default=False)
    parser.add_argument('--printsql', dest='printsql', help='print all executed SQL', action='store_const', const=True, default=False)
    args = parser.parse_args()

    csv2red = CSV2Reshift(settings)
    columns = csv2red.get_column_headers_from_csv(args.filename, tsv=args.tsv)

    if not args.nocreate:
        if not csv2red.schema_exists(args.schema):
            print("Creating schema %s and granting usage to %s...." % (args.schema, settings.DB_GRANT_USER))
            csv2red.create_schema(args.schema, args.printsql)
            if not args.nogrant:
                csv2red.grant_usage(args.schema, settings.DB_GRANT_USER, args.printsql)
        if not csv2red.table_exists(args.schema, args.table):
            print('Creating Redshift table %s.%s ...' % (args.schema, args.table))
            csv2red.create_table(args.schema, args.table, columns, args.printsql)
        else:
            print('Redshift table %s.%s already exists.' % (args.schema, args.table))

    if not args.nos3:
        print('Uploading file %s to S3 ...' % args.filename)
        csv2red.upload_file_to_s3(args.filename)

    if not args.nocopy:
        print('Importing file %s into Redshift table %s.%s ...' % (args.filename, args.schema, args.table))
        csv2red.copy_from_s3(args.schema, args.table, columns, args.filename, args.printsql)

    if not args.nogrant:
        print('Granting %s SELECT access to Redshift table %s.%s ...' % (settings.DB_GRANT_USER, args.schema, args.table))
        csv2red.grant_select_on_table(args.schema, args.table, settings.DB_GRANT_USER, args.printsql)
