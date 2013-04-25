#!/usr/local/bin/python
"""
Generic import from unclean / unlabelled sql table
______________________________
CreatedBy:  Steve Harris

CreatedAt:  120815
ModifiedAt: 120815

Filename:   import_sql.py
Project:

## Description

Mimic the import_excel functionality
then can follow on with index_table, make_table etc

## Dependencies

____
"""
import mypy

import sys      # file input / output
import re       # regular expressions
import os       # file system operations
import yaml     # YAML parser for the dictionary_tables file
import string   # string manipulation

                # date and time modules
import datetime
import time

import MySQLdb  # MySQL database wrapper
import re       # regular expressions
import argparse
import warnings
warnings.filterwarnings('error', '.*Out of range.*')
warnings.filterwarnings('error', '.*invalid.*')

# TODO: 2012-08-17 - switch back to global library files
# sys.path.remove('/Users/steve/usr/local/lib')
# sys.path.append('/Users/steve/data/spot_early/local/lib_usr')
import myspot   # user written spot functions

# print os.getcwd()
# myspot.reverse_field_dict()
# sys.exit(1)

# Parse command line options
# ____________________________
#
parser = argparse.ArgumentParser(description =
"""
Generic convert existing SQL table
to cleanly labelled and specified version
""")

# Mandatory args (positional)
parser.add_argument("target_db", help=":target MySQL database name")
parser.add_argument("raw_table", help=":name of existing table")

# if not specified (None) then set assume same db as target
parser.add_argument("-source", "--source_db",
    help=":source MySQL database name",
    default = None)

# force explict request to replace table
parser.add_argument("-replace", "--replace_table",
    help=":explicit request to replace existing table",
    action="store_true",
    default=False)

args = parser.parse_args()
parser.parse_args()

print "\nOK: Running import_sql.py\n"

# Function definitions
# ____________________

table_in = args.raw_table
target_db = args.target_db

if args.source_db is None:
    if table_in[-4:] != "_raw":
        print table_in[-4:]
        print "ERROR: source table must have suffix _raw if from same database"
        sys.exit(1)
    table_dict_name = table_in[:-4]
    print table_dict_name
    # sys.exit(1)
    # table_out = table_in[:-3]
    source_db = target_db
else:
    # see above - tables will have diff names if in same db
    source_db = args.source_db
    table_dict_name = table_in.lower()
    # table_out = table_in


print "OK: Input table: %s from %s\nOK: Target database: %s" % (
    table_in, source_db, target_db)
# print table_in, table_out
# sys.exit(1)
# check and connect to database provided using 'target_db'
cursor_out = myspot.sql_connect(target_db)
cursor_in = myspot.sql_connect(source_db)

table_out = table_dict_name + '_import'
if myspot.sql_tab_exists(target_db, table_out):
    print "WARNING: table %s already exists in database %s" % (target_db, table_out)
    if args.replace_table:
        stmt = "DROP TABLE IF EXISTS %s " % (table_out)
    else:
        print "ERROR: You must specify the '-replace' option to delete existing tables"
        sys.exit(1)

# check you can open the dictionares OK
tdict = myspot.get_yaml_dict('table', local_dict=True)
fdict = myspot.get_yaml_dict('field', local_dict=True)
# falias_dict = myspot.reverse_field_dict(local_dict = True)
# now focus the falias_dict on the likely tables
falias_dict = myspot.make_talias_dict(fdict)
talias_dict = falias_dict['no_alias']
# print len(talias_dict)
if table_dict_name in falias_dict:
    # print table_dict_name
    # print falias_dict[table_dict_name]
    talias_dict.update(falias_dict[table_dict_name])
talias_dict = {i[0].lower():i[1] for i in talias_dict.items()}
debug = False
if debug:
    print len(talias_dict)
    print talias_dict
    sys.exit(1)

# first check there are no identical field names
ttemp =[f['fname'] for f in fdict]
if len(ttemp) > len(set(ttemp)):
    print "ERROR: Duplicate field names found in dictionary"
    sys.exit(1)
else:
    fdict_lookup = {}
    for f in fdict:
        fdict_lookup[f['fname'].lower()] = f

# Get fields names from existing table
field_list = myspot.sql_get_cols(source_db, table_in)
field_list = [f.lower() for f in field_list]
print field_list

source_fields, target_fields = [], []
# for field in field_list:
#     if field not in talias_dict:
#         print "WARNING: %s not in field dictionary - will not be imported" % field
#         continue
#     fname = talias_dict[field]
#     fspec = fdict_lookup[fname]

#     source_fields.append(field)
#     target_fields.append(fname)

#     print field, fname

source_fields = field_list
target_fields = myspot.clean_field_aliases(field_list, table_dict_name)

target_fields_found = [i for i in target_fields if i != 'ALIAS_NOT_IN_FIELD_DICT']
# CHANGED: 2012-12-21 - tab_serial appended if does not already exist
# ... also see end of loop below
tab_serial_missing = False
if 'tab_serial' not in target_fields_found:
    tab_serial_missing = True
    target_fields_found.append('tab_serial')
debug = False
if debug:
    sys.exit(1)

# myspot.cr_mysql_tab(target_db, table_dict_name, target_fields_found,
#     raw_flg = 0,
#     import_flg = 1,
#     replace_flg = 1)

myspot.quick_mysql_tab(cursor_out, fdict, table_out, target_fields_found)

stmt_in = "SELECT %s FROM %s" % (', '.join(source_fields), table_in)
cursor_in.execute(stmt_in)
rows = cursor_in.fetchall()


for index, values in enumerate(rows):
    sfv = zip(source_fields, target_fields, list(values))
    # print sfv
    clean_values = []
    for source, fname, value in sfv:
        # print fname
        if fname == 'ALIAS_NOT_IN_FIELD_DICT':
            continue
        fspec = fdict_lookup[fname]
        if 'sqltype' in fspec:
            sqltype = fspec['sqltype']
        else:
            print "ERROR: sqltype key missing for %s" % fspec['fname']
            sys.exit(1)
        if 'sqllen' in fspec:
            sqllen = fspec['sqllen']
        else:
            sqllen = 255

        if isinstance(value, str):
            value = value.lower()

            if len(value) == 0 or value.isspace() is True:
                value = None

            # where the value is clearly indicated as not available
            else:
                na_regex = re.compile(r"\b(nr|na|n/a|missing|unknown|unanswered|z+)\b")
                if na_regex.match(value) != None:
                    value = None

        # where a regex is specified for replacing values
        if 'substitute' in fspec and value != None:
            regex = re.compile(fspec['substitute']['find'])
            value = regex.sub(fspec['substitute']['replace'], str(value))

        # where value aliases are provided
        # CHANGED: 2012-09-18 - to handle multiple valias types
        if 'valias' in fspec or 'valiases' in fspec:
            valias = None
            if 'valiases' in fspec:
                for v in fspec['valiases']:
                    # print fname, v, table_out
                    if table_out in v['valias_tables']:
                        valias = v['valias']
            else:
                valias = fspec['valias']
            if valias:
                valias_dict = dict((str(i[0]).lower(), i[1]) if isinstance(i[0], str)
                    else (i[0], i[1]) for i in valias.items())
                # print valias_dict
                # print value
                if value in valias_dict:
                    value = valias_dict[value]
                if value == 'NULL':
                    value = None
                # print value
                # raw_input()

        success, result = myspot.sql_2mysql(value, sqltype, sqllen, format='iso')
        if not success:
            print "WARNING: for %s failed to parse %s as %s" % (
                fname, value, sqltype)
        # print result
        clean_values.append(result)

    # CHANGED: 2012-12-21 - row index added if tab_serial does not already exist
    #  ... helps b/c index_table expects to find tab_serial
    if tab_serial_missing:
        clean_values.append(index + 1)
    stmt_out = myspot.sql_prepare_insert(
        table_out, target_fields_found, clean_values)
    # print stmt_out
    try:
        cursor_out.execute(stmt_out)
    except Exception, e:
        print stmt_out
        print repr(e)
    # print cursor_out.cursor.info()
    # if index == 0:
    #   break


# now write sourceFile/timestamp data

updatetime = myspot.sql_get_updatetime(source_db, table_in)
stmt = "ALTER TABLE %s ADD COLUMN sourceFile CHAR(64)" % table_out
cursor_out.execute(stmt)
stmt = "ALTER TABLE %s ADD COLUMN sourceFileTimeStamp TIMESTAMP" % table_out
cursor_out.execute(stmt)
sourceFile = source_db +'.' + table_in
stmt = (""" UPDATE %s  SET sourceFile = '%s',
        sourceFileTimeStamp = '%s' WHERE sourceFile IS NULL """
        % (table_out, sourceFile, updatetime ))
cursor_out.execute(stmt)



print "\nOK: Script terminated successfully\n"



