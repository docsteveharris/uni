#!/usr/local/bin/python

#  ## Clone table after removing duplicate primary keys - add dups to dvr
# _______________________________________________________________________
# CreatedBy:    Steve Harris

# CreatedAt:    120802
# ModifiedAt:   120802

# Filename: index_table.py
# Project:

# ## Description

# NOTE: 2012-09-05 -
# Make a copy of a table (old_name -> new_name)
# with a primary key as defined in the table dictionary
# Records which would violate the primary key are recorded in the keys_dvr table
# Use onyl for primary tables: other tables will have a primary key and index defined in make_table

# ## Dependencies

# - MySQL database name and connection
# - Name of table in MySQL database
# - Tabla specification dicitonary
# - keys_dvr table

# ____


import sys      # file input / output
# TODO: 2012-08-17 - push local myspot changes back to main myspot
# Use local version of myspot module
# You can switch back to using the main myspot module if you push local changes back

# sys.path.remove('/Users/steve/usr/local/lib')
# sys.path.append('/Users/steve/data/spot_early/local/lib_usr')

                # handles multiline MySQL statements
from mypy import sql_multistmt
                # default table spec dictionaries
from myspot import (get_yaml_dict, sql_connect, dict_get_keys, sql_get_cols,
    quick_mysql_tab, sql_prepare_insert)

import MySQLdb  # MySQL database wrapper
import argparse

def get_tspec(tdict, tab_name):
    # Pull table spec
    # needs tdict
    try:
        tspec = [t for t in tdict if t['tablename'] == tab_name][0]
        return tspec
    except IndexError:
        print "ERROR: %s not found in table dictionary" % tab_name
        sys.exit(1)


def derive_pkey_for_correction_fr(corr_item, pkeys):
    "Return the primary key for the correction"
    # convert pkey fields to string and sort into correct order
    corr_pkey_dict = {k[0]:k[1] for k in corr_item.items() if k[0] in pkeys}
    corr_pkey = sorted(corr_pkey_dict.items(), key=lambda k: pkeys.index(k[0]))
    corr_pkey = tuple([str(i[1]) for i in corr_pkey])
    return corr_pkey


# Parse command line options
# ____________________________
parser = argparse.ArgumentParser(description =
    "Clone table after removing duplicate primary keys - add dups to dvr")
# Mandatory args (positional)
parser.add_argument("target_db", help=":target MySQL database name")
parser.add_argument("target_table", help=":name of table to be cleaned")

parser.add_argument("-so", "--suffix_original",
    help=":suffix of unindexed table (default '_import')",
    default='_import')
parser.add_argument("-si", "--suffix_index",
    help=":suffix of table to be created (default '_ix')",
    default='_ix')


# uses the table dictionary (found in the myspot module unless specified here)
parser.add_argument("-d", "--dict",
    help=":path to YAML table specification",
    default='table')

args = parser.parse_args()
parser.parse_args()

t_tab = args.target_table
o_tab = args.target_table + args.suffix_original
c_tab = args.target_table + args.suffix_index
print "\nOK: Running index_table.py for %s\n" % o_tab

cursor, connection = sql_connect(args.target_db, connection=True)

tdict = get_yaml_dict('table', local_dict=True)
fdict = get_yaml_dict('field', local_dict=True)
cdict = get_yaml_dict('corrections', local_dict=True)

tspec = get_tspec(tdict, t_tab)
pkeys = tspec['pkey']

# if there are excel sheet, row references then grab these as well
field_names = sql_get_cols(args.target_db, o_tab)
if 'modifiedat' in field_names:
    field_names.remove('modifiedat')

excel_source = False
if 'excel_sheet' in field_names and 'excel_row' in field_names:
    excel_source = True
#     stmt = """ SELECT excel_sheet, excel_row FROM %s """ % (o_tab)
#     cursor.execute(stmt)
#     row_keys = cursor.fetchall()
#     row_keys = ["sheet %s row %d" % (i[0], i[1]) for i in row_keys]
# else:
    # assume sql_source

# CHANGED: 2012-09-07 - adds a unique row index to all tables
stmt = "ALTER TABLE %s DROP COLUMN tab_serial" % o_tab
try:
    cursor.execute(stmt)
except:
    print "WARNING: tab_serial column did not exist in %s" % o_tab

stmt = """ALTER TABLE %s ADD COLUMN tab_serial INT UNSIGNED NOT NULL
    AUTO_INCREMENT FIRST, ADD UNIQUE KEY (tab_serial)""" % o_tab
cursor.execute(stmt)
# NOTE: 2012-09-07 -  force commit here else does not see new column
# CHANGED: 2012-10-05 - connection now passed back by sql_connect function
connection.commit()
stmt = """ SELECT tab_serial FROM %s """ % (o_tab)
cursor.execute(stmt)
row_keys = cursor.fetchall()


#  ======================================================
#  = Case-by-case corrections before importing the data =
#  ======================================================
# NOTE: 2012-10-08 - this will only run the first time it sees the import table
# else the correction will already have been parsed
# CHANGED: 2012-12-21 - now checks to make sure cdict is not empty
if cdict is not None:
    if len(cdict) > 0:
        cdict = [i for i in cdict
            if i['table'] + '_import' == o_tab and i['database'] == args.target_db
            and i['type'] == 'row_only']
        for correction in cdict:
            print correction['statement']
            cursor.execute(correction['statement'])
            print "NOTE: %d updates" % cursor.rowcount

# Pull all rows
stmt = """ SELECT %s FROM %s """ % (", ".join(field_names), o_tab)
cursor.execute(stmt)
rows = cursor.fetchall()
# convert fr tuple to list so can update values later
rows = [list(i) for i in rows]


row_hashes = []
for row in rows:
    row_dict = dict(zip(field_names, row))
    row_key = hash(tuple([str(row_dict[key]) for key in pkeys]))
    row_hashes.append(row_key)
# print len(rows)
# print len(row_hashes)
# print len(set(row_hashes))


rows_clean = []
index_errors = []
hash_rows = zip(row_hashes, rows)

#  =====================================================================================
#  = Update rows with corrections from dvr where those corrections affect primary keys =
#  =====================================================================================
from myspot import make_talias_dict
from myspot import clean_field_names
from myspot import sql_2mysql

fdict_lookup = {}
talias_dict = make_talias_dict(fdict)
falias_dict = talias_dict['no_alias']
if t_tab in talias_dict:
    falias_dict.update(talias_dict[t_tab])

for f in fdict:
    fdict_lookup[f['fname']] = f
corrections = False
if 'corrections_table' in tspec and tspec['corrections_table'] is not None:
    corrections = True
    corrections_table = tspec['corrections_table']
    corr_fields = pkeys[:]  # shallow copy pkeys
    corr_fields.extend(
            ["field", "value", "validation_message",
            "new_response_code", "new_response_value", 'new_response_note'])
    # select all entries with a 2 (delete item) or 3 (update item) response
    # ignore 'Major' errors as these are a hangover from the stata code
    stmt = """SELECT %s FROM %s
                WHERE new_response_code
                AND field != 'Major'""" % (
            ', '.join(corr_fields), corrections_table)
    cursor.execute(stmt)
    corrections = cursor.fetchall()

    print "OK: Fetched %d entries from the corrections table" % len(corrections)
    corr_items = []
    for corr_row in corrections:
        row_corr_dict = dict(zip(corr_fields, corr_row))
        corr_items.append(row_corr_dict)

    # now convert field names in dvr to 'true' field names
    corr_set = list(set([corr_item['field'] for corr_item in corr_items]))
    # print corr_set
    corr_aliases = dict(zip(corr_set, clean_field_names(corr_set)))
    if 'ALIAS_NOT_FOUND_IN_FIELD_DICT' in corr_aliases.values():
        print "ERROR: Unable to find alias for field"
        print corr_aliases
        sys.exit(1)

    # now loop through all corrections but skip if the field is not a primary key field
    for corr_item in corr_items:
        # CHANGED: 2012-10-08 - code 6 handling not written but not tested
        # Check for code '6' that requests the row of data be dropped
        if corr_item['new_response_code'] == 6:
            corr_pkey = derive_pkey_for_correction_fr(corr_item, pkeys)
            corr_pkey_hash = hash(corr_pkey)
            if corr_pkey_hash not in row_hashes:
                print "WARNING: Could not delete row %s" % str(corr_pkey)
                continue
            row_index = row_hashes.index(corr_pkey_hash)
            # NOTE: 2012-10-08 - see http://stackoverflow.com/questions/627435/how-to-remove-an-element-from-a-list-by-index-in-python
            del rows[row_index]
            continue

        # Otherwise skip if it is not a correction
        elif corr_item['new_response_code'] != 3:
            continue
        # make sure that the field refers to a primary key
        if corr_aliases[corr_item['field']] not in pkeys:
            continue
        else:
            print "NOTE: %s refers to primary key (%s) - will update" % (corr_item, pkeys)
            field_name = corr_aliases[corr_item['field']]
            corr_pkey = derive_pkey_for_correction_fr(corr_item, pkeys)
            corr_pkey_hash = hash(corr_pkey)
            print "NOTE: sorted primary key for correction item is %s" % str(corr_pkey)
            fspec = fdict_lookup[field_name]
            sqltype = fspec['sqltype']
            if 'sqllen' in fspec:
                sqllen = fspec['sqllen']
            else:
                sqllen = 255
            print "NOTE: Try to parse proposed replacement value %s for field %s" % (
                corr_item['new_response_value'], field_name)
            success, corr_clean = sql_2mysql(corr_item['new_response_value'],
                sqltype, sqllen, format='iso')
            print "NOTE: Parsed to %s" % corr_clean

        if corr_pkey_hash not in row_hashes:
            print "WARNING: Could not apply correction for %s" % str(corr_pkey)
            continue

        row_index = row_hashes.index(corr_pkey_hash)
        field_index = field_names.index(field_name)
        print "NOTE: Row to update is %s" % rows[row_index]
        print "NOTE: Field to update is %s" %  rows[row_index][field_index]
        rows[row_index][field_index] = corr_clean
        print "NOTE: Field UPDATED is %s" %  rows[row_index][field_index]


# Rebuild hashes now you have updated the values
row_hashes = []
for row in rows:
    row_dict = dict(zip(field_names, row))
    row_key = hash(tuple([row_dict[key] for key in pkeys]))
    row_hashes.append(row_key)

rows_clean = []
index_errors = []
hash_rows = zip(row_hashes, rows)

index = 0
for row_hash, row in hash_rows:
    sql_dict, key_values, row_errors, pkeys_missing = {}, [], [], []
    row_dict = dict(zip(field_names, row))
    # debug = True
    # if debug:
    #     print len(key_rows), len(row_hashes), len(hash_rows)
    #     print row_hash, row
    #     print row_dict
    #     sys.exit(1)

    #  ======================================================
    #  = Raise an error where the primary key field is NULL =
    #  ======================================================
    for key in pkeys:
        key_values.append(row_dict[key])
        if row_dict[key] is None:
            pkeys_missing.append(key)

    #  =============================================
    #  = Raise an error where duplicate keys found =
    #  =============================================
    dup_count = row_hashes.count(row_hash)

    if pkeys_missing:
            row_errors.append("Missing essential field (primary key)")

    elif dup_count > 1:
        row_errors.append("%d rows with identical index fields found" % dup_count)
        # print "NOTE: %d copies of %s found" % (row_hashes.count(row_hash), key_values)


    else:
        # if no errors then add row to list to be added
        rows_clean.append(row)

    # if errors then insert into keys_dvr
    if row_errors:
        # print row_errors
        for validation_msg in row_errors:

            sql_dict = {
                'sourceFile': row_dict['sourceFile'],
                'sourceFileTimeStamp': row_dict['sourceFileTimeStamp'],
                'sql_table': o_tab,
                'tab_serial': row_dict['tab_serial'],
                'validation_msg': validation_msg,
                'key_fields': ', '.join(pkeys),
                'missing_fields': ', '.join(pkeys_missing),
                'key_values': ', '.join(['UNTIMED' if str(i) == '0:01:01'
                    else str(i) for i in key_values])
            }
            if excel_source:
                sql_dict['excel_sheet'] = row_dict['excel_sheet']
                sql_dict['excel_row'] = row_dict['excel_row']


            # print sql_dict
            stmt = "REPLACE INTO keys_dvr (%s) VALUES ('%s')" % (
                    ", ".join([str(i) for i in sql_dict.keys()]),
                    "', '".join([str(i) for i in sql_dict.values()]))
            # print stmt
            cursor.execute(stmt)

    index += 1
    if index % 100 == 0:
        # NOTE: 2012-12-29 - ending print with comma stops newline
        print ".",


#  =============================================================
#  = Now create indexed _ix suffix table and insert clean rows =
#  =============================================================
success = quick_mysql_tab(cursor, fdict, c_tab, field_names)
for index, row in enumerate(rows_clean):
    stmt = sql_prepare_insert(c_tab, field_names, row)
    # print stmt
    cursor.execute(stmt)
    if index % 100 == 0:
        # NOTE: 2012-12-29 - ending print with comma stops newline
        print ".",

#  ==========================================
#  = Pen-ultimately! add a primary key to the table =
#  ==========================================
stmt = "ALTER TABLE %s ADD PRIMARY KEY (%s)" % (c_tab, ', '.join(pkeys))
try:
    # print stmt
    cursor.execute(stmt)
except:
    print "ERROR: Failed to create primary key for %s" % c_tab
    sys.exit(1)


# CHANGED: 2012-10-04 - early code added for SQL level corrections
#  ==========================================================
#  = Finally access the SQL level corrections and run these =
#  ==========================================================
# CHANGED: 2012-12-21 - now checks to make sure cdict is not empty
if cdict is not None:
    if len(cdict) > 0:
        cdict = [i for i in cdict
            if i['table'] + '_ix' == c_tab and i['database'] == args.target_db
            and i['type'] == 'table_wide']
        for correction in cdict:
            print correction['statement']
            cursor.execute(correction['statement'])
            print "NOTE: %d updates" % cursor.rowcount

print "\nOK: Script terminated successfully\n"
