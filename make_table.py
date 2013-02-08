#!/usr/local/bin/python

# CHANGED: 2012-08-22 - using the local environment install of python

# Given the table spec. field spec and validation spec
# Create
#   - table
#       - with metadata flags
#   - pickled dictionary of lists of dictionaries
#       - dictionary of fields
#       - each field is an list of identical length (column values)
#       - each cell is a dictionary of checks (and the value)
#   - shadow table of field level validation responses
#       - standardised so field is either ok, unusual, impossible
#   - list of queries
# ? the above could be options passed from the command line
#   - insert table into MySQL
#   - insert query list into MySQL


# Bugz

# To do - now
# - create rows object with meta-data
#   - validated field value (i.e. if field imposs - replace with NULL)
#   - metadata fields
#       - row valid flag
#       - any impossible fields flag
#       - count missing fields
#       - count unusual fields
#       - count imposs fields

# - take rows object, insert values and meta-data fields into MySQL
# - take rows object, insert queries into MySQL
#       -

# To do - someday
# TODO: 2012-08-20 - cross row queries i.e. flag sudden discontinuties in heart rate

import string
import datetime
import re
import sys
import itertools
# a faster version of pickle
import cPickle as pickle
import argparse

# TODO: 2012-08-17 - push local myspot changes back to main myspot
# Use local version of myspot module
# You can switch back to using the main myspot module if you push local changes back

sys.path.remove('/Users/steve/usr/local/lib')
sys.path.append('/Users/steve/data/spot_early/local/lib_usr')

from myspot import sql_connect
from myspot import get_yaml_dict
from myspot import make_talias_dict
from myspot import dict_get_keys
from myspot import reverse_field_dict
from myspot import sql_get_cols
from myspot import quick_mysql_tab
from myspot import sql_prepare_insert
from myspot import sql_2mysql
from myspot import extract_fields_from
from myspot import clean_field_aliases

from mypy import sql_multistmt

def get_tspec(tdict, tab_name):
    # Pull table spec
    # needs tdict
    try:
        tspec = [t for t in tdict if t['tablename'] == tab_name][0]
        return tspec
    except IndexError:
        print "ERROR: %s not found in table dictionary" % tab_name
        sys.exit(1)

# Given your data now re-build your row and column keys
def rcdict_from_rows(mylist, pkeys, req='rows', list_is='rows'):
    """
    Given a list of rows with entries in field dict for rowkey and field
    Returns either a row or col dict
    Uses pkeys to run double check
    """
    # extract rows if you have been handed cols
    if list_is == 'cols':
         mylist = zip(*mylist)
    if req == 'rows':
        new_dict = {}
        for row in mylist:
            rowkey = tuple([f['raw'] for f in row if f['field'] in pkeys])
            if rowkey in new_dict:
                print kkey_dict.values()
                print rrow_dict.values()
                print "WARNING: duplicate row key found"
                # report_duplicate_rowkey(rowkey)
            else:
                new_dict[rowkey] = row

    elif req == 'cols':
        cols = zip(*mylist)
        new_dict = {col[0]['field']:col for col in cols}

    else:
        print "ERROR: Specify either mylist or cols to build dict"
        sys.exit(1)

    return new_dict

def run_fchk(col, chk):
    """
    Run field checks
    - errors here should always be 'IMPOSSIBLES'?
    Given a column object (i.e. a list of clean values) and the check code
    Returns a dictionary with the following appended to list 'checks'
    - result: [ok|unusual|impossible]
    - type: field / row
    - check name
    """
    imposs_code = ''
    unusual_code = ''
    chk_field_name = col[0]['field']

    chk_type = chk['type']
    print "OK: For field %s, checking %s" % (chk_field_name, chk_type)

    chk_types = ['legal_vals', 'min', 'max', 'regex', 'raw']
    if chk_type not in chk_types:
        print "ERROR: Check type (%s) not specified" % chk_type
        sys.exit(1)

    # All checks assume the value to be tested is a variable called 'value'
    if chk_type == 'legal_vals':
        # sqltype = col[0]['sqltype']
        # sqltype_is_text = sqltype.lower() in ['char', 'text']
        # Make sure value list is in lower case too where values are strings
        legal_val_list = [str(val).lower() for val in chk['value']]
        # Impossible level check hence only imposs_code
        imposs_code = """ "'%s' not in %r" % (value, legal_val_list) """

    if chk_type == 'min':
        # NOTE: 2012-08-08 - might need to check if value and min are same type?
        min_value = chk['value']
        # print "NB: %r is type %r\n" % (col[0]['clean'], type(col[0]['clean']))
        # print "NB: %r is type %r\n" % (min_value, type(min_value))
        imposs_code = """ "%r < %r" % (value, min_value) """

    if chk_type == 'max':
        max_value = chk['value']
        imposs_code = """ "%r > %r" % (value, max_value) """

    if chk_type == 'regex':
        # NOTE: 2012-08-08 - not yet debugged as no regex in idpatient
        # print "NB: regex is %r" % chk['value']
        regex = re.compile(chk['value'], re.IGNORECASE)
        # NOTE: 2012-08-17 - do you need to escape string the value here?
        imposs_code = """ "regex.match('%s') == None" %  value """

    # essential = False
    # if chk_type == 'essential':
    #     imposs_code = """ "%r == None" % value """
    #     essential = True

    # Run the check across the column if check code exists else return now
    if imposs_code == '' and unusual_code == '':
        success = False
        return col, success

    results = []
    if chk['response'] == 'drop_record':
        chk_level = 'row'
    elif chk['response'] == 'drop_field':
        chk_level = 'field'
    else:
        print "ERROR: Unrecognised check respone for %s" % chk_field_name
        sys.exit(1)

    for index, item in enumerate(col):
        value = item['clean']
        if value == None:
            continue

        impossible, unusual = False, False

        try:
            if imposs_code != '':
                # double eval: 1st to code string, 2nd to code result
                impossible = eval(eval(imposs_code))
                # if impossible:
                #     print imposs_code
                #     print eval(imposs_code)
                #     print eval(eval(imposs_code))
            if unusual_code != '':
                unusual = eval(unusual_code)

        except TypeError:
            # DEBUGGING: 2012-08-08 -
            print "ERROR: TypeError ... this might help?\n"
            print "In row %d with key %s" % (
                index, item['rowkey'])
            print "Raw value %s was tested with %s" % (
                item['raw'], imposs_code)
            print "Hence value (%s) should have evaluated as %s" % (
                value, eval(imposs_code))
            print "Full item spec follows"
            for k,v  in item.items():
                print k, v
            sys.exit(1)

        if impossible or unusual:
            # DEBUGGING: 2012-08-30 - checking that field checks firing
            # print imposs_code , eval(imposs_code), impossible
            if impossible:
                result = 'impossible'
                code = eval(imposs_code)
            else:
                result = 'unusual'
                code = (unusual_code)

            chk_result = {'name':chk_type, 'result':result,
                'level':chk_level, 'code':code}
            # print item['field'], eval(imposs_code), chk_type, value, chk_result
            col[index]['checks'].append(chk_result)

    success = True
    return col, success


def run_rcheck(rows, chk):
    """
    Requires
    - a validation check specification (chk)
    - rows of data
    Returns
    - the same rows with chk_results appended to the 'checks' list item of the data
    """
    success = False
    chk_name = chk['checkname']
    print "OK: Running row check %s" % chk_name
    chk_fields = chk['fields']

    # Produce a list of field names for the row
    fields =[i['field'] for i in rows[0]]

    # Check python code for check is available
    if 'python' in chk['code']:
        chk_code = chk['code']['python']
    else:
        print "ERROR: Python code for check not found"
        sys.exit(1)

    # Check that the fields required for the check exist in the row
    if False in [f in fields for f in chk_fields]:
        print "ERROR: Fields (%s) required for check (%s) not found" % \
            (chk_fields, chk_name)
        sys.exit(1)

    # Replace fields with row-index-dictionary in chk_code
    values = []
    findices = []
    for field in chk_fields:
        field_regex = r"\b%s\b" % field
        findex = fields.index(field)
        findices.append(findex)
        replacement = "row[%d]['clean']" % findex
        # CHANGED: 2012-08-10 - use regex to replace not string so can specifiy whole words
        # else fio2 replaces in fio2u
        # chk_code = re.chk_code.replace(f, r)
        chk_code = re.sub(field_regex, replacement, chk_code)
        values.append(replacement)

    # Now extract check levels
    imposs_code, unusual_code, standard_code = '', '', ''
    if 'chklvls' not in chk:
        print "ERROR: check levels not specified for %s" % chk_name

    # NOTE: 2012-08-09 - where chklvls are boolean (True/False) then these lines
    # should have no effect ...
    if 'impossible' in chk['chklvls']:
        imposs_val = str(chk['chklvls']['impossible'])
        imposs_code = chk_code.replace('chklvl', imposs_val)
    if 'unusual' in chk['chklvls']:
        unusual_val = str(chk['chklvls']['unusual'])
        unusual_code = chk_code.replace('chklvl', unusual_val)

    # print imposs_code, unusual_code

    unusual_result, imposs_result, na_result = False, False, False

    unusuals, impossibles = 0, 0
    for row in rows:
        # Check field values exist (or wrap this in a try block)
        row_vals = [eval(v) for v in values]
        if None in row_vals:
            # print "Values: %r ... skipping" % row_vals
            continue

        debug = False

        if unusual_code != '':
            if debug:
                print row_vals
                print imposs_code
            unusual_result = (eval(unusual_code))
            if unusual_result:
                unusuals += 1

        if imposs_code != '':
            if debug:
                print row_vals
                print imposs_code
            imposs_result = (eval(imposs_code))
            if imposs_result:
                impossibles += 1

        # print "Values: %r Unusual: %r Impossible: %r" % \
        #    (row_vals, unusual_result, imposs_result)

        if imposs_result:
            result = 'impossible'
            code = imposs_code
        elif unusual_result:
            result = 'unusual'
            code = unusual_code
        else:
            result = 'ok'

        # Now based on level of check either post results as field/row issues
        for f in findices:
            if result != 'ok':
                chk_result = {'name':chk_name, 'result':result,
                    'level':chk['level'], 'code':code}
                row[f]['checks'].append(chk_result)




    # print unusuals, impossibles
    success = True
    return rows, success

def row_to_insert_dict(row, fields, result = 'validated'):
    """
    Given the row contains fields
    - field: (i.e. field name)
    - values as
        - raw
        - clean - as per fspec
        - validated - as per checks
        - labelled - as per validated plus label if available
        NOTE: 2012-08-18 -  labelled requires different field spec
    Returns an insert_dict
    """
    if result not in ['raw', 'clean', 'validated', 'labelled']:
        print "ERROR: unrecognised result type %s" % result
        sys.exit(1)

    insert_dict = {i['field']:i[result] for i in row}

    # print insert_dict  # DEBUGGING: 2012-08-24 -


    n_miss, n_unusual, n_imposs = 0, 0, 0
    uchks, ichks = [], []
    valid_row, valid_all_fields = 1, 1

    for i in row:

        n_unusual += i['unusual_rchks'] + i['unusual_fchks']
        n_imposs += i['imposs_rchks'] + i['imposs_fchks']

        # CHANGED: 2012-08-30 - checks renamed to include field name !UNTESTED
        # uchks.extend(i['list_unusualchks'])
        # ichks.extend(i['list_imposschks'])
        # print i['checks']

        for chk in i['checks']:
            if chk['name'] in ['legal_vals', 'min', 'max', 'regex', 'essential', 'raw']:
            # if chk['level'] == 'field':
                chk_name = chk['name'] + '_' + i['field']
            else:
                chk_name = chk['name']

            if chk['result'] == 'impossible':
                ichks.append(chk_name)
            else:
                uchks.append(chk_name)

        # for chk in i['list_unusualchks']:
        #     chk_name = chk['name']
        #     if chk['level'] == 'field':
        #         chk_name = chk_name + '_' + i['field']
        #     uchks.append(chk_name)

        # for chk in i['list_imposschks']:
        #     chk_name = chk['name']
        #     if chk['level'] == 'field':
        #         chk_name = chk_name + '_' + i['field']
        #     ichks.append(chk_name)


        if i['empty']:
            n_miss += 1

        if i['imposs_rchks']:
            valid_row = 0

    if n_imposs > 0:
        valid_all_fields = 0


    # Uses the underscore to look for metadata fields
    meta_fields = [f for f in fields if f[0] == '_']
    for f in meta_fields:
        if f == '_valid_row':
            insert_dict[f] = valid_row
        elif f == '_valid_allfields':
            insert_dict[f] = valid_all_fields
        elif f == '_count_missfields':
            insert_dict[f] = n_miss
        elif f == '_count_unusualfields':
            insert_dict[f] = n_unusual
        elif f == '_count_impossfields':
            insert_dict[f] = n_imposs
        elif f == '_list_unusualchks':
            insert_dict[f] = ', '.join(set(uchks))
        elif f == '_list_imposschks':
            insert_dict[f] = ', '.join(set(ichks))
        else:
            print "ERROR: metadata field %s not recognised" % f
            sys.exit(1)

    return insert_dict


#  =================================
#  = Handle command line arguments =
#  =================================
parser = argparse.ArgumentParser(description =
"""
Make table based on provided table spec\n
- assumes all tables in same database\n
""")

# Mandatory args (positional)
parser.add_argument("database", help=":target/source MySQL database name")
parser.add_argument("table_spec", help=":name of table to be created")

# Option to pickle file
parser.add_argument("-p", "--pickle_it",
    help=":pickle file to ../data/pickle/tab_name",
    action="store_true",
    default=False)

# Option to sql export file to different db
parser.add_argument("-s", "--sql_out",
    help=":export file to the sql database specified")

# Option to specify mysql output level (raw, clean, validated)
parser.add_argument("-o", "--output_lvl",
    help=":MySQL output level (raw, clean, validated)",
    default='clean')

args = parser.parse_args()
parser.parse_args()


db_name = args.database
tab_name = args.table_spec
pickle_it = args.pickle_it
mysql_output_lvl = args.output_lvl
print "\nOK: Running make_table.py for %s \n" % tab_name

if args.sql_out is None:
    sql_out = db_name
else:
    sql_out = args.sql_out



#  =====================
#  = Load dictionaries =
#  =====================

# TODO: 2012-08-17 - reset to global dict when development finished
fdict = get_yaml_dict('field', local_dict = True)
tdict = get_yaml_dict('table', local_dict = True)
vdict = get_yaml_dict('checks', local_dict = True)

tspec = get_tspec(tdict, tab_name)

talias_dict = make_talias_dict(fdict)
# print talias_dict
falias_dict = talias_dict['no_alias']
# print len(falias_dict)
# print tab_name
if tab_name in talias_dict:
    falias_dict.update(talias_dict[tab_name])
# print len(falias_dict)
# print falias_dict

if 'corrections_table' not in tspec:
    pass
elif tspec['corrections_table'] is None:
    pass
else:
    falias_dict.update(talias_dict[tspec['corrections_table']])

# print len(falias_dict)
if 'sourcetables' in tspec and len(tspec['sourcetables']):
    for t in tspec['sourcetables']:
        falias_dict.update(talias_dict[t])
        # print len(falias_dict)
# print falias_dict
# sys.exit(1)
# print falias_dict
if 'essentialfields' in tspec:
    fields_essential = tspec['essentialfields']
else:
    fields_essential = []
    
# print fields_essential

# define new sql_type based on valias
for fspec in fdict:
    # CHANGED: 2013-01-28 - skips fields without sqltype definition 
    # such fields cannot be part of the import/index/make process
    if 'sqltype' not in fspec:
        continue
    sqltype = fspec['sqltype']
    # CHANGED: 2012-09-18 - to handle multiple valias types
    if 'valias' in fspec or 'valiases' in fspec:
            valias = None
            if 'valiases' in fspec:
                for v in fspec['valiases']:
                    if tab_name in v['valias_tables']:
                        valias = v['valias']
            else:
                valias = fspec['valias']
            if valias:
                valias_values = valias.values()
                maxrank = 0
                for v in valias_values:
                    if v is None:
                        continue
                    rank = {int:1, float:2, str:3}[type(v)]
                    if rank > maxrank:
                        maxrank = rank
                sqltype = {1:'smallint', 2:'float', 3:'char'}[maxrank]
                # valias_type = type(fspec['valias'].values()[0])
                # if valias_type == str:
                #     sqltype = 'char'
                # elif valias_type == int:
                #     sqltype = 'smallint'
                # elif valias_type == float:
                #     sqltype = 'float'
                # else:
                #     print "ERROR: Unrecognised type: %r" % valias_type
                #     sys.exit(1)

    fspec['sqltype_new'] = sqltype

    if 'derived' in fspec:
        derived_field = True
    else:
        derived_field = False
    fspec['derived'] = derived_field


# and prepare backwards version of field dict for looking up field aliases
# falias_dict = reverse_field_dict()

# and prepare a dictionary version of fdict
# first check there are no identical field names
ttemp =[f['fname'] for f in fdict]
if len(ttemp) > len(set(ttemp)):
    print "ERROR: Duplicate field names found in dictionary"
    sys.exit(1)
else:
    fdict_lookup = {}
    for f in fdict:
        fdict_lookup[f['fname'].lower()] = f

# print fdict_lookup['lrti_micro']
# sys.exit(1)  # DEBUGGING: 2012-08-24 -



# Get primary key definition - should be a list of fields
pkeys = tuple(tspec['pkey'])


# Derived fields
if 'derivedfields' in tspec:
    derivedfields = tspec['derivedfields']
else:
    derivedfields = []

# SQL database connection
cursor = sql_connect(db_name)

# Run pre-flight SQL script
if 'preflight' in tspec and tspec['preflight'] is not None:
    sql_multistmt(cursor, tspec['preflight'])

# Delete existing table
# Means field list order will not depend on sourcefields or sql_select
# But instead on sql_get cols
# stmt = "DROP TABLE IF EXISTS %s" % tab_name
# cursor.execute(stmt)

# Decide if this is a primary table or a derived one
# i.e. if you need an sql_create statement
if tspec['type'] == 'primary' and 'sql_select' not in tspec:
    # Assume that raw indexed 'source tables' always have the suffix _ix
    # Does not need the sql_create statement to be specified
    # CHANGED: 2012-12-21 - uses the same method as import_sql to resolve aliases
    tab_ix = tab_name + '_ix'
    target_fields = clean_field_aliases(tspec['sourcefields'], tab_name)
    fields = [i for i in target_fields if i != 'ALIAS_NOT_IN_FIELD_DICT']
    fields.append('tab_serial')
    # print falias_dict.items()
    # fields = ['tab_serial']
    # for f in tspec['sourcefields']:
    #     if f in fdict_lookup:
    #         fields.append(f)
    #     else:
    #         fields.append(falias_dict[f])
    # fields = [falias_dict[f] for f in tspec['sourcefields']]
    stmt = "SELECT %s FROM %s" % (', '.join(fields), tab_ix)
else:
    if 'sql_select_fields' in tspec:
        fields = tspec['sql_select_fields']
        stmt = tspec['sql_select'].replace(
            'sql_select_fields', ', '.join(fields))
    elif 'sourcefields' not in tspec:
        # derive fields from select statement
        fields = extract_fields_from(tspec['sql_select'])
        stmt = tspec['sql_select']
    else:
        print "WARNING: field sort order specified separately to SELECT statement"
        print "- check order source field order matches SELECT statement"

# Check your field extraction worked
# print fields
# sys.exit(1)
assert len(fields) > 0
# Pull data from MySQL
# print stmt
cursor.execute(stmt)
if cursor.rowcount == 0:
    print "WARNING: No rows for %s\nExiting ...\n" % tab_name
    sys.exit(1)
else:
    print "NOTE: %d rows found" % cursor.rowcount

# CHANGED: 2012-08-13 - prev pulling keys as separate sql statements
# Now all extracted from rows to minimise chance of key errors
# Select from table and return to cursor

# Fetches rows as tuples
orows = cursor.fetchall()
# Keep everything as tuples for now so 'immutable'
ocols = zip(*orows)
ocols_dict = dict(zip(fields, ocols))

pkey_list = zip(*[ocols_dict[pkey] for pkey in pkeys])
# Pull 2nd level (site) key
if 'sitekey' in tspec:
    sitekey = tspec['sitekey']
    sitekey_list = ocols_dict[sitekey]

# CHANGED: 2012-08-14 - double double check key is applied correctly
orows_dict = dict(zip(pkey_list, orows))
# Check the dictionary by comparing pkey_list vals with orow vals
for item in orows_dict.items():
    kkey, rrow = item[0], item[1]
    # Create a dictionary of primary key fields based on key,val from orows_dict
    kkey_dict = dict(zip(pkeys, kkey))
    rrow_dict = dict(zip(fields, rrow))
    # Filter row dict so just contains primary key fields
    rrow_dict = {key:rrow_dict[key] for key in pkeys}
    if kkey_dict.values() != rrow_dict.values():
        print kkey_dict.values()
        print rrow_dict.values()
        print "ERROR: primary key not correctly applied to rows"
        sys.exit(1)

#  =================================
#  = Prepare table of dictionaries =
#  =================================

rows = []
for index, orows_item in enumerate(orows_dict.items()):
    rowkey, thisrow = orows_item[0], orows_item[1]

    kv = zip(fields, thisrow)
    if len(derivedfields) > 0:
        kv_derived = [(field,None) for field in derivedfields]
        kv.extend(kv_derived)

        # print kv
    # import pdb; pdb.set_trace()
    row = [{    'field':kkey,
                'raw':vval,
                'rowkey': rowkey,
                'sqltype': fdict_lookup[kkey]['sqltype_new'],
                'derived': fdict_lookup[kkey]['derived'],
                'clean': None,
                'labelled':None,
                'validated':None,
                'empty': False,
                'unavailable': False,
                'checks':[],
                'list_unusualchks': [],
                'list_imposschks': [],
                'unusual_fchks':0,
                'imposs_fchks':0,
                'unusual_rchks':0,
                'imposs_rchks':0}
                for kkey, vval in kv]
    rows.append(tuple(row))
    # NOTE: 2012-11-14 - cannot handle v large lists: memory use goes to 6GB!!
    # if index > 16000:
    #     print index, sys.getsizeof(rows)
    #     # NOTE: 2012-11-14 - debugging
    #     import pdb; pdb.set_trace()

fields.extend(derivedfields)

# NOTE: 2012-08-13 - IMPORTANT THIS IS A GOOD DATA STRUCTURE
rows_dict = rcdict_from_rows(rows, pkeys)
del rows
# BUT!!! only one can be primary (rows)
# as updates will not propogate through automatically)
# hence delete this now so you are forced to remake as needed
# TODO: 2012-08-13 - pickle this object


# Now rows and cols are tuples of tuples of dictionaries
# You can extract columns items by
# list_of_spot2 = [item['raw'] for item in cols_dict['spo2']]

# You can extract row items by using the pkey e.g.
# print pkey_list[0]
# print [item['raw'] for item in rows_dict[pkey_list[0]]]

# Update row dictionary

#  ===========================================================
#  = Update raw values with corrections from DVR returns etc =
#  ===========================================================

# CHANGED: 2012-09-07 - will not update primary key fields:
# this has to be done in index_table

# Before working with columns now update with corrections from DVR
# Loop though rows
corrections = False
correction_key_search_OK = False
if 'corrections_table' in tspec and tspec['corrections_table'] is not None:
    corrections = True
    corrections_table = tspec['corrections_table']
    corr_fields = list(pkeys) # copy pkey to new list then extend
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

    # # Prepare a field alias lookup dictionary for the DVR
    # dvr_alias_dict = falias_dict['id_dvr']
    # dvr_alias_dict.update(falias_dict['no_alias'])

    # print rows_dict.keys()[0]
    for corr_row in corrections:
        corr_dict = dict(zip(corr_fields, corr_row))
        # print corr_dict


        # corr_pkey = tuple()
        corr_pkey = tuple([i.lower() if isinstance(i, str) else i
            for i in corr_row[:len(pkeys)]])
        # print corr_pkey
        # print rows_dict.keys()[:3]
        # sys.exit(1)
        # print corr_dict
        # print rows_dict.keys()[:10]
        # sys.exit(1)
        if corr_pkey in rows_dict:
            correction_key_search_OK = True
            print corr_pkey
            thisrow = rows_dict[corr_pkey]
            thesefields = [item['field'] for item in thisrow]
            try:
                # CHANGED: 2012-08-20 - now looks first for field in fdict
                # Will this cause problems if field appears in in alias with different meaning?
                if corr_dict['field'] in fdict_lookup:
                    field_name = corr_dict['field']
                else:
                    field_name = falias_dict[corr_dict['field']]

            except KeyError:
                print "ERROR: field (%s) not found in field dictionary" % corr_dict['field']
                print "NB: DVR aliases (id_long_dvr, id_wide_dvr) will need adding to fdict"
                # print corr_dict['field']
                # print fdict_lookup.keys()
                # print falias_dict.keys()
                # sys.exit(1)
                sys.exit(1)



            # print field_name, thesefields
            success = False
            if field_name in thesefields:
                thisrow_dict = dict(zip(thesefields, thisrow))
                thisfield = thisrow_dict[field_name]
                # Skip if the correction field in in the primary key list
                # append corrections and copy existing values to field
                thisfield['raw_pre_dvr'] = thisfield['raw']
                thisfield['dvr_response_code'] = corr_dict['new_response_code']
                thisfield['dvr_response_note'] = corr_dict['new_response_note']
                thisfield['dvr_response_value'] = corr_dict['new_response_value']
                # print thisfield['dvr_response_code'], corr_dict['new_response_note']

                if field_name in pkeys:
                    # NOTE: 2012-09-07 - this should be impossible
                    # since pkey fields were never in origial dvrs
                    print "NOTE: skipping %s correction for %s - in primary keys" % (
                        corr_dict['field'], corr_row[:len(pkeys)])
                    continue
                elif corr_dict['new_response_code'] == 3:
                    dvr_value = corr_dict['new_response_value']
                    # update with returned response
                    # returned response needs to be converted from string
                    fspec = fdict_lookup[thisfield['field']]
                    sqltype = fspec['sqltype']
                    if 'sqllen' in fspec:
                        sqllen = fspec['sqllen']
                    else:
                        sqllen = 255

                    # CHANGED: 2012-09-14 -
                    # now assume the response will be the label not the value then back convert
                    if 'vallab' in fspec:
                        vallab_dict_reverse = {str(i[1]).lower(): i[0]
                            for i in fspec['vallab'].items()}
                        # print dvr_value, vallab_dict_reverse
                        if str(dvr_value).lower() in vallab_dict_reverse:
                            dvr_value = vallab_dict_reverse[str(dvr_value).lower()]
                            if sqltype in ['int', 'tinyint', 'smallint']:
                                dvr_value = int(dvr_value)
                            elif sqltype == 'char':
                                dvr_value = str(dvr_value)
                        # print dvr_value, type(dvr_value)

                    success, cleaned_response = sql_2mysql(
                        dvr_value, sqltype, sqllen, format='iso')
                    thisfield['raw'] = cleaned_response

                elif corr_dict['new_response_code'] == 2:
                    # returned response is 2 hence delete current val
                    thisfield['raw'] = None
                    success = True
                else:
                    pass

                if corr_dict['new_response_code'] in [2,3]:
                    print "OK: In response to ... %s" % corr_dict['validation_message']
                    if not success and cleaned_response is None:
                        print "WARNING: ?failed to update %s from %s to %s for %s" % (
                            field_name, thisfield['raw_pre_dvr'],
                            dvr_value, corr_pkey)
                        # sys.exit(1)
                        # raw_input('DEBUGGING: Pause and read then hit <ENTER>')
                    else:
                        print "OK: Updated %s from %s to %s for %s" % (
                            field_name, thisfield['raw_pre_dvr'],
                            thisfield['raw'], corr_pkey)

else:
    print "\nWARNING: corrections_table not specified for %s\n" % tab_name
if corrections == True and correction_key_search_OK == False:
    print "\nWARNING: corrections primary key(s) never found in row dictionary"

#  ====================
#  = Clean raw values =
#  ====================
cols_dict = rcdict_from_rows(rows_dict.values(), pkeys, req='cols')
del rows_dict

print "OK: Cleaning raw values"
for field, col in cols_dict.items():
    # Can't clean a derived field so skip
    if field in derivedfields:
        continue
    # pull col using field key, convert to list so can update values
    col = list(col)
    fspec = fdict_lookup[field]
    # DEBUGGING: 2012-08-20 -
    # if 'valias' in fspec:
    #   print field, fspec['valias']
    for index, item in enumerate(col):
        value = item['raw']
        # automatically transfer primary key values without further checks
        # if field in list(pkeys):
        #     if isinstance(value, str):
        #         value = value.lower()
        #     item['clean'] = value
        #     item['unavailable'] = False
        #     continue
        # for everything else then try and clean
        reported_na = False
        # print "\n", item['clean']

        # convert all strings to lower case
        if isinstance(value, str):
            value = value.lower()

            if len(value) == 0 or value.isspace() is True:
                value = None

            # where the value is clearly indicated as not available
            else:
                na_regex = re.compile(r"\b(nr|na|n/a|missing|unknown|z+)\b")
                if na_regex.match(value) != None:
                    reported_na = True
                    value = None
                    item['unavailable'] = True
                    # TODO: 2012-08-17 - need method of looking for (SPOT)light encoded unavailable

        # where a regex is specified for replacing values
        if 'substitute' in fspec and value != None:
            regex = re.compile(fspec['substitute']['find'])
            value = regex.sub(fspec['substitute']['replace'], str(value))

        # where value aliases are provided
        # if 'valias' in fspec:
        #     if value in fspec['valias']:
        #         value = fspec['valias'][value]
                # DEBUGGING: 2012-08-24 - type conversion already done: don't need ...
                # if (fspec['sqltype_new'] in ['tinyint', 'smallint']
                #     and value.isdigit()):
                #     value = int(value)
        # CHANGED: 2012-09-14 - now converts valias dict as needed: untested
        # CHANGED: 2012-09-18 - now handles multiple valias types
        if 'valias' in fspec or 'valiases' in fspec:
            valias = None
            if 'valiases' in fspec:
                for v in fspec['valiases']:
                    if tab_name in v['valias_tables']:
                        valias = v['valias']
            else:
                valias = fspec['valias']
            if valias:
                valias_dict = dict((str(i[0]).lower(), i[1]) if isinstance(i[0], str)
                    else (i[0], i[1]) for i in valias.items())
                if value in valias_dict:
                    value = valias_dict[value]


        if value == None:
            item['empty'] = True

        if value == None and field in fields_essential:
            chk_result = {'name': 'essential', 'result': 'unusual',
                'level': 'field', 'code': ''}
            # print field, chk_result
            item['checks'].append(chk_result)

        # NOTE: 2012-08-14 - this inserts value into cols_dict
        # CHANGED: 2012-08-28 - convert to lower case again after valias operations
        # convert all strings to lower case
        if isinstance(value, str):
            value = value.lower()
        item['clean'] = value
        # print   item['field'], item['raw'], value, col[index]['clean']
        if 'raw_pre_dvr' in item and item['raw_pre_dvr'] != item['dvr_response_value']:
            print item['rowkey'], item['field'], item['raw_pre_dvr'], item['dvr_response_value'], item['raw'], item['clean']

# sys.exit(1)

#  ===============================================
#  = Derive fields now you have clean raw values =
#  ===============================================
rows_dict = rcdict_from_rows(cols_dict.values(), pkeys, list_is = 'cols')
del cols_dict
print "OK: Generating derived variables"
debug = False
fields_derived_ok = False
if len(derivedfields) > 0:

    for dfield in derivedfields:
        fspec = fdict_lookup[dfield]
        code = fspec['code']['python']
        print "OK: Deriving %s ..." % dfield
        # print fspec['primaryfields']
        for pfield in fspec['primaryfields']:
            # Replace fields in code with their dictionary keys
            if pfield not in fields:
                # CHANGED: 2012-12-13 - no longer an error
                # where a field is missing it will be assigned a None value
                print "WARNING: primary field %s assigned None value" % (pfield)
                # print "ERROR: primary field %s not in fields(%s)" % (pfield, fields)
                # sys.exit(1)
            pf_regex = re.compile(r'\b' + pfield + r'\b', re.IGNORECASE)
            if pf_regex.search(code) is None:
                print "ERROR: %s not found in code \n\t'%s'" % (pfield, code)
                sys.exit(1)

            code = pf_regex.sub("pf_dict['" + pfield + "']", code)
            # print "DEBUGGING: 2012-08-27 - \n%s " % code

        derived_missing_ok = True
        if 'derived_missing_ok' in fspec:
            derived_missing_ok = fspec['derived_missing_ok']
        else:
            print "NOTE: derived_missing_ok flag not set - assumes OK to have missing source values"

        for rowkey, row in rows_dict.items():
            # CHANGED: 2012-12-13 - to handle fields which are not found
            # Set up dictionary with null values for each key
            pf_dict = {i:None for i in fspec['primaryfields']}
            # Now replace where a value is found
            for i in row:
                if i['field'] not in fspec['primaryfields']:
                    continue
                pf_dict[i['field']] = i['clean']
            df = [item for item in row if item['field'] == dfield][0]

            # Check fields have values before trying code
            missing_pf = False
            debug = False
            if derived_missing_ok == False:
                for v in pf_dict.items():
                    if v[1] is None:
                        missing_pf = True
                        if debug:
                            print "WARNING: missing primary value (%s) for field %s in row %s" % (
                        v[1], v[0], rowkey)
                    if isinstance(v, str):
                        if len(v) == 0:
                            missing_pf = True
                            if debug:
                                print "WARNING: missing primary value (%s) for field %s in row %s" % (
                            v[1], v[0], rowkey)

            if missing_pf:
                continue
            # all values OK then evaluate code
            # DEBUGGING: 2012-08-22 -
            # sys.exit(1)
            try:
                # if this works returns the var derived_var
                exec code
                fields_derived_ok = True
            except:
                print "WARNING: code to derive %s failed in row %s" % (
                    dfield, rowkey)
                debug = True
                if debug == True:
                    print code
                    print pf_dict
                    print "Debug =  True: Aborting"
                    import pdb; pdb.set_trace()
                    # sys.exit(1)
                else:
                    continue

            df['clean'] = derived_var

if fields_derived_ok:
    print "OK: At least one field derived!"


#  ==========================
#  = Run field level checks =
#  ==========================
# Switch from rows to cols for fields work
# First rebuild cols_dict since you have updated rows dict
# and then delete the old rows dict so you only have one master copy
cols_dict = rcdict_from_rows(rows_dict.values(), pkeys, req='cols')
del rows_dict

print "OK: Running field checks"

for field, col in cols_dict.items():
    col = list(col)
    fspec = fdict_lookup[field]
    checks = []
    # CHANGED: 2012-08-31 - add in a check for missing data in an essential field
    if 'checks' in fspec:
       checks.extend(fspec['checks'])
    # if field in fields_essential:
    #     essential_chk = {'type': 'essential', 'msg': 'Required field', 'response': None}
    #     checks.append(essential_chk)
    if checks:
        for chk in fspec['checks']:
            # print fspec
            col, success = run_fchk(col, chk)

        # cols_dict[field] = tuple(col)

# NOTE: 2012-08-14 - Uncomment to inspect field checks
# for k in cols_dict.keys():
#   for i in cols_dict[k]:
#       if i['checks']:
#           print i['field'], i['raw'], i['clean'], i['checks']
# sys.exit(1)

#  ===========================
#  = Run record level checks =
#  ===========================
print "OK: Running record checks"

# Switch back to rows dict
# First rebuild rows_dict since you have updated cols_dict
rows_dict = rcdict_from_rows(cols_dict.values(), pkeys, list_is = 'cols')
del cols_dict

# Now get row checks that are stored with the table spec
rchecks = []
try:
    rchecks = tspec['checks']
    assert len(rchecks) is not None
except:
    print "WARNING: row checks not found in %s" % tspec['tablename']
# emtpy list unless checks found so loop will be skipped
for chk in rchecks:
    # Confirm that these checks can be found in the check_dictionary
    try:
        cspec = [i for i in vdict if i['checkname'] == chk][0]
    except IndexError:
        print "ERROR: Check (%s) not found in check dictionary" % chk
        sys.exit(1)
    rows = rows_dict.values()

    rows, success = run_rcheck(rows, cspec)

# NOTE: 2012-08-14 - Uncomment to inspect field checks
# for k in rows_dict.keys():
#   for i in rows_dict[k]:
#       if i['checks']:
#           print i['field'], i['raw'], i['clean'], i['checks']

# Generate validated value based on row and field checks
print "OK: Generating validated values based on row / field checks"
for rowkey, row in rows_dict.items():
    for index, item in enumerate(row):
        item['validated'] = item['clean']

        # DEBUGGING: 2012-08-24 -
        # if 'cxr' in item['field']:
        #     print type(item['raw']), type(item['clean']), item['validated'],  item['checks']

        if not item['checks']:
            continue
        # if list of checks is not empty then
        for chk in item['checks']:

            if chk['result'] == 'impossible':
                item['list_imposschks'].append(chk['name'])
                if chk['level'] == 'row':
                    item['imposs_rchks'] += 1
                else:
                    item['imposs_fchks'] += 1

            else: # unusual result
                item['list_unusualchks'].append(chk['name'])
                if chk['level'] == 'row':
                    item['unusual_rchks'] += 1
                else:
                    item['unusual_fchks'] += 1

        if item['imposs_fchks'] > 0:
            item['validated'] = None

# CHANGED: 2012-08-13 - add a labelled field values
print "OK: Labelling fields"
cols_dict = rcdict_from_rows(rows_dict.values(), pkeys, req = 'cols')
del rows_dict

for field, col in cols_dict.items():
    fspec = fdict_lookup[field]

    if 'vallab' in fspec:
        # print fspec
        item_pairs = fspec['vallab'].items()
        # item_dict = dict([(kv[0].lower(), kv[1]) if type(kv[0]) == str
        #     else kv for kv in item_pairs])
        item_dict = dict([(str(kv[0]).lower(), kv[1]) for kv in item_pairs])

    for item in col:
        item['labelled'] = item['validated']
        if item['validated'] is None:
            continue
        elif 'vallab' not in fspec:
            continue
        elif str(item['validated']) not in item_dict:
            print type(item['validated'])
            print "WARNING: No label found for %s in field %s" % (
                item['validated'], item['field'])
            continue
        else:
            item['labelled'] = item_dict[str(item['validated'])]

#  ==========================================================
#  = Everything from here on should work with the rows dict =
#  ==========================================================
rows_dict = rcdict_from_rows(cols_dict.values(), pkeys, list_is = 'cols')
del cols_dict

#  =====================================
#  = Export the data to MySQL database =
#  =====================================
if sql_out != db_name:
    # SQL database connection
    cursor_out = sql_connect(sql_out)
else:
    cursor_out = cursor

metadata_fields = [
    '_valid_row', '_valid_allfields', '_count_missfields',
    '_count_unusualfields', '_count_impossfields',
    '_list_unusualchks', '_list_imposschks']

# Prepare insert statement field list
# fields = [i['field'] for i in rows_dict.values()[0]]
insert_fields = fields
# print insert_fields
insert_fields.extend(metadata_fields)

# Create SQL table
# TODO: 2012-08-18 - need a different create table statement if labelled fields requested
success = quick_mysql_tab(cursor_out, fdict, tab_name, insert_fields)
# success = quick_mysql_tab(cursor, fdict, tab_name, insert_fields, labelled = True)

for rowkey, row in rows_dict.items():

    insert_dict = row_to_insert_dict(
        row, insert_fields, result=mysql_output_lvl)
    fv = insert_dict.items()
    insert_fields, values = tuple(zip(*fv))
    stmt = sql_prepare_insert(tab_name, insert_fields, values)
    # print stmt
    cursor_out.execute(stmt)
    if debug:
        sys.exit(1)

# Finally add a primary key to the table
stmt = "ALTER TABLE %s ADD PRIMARY KEY (%s)" % (tab_name, ', '.join(pkeys))
cursor_out.execute(stmt)

# Run post-flight SQL script
if 'postflight' in tspec and tspec['postflight'] is not None:
    sql_multistmt(cursor, tspec['postflight'])

#  ====================
#  = Pickle the data  =
#  ====================
# pickle_it = True
pickle_path_file = "../data/pickled/" + tab_name
if pickle_it:
    with open(pickle_path_file, 'wb') as pickle_file:
        pickle.dump(rows_dict, pickle_file, protocol=-1)



print "\nOK: Script terminated"

