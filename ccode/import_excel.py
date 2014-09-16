#!/usr/local/bin/python

#  ## Generic import excel sheet
# ______________________________
# CreatedBy:    Steve Harris

# CreatedAt:    120726
# ModifiedAt:   120726

# Filename: import_excel.py
# Project:

# ## Description

# Generic import excel functionality

# ## Dependencies

# - mysql table 'excel_sheets' (to record import, check whether already imported)
# - mysql database into which to insert the data
# - does not generate new tables in MySQL: these must already exisit
# - file name must be key_YYMMDD_v1 format
#   i.e. UCL_120726_v1

# ____
import sys      # file input / output
# TODO: 2012-08-17 - push local myspot changes back to main myspot
# Use local version of myspot module
# You can switch back to using the main myspot module if you push local changes back

# sys.path.remove('/Users/steve/usr/local/lib')
# sys.path.append('/Users/steve/data/spot_id/local/lib_usr')

import myspot   # user written spot functions

import re       # regular expressions
import os       # file system operations
import yaml     # YAML parser for the dictionary_tables file
import string   # string manipulation

                # date and time modules
import datetime
import time

import MySQLdb  # MySQL database wrapper
import re       # regular expressions
import argparse # command line parsing

import pprint


                # read excel data
from xlrd import open_workbook, xldate_as_tuple, XL_CELL_DATE
global XL_DATEMODE

# Parse command line options
# ____________________________
parser = argparse.ArgumentParser(description =
    "Generic import excel file to MySQL with formatting corrections")
# Mandatory args (positional)
parser.add_argument("import_file", help=":path to excel file")
parser.add_argument("target_db", help=":target MySQL database name")

# if you don't specify the -all option then will need sheet / table options
parser.add_argument("-all", "--all_sheets",
    help=":import all workbook sheets",
    action="store_true",
    default=False)

# create new table structure as well
parser.add_argument("-new", "--new_table",
    help=":create new SQL table for this sheet",
    action="store_true",
    default=False)

parser.add_argument("-s", "--this_sheet", help=":name of excel sheet (case sensitive)")
parser.add_argument("-t", "--this_table", help=":name of MySQL table")

args = parser.parse_args()
parser.parse_args()

print """
#  ===============================
#  = OK: Running import_excel.py =
#  ===============================
"""

# Function definitions
# ____________________
def parse_filename(path2check,filename_regex):
    """
    Check proposed file name is OK
    Needs a string containing the proposed path
    Needs a regex to check the filename format
    Returns a tuple with path, filename, filedate
    """

    # check string is valid path
    if not os.path.isfile(path2check):
        print "Error: %s is not a file\n" % path2check
        sys.exit(1)

    filename = os.path.basename(path2check)
    if not filename_regex.match(filename):
        print "Error: Expected filename.xls (not xlsx), but got %s" % filename
        sys.exit(1)

    print """OK: Will import "%s" """ % filename

    file_modifiedat = datetime.datetime.fromtimestamp(
        os.path.getmtime(path2check)).isoformat(' ')

    input_path, input_filename = os.path.split(path2check)
    return input_path, input_filename, file_modifiedat



input_file = args.import_file
target_db = args.target_db
print "OK: Input file: %s\nOK: Target database: %s" % (input_file, target_db)


# define the regex to match the filename
# check filename and path valid
filename_regex = re.compile(r""".*?\w+?\.xls""")
input_path, input_filename, input_filetime = parse_filename(input_file,
    filename_regex)

# check can open excel workbook and get list of sheets
# define sheets to be imported
# if command line --all_sheets then full list
# if command line sheet specified then check exists
with open(input_file, 'r') as excel_file:
    try:
        xlbook = open_workbook(input_file)
        xlsheet_dict = {n.lower():i
            for i, n in enumerate(xlbook.sheet_names())}
        # print xlsheet_dict
        if args.all_sheets:
            xlsheets = xlbook.sheet_names()
        else:
            # print args.this_sheet
            if args.this_sheet.lower() in xlsheet_dict:
                xlsheets = []
                xlsheets.append(args.this_sheet.lower())
            else:
                print "ERROR: sheet '%s' not found in workbook" % (
                    args.this_sheet)
                # CHANGED: 2012-08-21 - above should now work w sheet index
                # hence case insensitive
                # print "ADVICE: sheet names are case sensitive"
                sys.exit(1)
        XL_DATEMODE = xlbook.datemode
    except:
        print "ERROR: open_workbook method failed on '%s'" % input_file
        sys.exit(1)

print "OK: Will import %r" % xlsheets

# ditto for tables
if args.this_table is None:
    print "OK: Will generate new table for each sheet"
else:
    print "OK: Will create table %s" % args.this_table

# check and connect to database provided using 'target_db'
#  __________________________________________________________
my_cursor = myspot.sql_connect(target_db)

# check you can open the dictionares OK
# ____________________________________________
tdict = myspot.get_yaml_dict('table', local_dict = True)
tdict_lookup = {t['tablename']:t for t in tdict}
fdict = myspot.get_yaml_dict('field', local_dict = True)
fdict_lookup = {f['fname']:f for f in fdict}
reverse_fdict = myspot.reverse_field_dict()


#  =============================================================
#  = Check sheet not already imported using excel_sheets table =
#  =============================================================
new_sheets = []
for sheet in xlsheets:
    # print xlsheets
    stmt = """  SELECT sourceFile FROM excel_sheets
                WHERE sourceFile = '%s' AND sheetName = '%s' AND import_ok = 1;
                """ % (input_filename, sheet)
    # print stmt
    my_cursor.execute(stmt)
    # print my_cursor.rowcount
    if my_cursor.rowcount > 0:
        print "WARNING: Skipping sheet %s - already imported" % sheet
        # CHANGED: 2012-08-22
        # - for some reason xlsheets.remove was terminating loop early
        # xlsheets.remove(sheet)
    else:
        print "OK: Sheet %s to be imported" % sheet
        new_sheets.append(sheet)


# Clean version of sheets
for sheet in new_sheets:
# Makes the code look ugly but avoids indentation
    print """

#  ======================================
#  = Importing sheet from file as below =
#  ======================================

OK: Processing %s from %s"
""" % (sheet, input_filename)

    # convert to lowercase since the dictionary aliases are in lowercase
    sheet = sheet.lower()

    stmt = """INSERT INTO excel_sheets
            (sourceFile, sheetName, sourceFileTimeStamp)
            VALUES ('%s', '%s', '%s');
            """ % (input_filename, sheet, input_filetime)
    try:
        my_cursor.execute(stmt)
    except:
        print "Error: Failed to insert record into excel_sheets record table"
        sys.exit(1)

    # derive the field list from the column headers in the excel sheet
    # CHANGED: 2012-08-21 - to work with sheet indices - hence case insensitive
    # sheet_obj = xlbook.sheet_by_name(sheet)
    sheet_obj = xlbook.sheet_by_index(xlsheet_dict[sheet])
    rows_in_sheet = sheet_obj.nrows - 1  # subtract 1 for the header row
    field_list = myspot.xl_get_col_headers(sheet_obj)
    # CHANGED: 2012-08-31 - now add row_numbers to field list
    field_list.insert(0, 'excel_sheet')
    field_list.insert(0, 'excel_row')
    # print field_list

    # Derive table name
    # use the table dictionary to get the correct name for the table
    if args.this_table is not None:
        tab_name = args.this_table
        # CHANGED: 2012-08-15 - standardised suffix to _import from _xl for all imports
        tab_name_sql = args.this_table + '_import'
        tspec = tdict_lookup[tab_name]
    else:
        # use the table dictionary to work out table from sheet name
        tspec = None
        for table, tspec in tdict_lookup.items():
            if 'talias' in tspec:
                # print tspec
                if sheet in tspec['talias']:
                    break
                else:
                    tspec = None

        # print tspec
        if tspec is None:
            print "WARNING: Cannot import %s - table spec not found" % sheet
            continue  # skip the remainder of the loop
        else:
            tab_name = tspec['tablename']
            tab_name_sql = tspec['tablename'] + '_import'
            print "OK: To import sheet (%s) to table (%s)" % (
                sheet, tab_name_sql)

    # create a new table if requested
    # TODO: 2012-07-31 - need to add raw table creation to this
    if args.new_table:
        myspot.cr_mysql_tab(target_db, tab_name, field_list,
            raw_flg = 1,
            import_flg = 1,
            replace_flg = 1)

    # use tab_name and 'no_alias' to construct a dictionary for this sheet
    # falias_dict = dict(reverse_fdict['no_alias'].items()
    #     + reverse_fdict[tab_name].items())
    f= reverse_fdict['no_alias'].items()
    if tab_name in reverse_fdict:
        f.extend(reverse_fdict[tab_name].items())
    falias_dict = dict(f)

    # CHANGED: 2012-08-31 - add a row index to each column first
    # headers should be row 1
    print sheet_obj.nrows
    row_id = [[sheet, i+2] for i in range(sheet_obj.nrows)]
    # now add a 'header' with a title to the beginning of your list
    row_id.insert(0,('excel_sheet', 'excel_row'))
    # print row_id
    # print row_id

    # get excel data as a dictionary of lists
    # Declare empty lists for columns and clean columns for this sheet
    cols_raw = zip(*row_id)
    cols_clean = zip(*row_id)

    # now loop through column by column (using the field_list)
    for field_orig in field_list:
        if field_orig == 'excel_row' or field_orig == 'excel_sheet':
            continue

        if field_orig in falias_dict:
            # Look-up original name
            fname = falias_dict[field_orig]
        elif field_orig in fdict_lookup:
            fname = field_orig
        else:
            print "ERROR: %s not found in dictionary for %s\n" % \
                (field_orig, tab_name)
            sys.exit(1)
        fspec = fdict_lookup[fname]
        # get field spec
        # now search for field spec

        if 'sqltype' in fspec:
            sqltype = fspec['sqltype']
        else:
            print "ERROR: sqltype key missing for %s" % fname
            sys.exit(1)
        if 'sqllen' in fspec:
            sqllen = fspec['sqllen']
        else:
            sqllen = 255
        # CHANGED: 2012-12-13 - extract valias so can convert
        valias_dict = None
        valias = None
        if 'valias' in fspec or 'valiases' in fspec:
            if 'valiases' in fspec:
                for v in fspec['valiases']:
                    if tab_name in v['valias_tables']:
                        valias = v['valias']
            else:
                valias = fspec['valias']
            if valias:
                valias_dict = dict((str(i[0]).lower(), i[1]) if isinstance(i[0], str)
                    else (i[0], i[1]) for i in valias.items())

        print "OK: Parse %s to %s as %s(%s)" % \
            (field_orig, fname, sqltype, sqllen)

        # returns a list of cells objects
        cells = myspot.xl_col2list(sheet_obj, field_orig)
            # Parse the cell object wrt to sqltype, sqllen

        # List of clean cells with field name as first entry
        cells_raw = [fname]
        cells_clean = [fname]

        # CHANGED: 2012-08-31 - moving the essential field checks
        # if 'essentialfields' in tspec:
        #     if fname in tspec['essentialfields']:
        #         essentialfields = True
        #     else:
        #         essentialfields = False
        # else:
        #     essentialfields = False
        #     "WARNING: essential fields not defined for %s" % tab_name

        # import pdb; pdb.set_trace()
        row_number = 2  # this is the excel row number where 1 is header row
        # Slice so skip header and start at second cell i.e. [1]
        for cell in cells[1:]:
            row_number += 1
            result = myspot.xl_2mysql(cell, sqltype, sqllen, valias_dict)
            validation_ok = result['validation_ok']
            empty_cell = result['validation_msg']

            # CHANGED: 2012-08-28 - where raw is empty string then None not empty string
            # subsequent handling in sql_prepare_insert should convert this to NULL
            if len(str(cell.value)) == 0:
                cell_raw_value = None
            else:
                cell_raw_value = MySQLdb.escape_string(str(cell.value))[:255]
            cells_raw.append(cell_raw_value)
            cells_clean.append(result['cell_value'])

            if validation_ok or cell_raw_value is None:
                post_2excel_dvr = False
            # elif empty_cell and not essentialfields:
            #     post_2excel_dvr = False
            else:
                post_2excel_dvr = True

            if post_2excel_dvr:
                stmt = ("""INSERT INTO excel_dvr
                        (sourceFile, sheet_name, sourceFileTimeStamp,
                        tab_name, row_number, field_name,
                        cell_value, validation_msg)
                        VALUES ('%s', '%s', '%s', '%s', %d, '%s', '%s', '%s')
                        """ % (input_filename, sheet, input_filetime,
                            tab_name_sql, row_number, fname,
                            cell_raw_value, result['validation_msg']))
                my_cursor.execute(stmt)

        # Now add this column of cells to your list of columns
        cols_raw.append(cells_raw)
        cols_clean.append(cells_clean)

    # TODO: 2012-07-31 -  Get rid of empty rows but this means you'll lose excel row as counter
    rows_raw = zip(*cols_raw)
    rows_clean = zip(*cols_clean)

    myspot.sql_append_headed_list(my_cursor, tab_name_sql + '_raw', rows_raw)
    # pprint.pprint(rows_clean)
    myspot.sql_append_headed_list(my_cursor, tab_name_sql, rows_clean)

    # now write sourceFile/timestamp data
    for suffix in ['', '_raw']:
        stmt = (""" UPDATE %s  SET sourceFile = '%s',
                sourceFileTimeStamp = '%s' WHERE sourceFile IS NULL """
                % (tab_name_sql + suffix, input_filename, input_filetime))
        # print stmt
        my_cursor.execute(stmt)

    # now write excel_sheet imported ok (ORDER BY LIMIT so only the last record)
    stmt = """UPDATE excel_sheets
                SET import_ok = 1,
                rows_in_sheet = %d
                WHERE sourceFile= '%s'
                    AND sheetName = '%s'
                    AND sourceFileTimeStamp = '%s'
                ORDER BY modifiedAt DESC LIMIT 1;
        """ % (rows_in_sheet, input_filename, sheet, input_filetime)
    # print stmt
    my_cursor.execute(stmt)

    # print "OK: Importing worksheet %s" % sheet
    # TODO: 2012-07-30 - update excel_sheets here with code 1 for imported OK

print "\nOK: Script terminated successfully\n"
