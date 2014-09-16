#!/usr/local/bin/python
# CHANGED: 2012-08-12 - following homebrew install
# retired this path for now #!/usr/bin/python

#  ## Import returned DVR and add to MySQL table
# ______________________________________________
# CreatedBy:    Steve Harris

# CreatedAt:    120706
# ModifiedAt:   120706

# Filename: import_dvr.py
# Project:  spotid

# ## Description

# Get Hazel to use this script to automatically import when file saved


# ## Dependencies

# -  List dependencies here

# ____
import sys      # file input / output
import re       # regular expressions
import os       # file system operations
                # date and time modules
import datetime
import MySQLdb  # MySQL database wrapper
import re       # regular expressions

                # read excel data
from xlrd import open_workbook, xldate_as_tuple, XL_CELL_DATE
global XL_DATEMODE

import mypy     # specifically for the sql_multistmt code

def check_file(path2check,filename_regex):
    """
    Check proposed file name is OK
    Needs a string containing the proposed path
    Needs a regex to check the filename format
    """

    # check string is valid path
    if not os.path.isfile(path2check):
        print "Error: %s is not a file\n" % path2check
        sys.exit(1)

    filename = os.path.basename(path2check)
    if not filename_regex.match(filename):
        print "Error: Expected SIT_YYMMDD.xls, but got %s" % filename
        sys.exit(1)


# NOTE: 2012-10-10 - this should be upgraded to use the conversion functions in myspot
def xl2mysql_string(cell):
    """
    Take excel cell object and return the value
    If that value is a date / time then encode correctly
    """
    if cell.ctype == XL_CELL_DATE:
        year, month, day, hour, minute, second = (
            xldate_as_tuple(cell.value, XL_DATEMODE))
        myvalue = datetime.datetime(year, month, day,
            hour, minute, second).isoformat(' ')
    else:
        myvalue = cell.value
    return (myvalue)


def xlcol2list(xlsheet, col_name):
    """
    Take an excel sheet and the column header name
    Find the column containing that data
    Return as a python list
    """
    # extract row 1 of the sheet
    xlheaders = xlsheet.row_values(0)
    # find the index of col_name
    try:
        col_index = xlheaders.index(col_name)
        # use column_values method to return the list
        # iterate through list and convert dates / times
        cells = xlsheet.col(col_index)
    except:
        print ("Error: Failed to extract col %s from sheet %s" %
            (col_name, xlsheet.name))
        sys.exit(1)

    return [xl2mysql_string(cell) for cell in cells]

def parse_untimed_fields(dirty_list):
    """
    Convert timed entries for untimed fields into the artificial time 00:01:01
    """
    pass

def sql_append_headed_list(sql_cursor, table_name, headed_list):
    """
    Given a table name, a mysql connection object AND a list of lists
    where the first item in the list is a list of column names
    and the remaining items are lists of data
    Then insert this list into the MySQL table
    """
    # first extract the field names and remove the header row
    field_names = [str(field_name) for field_name in headed_list[0]]
    headed_list = headed_list[1:]

    # now loop through the rows
    for row in headed_list:
        # extract the the field values as strings
        field_values = [str(field_value) for field_value in row]
        # prepare SQL statement
        statement = (" INSERT INTO %s (%s) VALUES ('%s') " %
                (table_name,
                ", ".join(field_names),
                "', '".join(field_values)))
        print statement
        sql_cursor.execute(statement)



# Body of  code starts here after all function definitions
# connect to mysql
# Connect to the MySQL database

try:
    my_conn = MySQLdb.connect(host='localhost', db='spot', user='stevetm')
    print "Connected"
    my_cursor = my_conn.cursor()
except MySQLdb.Error, e:
    print "Cannot connect to MySQL database"
    print "Error code:", e.args[0]
    print "Error message:", e.args[1]

    sys.exit (0)

# define the regex to match the filename
filename_regex = re.compile(r"""
            (UCL|HAR|KET|LIS|MED|NOR|POL|RVI|SOU|UCL|YEO|FRH)
            _(\d{6})
            (_v(\d))?
            \.xls""",
            re.VERBOSE)

filename = sys.argv[1]
check_file(filename, filename_regex)

#  use with methods to open file b/c then closed automatically if error
with open(filename,'r') as excel_file:

    (sitecode, dvr_date,jj, dvr_version) = (
        filename_regex.match(os.path.basename(filename)).groups())
    if dvr_version is None:
        dvr_version = 1
    print "Sitecode: %s \nDVR date: %s \nDVR version: %s\n" %  (
        sitecode, dvr_date, dvr_version)
    # now create a workbook object
    try:
        book = open_workbook(filename)
        XL_DATEMODE = book.datemode
    except:
        print "Error: open_workbook method failed on %s\n" % filename
        sys.exit(1)

# Check that the file has not already been imported
the_file = os.path.basename(filename)
statement = (
    "SELECT COUNT(*) FROM id_DVR_files WHERE fileName = '%s' " % the_file)
my_cursor.execute(statement)
if my_cursor.fetchone()[0] != 0:
    print "Error: %s already imported\n" % the_file
    sys.exit(1)
else:
    statement = (
        """INSERT INTO id_DVR_files
            (sitecode, filename, filedate, fileVersion)
            VALUES ('%s', '%s', '%s', '%s')""" %
                (sitecode,
                the_file,
                datetime.date(
                    2000+int((dvr_date[0:2])),
                    int(dvr_date[2:4]),
                    int(dvr_date[4:6])).isoformat(),
                dvr_version))
    print statement
    try:
        my_cursor.execute(statement)
    except:
        print "Error: Failed to insert record into ID_DVR_files"
        sys.exit(1)

# using a list of necessary columns now extract the data from each column
cols_patient = ('sitecode', 'id', 'field', 'value', 'validation_message',
    'new_response_code', 'new_response_value',  'new_response_note')

cols_timed = ('sitecode', 'id', 'dass', 'tass', 'field', 'value',
    'validation_message', 'new_response_code', 'new_response_value',
    'new_response_note')

# extract the relevant worksheets
try:
    sheet_patient = book.sheet_by_name("DVR_patient")
    sheet_timed = book.sheet_by_name("DVR_timed")
except:
    print "Error: failed to find DVR worksheets"
    sys.exit(1)

sheets = ( (sheet_patient, cols_patient, 'IDwideDVR_responses'),
            (sheet_timed, cols_timed, 'IDlongDVR_responses'), )

for sheet in sheets:
    this_sheet , these_cols , this_table = sheet

    col_list = []

    for col in these_cols:

        dirty_list = xlcol2list(this_sheet, col)
        print dirty_list[0]
        #  Converts untimed entries (currently coded with the stata '.') to
        # your arbitrary 00:01:01 time
        if dirty_list[0] == 'tass':
            # print dirty_list
            dirty_list = ['00:01:01' if item == '.'
                else item for item in dirty_list]
            # print dirty_list

        # rename columns to standard names
        if dirty_list[0] == 'tass':
            dirty_list[0] = 'v_time'
        if dirty_list[0] == 'dass':
            dirty_list[0] = 'v_date'
        if dirty_list[0] == 'id':
            dirty_list[0] = 'spotidno' 


        # clean up the column / remove empty dot marker
        # also handle quotes and other probs with MySQKdb.escape_string
        result_list = [MySQLdb.escape_string(re.sub(r'^\.$', '', field))
            if isinstance(field, basestring)
            else field
            for field in dirty_list]
        # method: a list of lists
        # important that all lists are of the same length
        # print result_list
        col_list.append(result_list)

    # method: use the zip function to produce a list of tuples
    # the asterisk before the list of lists unpacks the list
    # then zip re-packs them in the opposite order
    row_list = zip(*col_list)

 #  print row_list[0]
 #  print this_sheet.name
 #  if this_sheet.name == 'DVR_timed':
 #      row1names = list(row_list[0])
 #      print row1names
    #   field_index = row1names.index('field')
    #   tass_index = row1names.index('tass')
    #   field_tass = zip(col_list[field_index], col_list[tass_index])
    #   print field_index, tass_index, field_tass
    #   for myfield, mytass in field_tass:
    #       print myfield, mytass
    # else:
    #   continue

    # now get rid of empty rows
    clean_rows = []
    for row in row_list:
        if len(row[0]) > 0:
            clean_rows.append(row)

    # now append this data
    sql_append_headed_list( my_cursor, this_table, clean_rows )



# Now produce a clean version of the tables with the latest response ony
# REFERENCE: 2012-07-10 - http://stackoverflow.com/questions/10593876/execute-sql-file-in-python-with-mysqldb
# MYSQL_OPTION_MULTI_STATEMENTS_ON = 0
# MYSQL_OPTION_MULTI_STATEMENTS_OFF = 1

# my_conn.set_server_option(MYSQL_OPTION_MULTI_STATEMENTS_ON)
# CHANGED: 2012-08-12 - swapping from above to mypy.sql_multistm

multi_statement = """
-- Steve Harris
-- 2012-07-10
-- run this after each import - just need to prep the sql code
-- will be appended to import_dvr.py

-- delete empty responses
DELETE FROM idlongDVR_responses WHERE new_response_code = '';
DELETE FROM idwideDVR_responses WHERE new_response_code = '';

-- create a current responses table for idlongDVR
DROP TABLE IF EXISTS idlongDVR_respCurr;
CREATE  TABLE idlongDVR_respCurr
    SELECT *, MAX(modifiedat) AS latest  FROM idlongDVR_responses
    GROUP BY sitecode, spotidno, v_date, v_time, field, value, validation_message
    ORDER BY sitecode, spotidno, v_date, v_time;

ALTER TABLE idlongDVR_respCurr DROP COLUMN latest;
ALTER TABLE idlongDVR_respCurr ADD KEY (sitecode, spotidno, v_date, v_time, field, validation_message);
ALTER TABLE idlongDVR_respCurr ADD COLUMN response_outcome CHAR(128);
ALTER TABLE idlongDVR_respCurr MODIFY COLUMN new_response_code TINYINT;

-- create a current responses table for idwideDVR
DROP TABLE IF EXISTS idwideDVR_respCurr;
CREATE  TABLE idwideDVR_respCurr
    SELECT *, MAX(modifiedat) AS latest  FROM idwideDVR_responses
    GROUP BY sitecode, spotidno, field, value, validation_message
    ORDER BY sitecode, spotidno;

ALTER TABLE idwideDVR_respCurr DROP COLUMN latest;
ALTER TABLE idwideDVR_respCurr ADD KEY (sitecode, id, field, validation_message);
ALTER TABLE idwideDVR_respCurr ADD COLUMN response_outcome CHAR(128);
ALTER TABLE idwideDVR_respCurr MODIFY COLUMN new_response_code TINYINT;
"""

mypy.sql_multistmt(my_cursor, multi_statement)

# try:
#   # my_cursor.execute(multi_statement)
# except:
#   print "Error: Failed to excute latest_only DVR SQL statement"
#   sys.exit(1)

# my_conn.set_server_option(MYSQL_OPTION_MULTI_STATEMENTS_OFF)
# Now close the connection
my_conn.close()
print "Disconnected"
# return a (no erorr) success code if all went well
print (0)
