#!/usr/local/bin/python

# NOTE: 2012-09-02 - using the local environment install of python
# NOTE: 2012-09-02 - depends on idlong and idwide pickled files

def open_pickles():
    pickles = ['idwide', 'idlong']
    pickle_path = '../data/pickled/'
    pickle_rows = {}
    for p in pickles:
        print "NOTE: Opening pickled version of %s - this might be slow" % p
        with open(pickle_path + p, 'rb') as pickle_file:
            rows_dict = pickle.load(pickle_file)
            pickle_rows[p] = rows_dict
    return pickle_rows

def xl_dvr_from_(rows_dict, new_xl_file):
    # CHANGED: 2012-08-17 - untested as a function call
    # NOTE: 2012-08-14 - export dvr version to excel
    # Try and write this as a self-contained code block that just takes rows_dict
    # Then can factor out into a function

    success = False

    # build a dictionary of check names and messages
    chk_msg_dict = {}
    for chk in vdict:
        chk_msg_dict[chk['checkname']] = chk['msg']
    # Add field messages to dictionary
    fchk_types = ['legal_vals', 'min', 'max', 'regex', 'essential', 'raw']
    fchk_msgs = ['Unrecognised response code', 'Below minimum range',
        'Above maximum range', 'Unrecognised format for field',
        'Required field', 'Other']
    fchk_msg_dict =  zip(fchk_types, fchk_msgs)
    for kkey, vval in fchk_msg_dict:
        chk_msg_dict[kkey] = vval

    response_code_dict = {  1:'data missing / unavailable',
                            2:'please delete existing value',
                            3:'updated response offered',
                            4:'confirm existing value',
                            5:'other'}
    dvr_headers = list(pkeys)
    dvr_headers.extend(['field', 'field_label',
            'raw_value', 'labelled_value', 'validation_message', 'last_response'])
    xl_rows = []

    for row in rows_dict.values():
        for field in row:
            if not field['checks']:
                continue
            pkey_vals, dvr_row, dvr_string = None, None, None
            # print "%r" % (field['rowkey'],)
            # first clean primary key field
            field_name = field['field']
            field_label =fdict_lookup[field_name]['varlab']
            raw_val = field['raw']
            labelled_val = field['labelled']
            # CHANGED: 2012-08-14 - remake list for each check
            for chk in field['checks']:
                dvr_row = ['UNTIMED' if isinstance(i,datetime.timedelta)
                                and i == datetime.timedelta(0,61)
                                else i
                                for i in list(field['rowkey'])]

                chk_name = chk['name']
                # if 'legal_vals' in chk_name:
                #     chk_name = 'legal_vals'

                validation_message = chk_msg_dict[chk_name]
                if 'dvr_response' in field:
                    last_response_code = field['dvr_response']['new_response_code']
                    last_response = response_code_dict[last_response_code]
                    if last_response_code == 5:
                        last_response = field['dvr_response']['new_response_note']
                else:
                    last_response, last_response_code = None, None

                dvr_row.extend(
                    [field_name, field_label, raw_val, labelled_val,
                        validation_message, last_response ])


                dvr_string = [str(i) for i in dvr_row]
                xl_rows.append(dvr_string)
                # print ', '.join(dvr_string)

            # then raw, labelled val
    # print len(xl_rows)


    wbook = Workbook()
    sheet1 = wbook.add_sheet(tab_name)

    for i, h in enumerate(dvr_headers):
        sheet1.write(0,i,h)

    for r, row in enumerate(xl_rows):
        for c, val in enumerate(row):
            sheet1.write(r + 1,c,val)

    wbook.save('/users/steve/desktop/jj_dvr.xls')
    success = True
    return success

#  =========================
#  = Import python modules =
#  =========================
import cPickle as pickle
import argparse
import sys
import datetime
from datetime import date
import copy
from xlwt import Workbook, easyxf

# TODO: 2012-08-17 - push local myspot changes back to main myspot
# Use local version of myspot module
# You can switch back to using the main myspot module if you push local changes back

sys.path.remove('/Users/steve/usr/local/lib')
sys.path.append('/Users/steve/data/spot_id/local/lib_usr')

from myspot import get_yaml_dict
from myspot import make_talias_dict
from myspot import extract_fields_from
from myspot import sql_connect
from myspot import sql_get_cols

#  =================
#  = Main function =
#  =================

debug = False
print """
#  ========================================================
#  = "WARNING: MAKE SURE THE PICKLED FILES ARE UP-TO-DATE =
#  ========================================================
"""

#  ===================================
#  = Handle command line argumenents =
#  ===================================
if debug == True:
    # set command line args here for use while debugging
    sitecode = "pol"
else:
    parser = argparse.ArgumentParser(description=
"""
Make excel dvr report for a specific site
- assumes pickled contemporary versions of idwide, idlong in ../data/pickled)
- saves to ../sitecomms/outgoing unless otherwise specified
""")

    parser.add_argument("sitecode", help=":3 letter sitecode for which you require dvr")
    args = parser.parse_args()
    parser.parse_args()
    sitecode = args.sitecode.lower()
    sitecodes = []
    if sitecode == 'all':
        sitecodes.extend(['ucl', 'ket', 'sou', 'med', 'pol', 'yeo', 'nor', 'lis', 'har', 'rvi', 'fre'])
    else:
        sitecodes.append(sitecode)

# Pull fdict lookup now because need to look-up hosp name

fdict_lookup = get_yaml_dict('field', return_type='dictionary', local_dict=True)
# open the pickled data - this will take a long time!
pickle_rows = open_pickles()

#  =======================
#  = Pull table spec etc =
#  =======================
tdict_lookup = get_yaml_dict('table', return_type='dictionary', local_dict=True)
talias_dict = make_talias_dict(get_yaml_dict('field', local_dict=True))
source_tables = {
    'idpid': '(SPOT)id web portal',
    'idpatient': 'Initial assessment',
    'idvisit': 'Daily assessment',
    'idlab': 'Laboratory flow chart',
    'idlabuclh': 'Biological sampling (UCLH only)'
}

#  ===================================
#  = Define checks and DVR variables =
#  ===================================
# Check dictionary
vdict = get_yaml_dict('checks', local_dict=True)

# build a dictionary of check names and messages
chk_msg_dict = {}
for chk in vdict:
    chk_msg_dict[chk['checkname']] = chk['msg']
# Add field messages to dictionary
fchk_types = ['legal_vals', 'min', 'max', 'regex', 'essential', 'raw']
fchk_msgs = ['Unrecognised response code', 'Below minimum range',
    'Above maximum range', 'Unrecognised format for field',
    'Required field', 'Other']
fchk_msg_dict =  zip(fchk_types, fchk_msgs)
for kkey, vval in fchk_msg_dict:
    chk_msg_dict[kkey] = vval

response_code_dict = {  1:'data missing / unavailable',
                        2:'please delete existing value',
                        3:'updated response offered',
                        4:'confirm existing value',
                        5:'other'}

# NOTE: 2012-09-02 - define easxy exel styles now
# not on the fly else each loop counts as a new definition
# and there is a limit of 4024 defs
style_default = easyxf('font: colour black;')
style_header = easyxf('font: colour black, bold True; borders: bottom medium; pattern: back_colour white;')
style_chk_warning = easyxf('font: colour red, bold True;')
style_missing = easyxf('font: colour gray40;')
style_response_cell = easyxf('pattern: back_colour light_yellow;')


#  ==============
#  = Start loop =
#  ==============
for sitecode in sitecodes:
    try:
        sitename = fdict_lookup['sitecode']['vallab'][sitecode]
    except KeyError:
        print "ERROR: '%s' is not a recognised sitecode" % sitecode

    print "\nOK: Running make_dvr.py for %s" % sitename.upper()
    today = datetime.date.today().strftime('%y%m%d')
    dvr_filename = today + '_DVR_' + sitecode.upper() + '.xls'



    #  =====================================================
    #  = Create excel file for DVR, add a field dictionary =
    #  =====================================================
    dvr_book = Workbook()
    sh_dict = dvr_book.add_sheet('Field dictionary')
    sh_dict.write(0, 0, 'Short field name', style_header)
    sh_dict.write(0, 1, 'Field description', style_header)
    sh_dict.write(0, 2, 'CRF short name', style_header)
    sh_dict.write(0, 3, 'Case report form (CRF)', style_header)
    tab_offset = 1
    for tab, tab_label in source_tables.items():

        falias_dict = talias_dict['no_alias']
        if tab in talias_dict:
            falias_dict.update(talias_dict[tab])

        # print falias_dict
        source_fields = tdict_lookup[tab]['sourcefields']
        # print tab, fields
        for i, f in enumerate(sorted(source_fields)):
            fname = falias_dict[f]
            # print tab, tab_label, tab_offset, i, f, fname
            sh_dict.write(i + tab_offset, 0, fname)
            sh_dict.write(i + tab_offset, 1, fdict_lookup[fname]['varlab'])
            sh_dict.write(i + tab_offset, 2, tab)
            sh_dict.write(i + tab_offset, 3, tab_label)
            next_offset = i + tab_offset

        tab_offset = next_offset + 1

    dvr_book.save('../sitecomms/outgoing/' + dvr_filename)

    #  ========================
    #  = Add key errors sheet =
    #  ========================
    kerror_headings_original = ['validation_msg', 'key_fields', 'key_values', 'missing_fields']
    columns_to_skip = ['modifiedat', 'sourceFileTimeStamp']
    columns_to_redact = ['dob', 'namef', 'namel', 'idnhs', 'idpcode']
    cursor = sql_connect('spotid')
    for tab, tab_name in source_tables.items():
        kerror_fields = sql_get_cols('spotid', 'keys_dvr')
        stmt = """SELECT %s FROM keys_dvr
                    WHERE locate('%s', key_values)
                    AND sql_table = '%s' """ % (
                ', '.join(kerror_fields), sitecode, tab + '_import')
        cursor.execute(stmt)
        kerror_rows = cursor.fetchall()
        # skip on if no key errors
        if not kerror_rows:
            continue

        # create dictionary of kerror vals
        krows = []
        for kerror_row in kerror_rows:
            kdict = dict(zip(kerror_fields, kerror_row))
            krows.append(kdict)

        # add new sheet for key errors
        sh_kerror = dvr_book.add_sheet('Key error - %s' % tab)
        r = 0  # counter for rows in this sheet

        # get raw data and field names for raw data
        # TODO: 2012-09-07 - previously used the _raw table but now using _import
        # should switch back but this would mean adding tab_serial code into import_excel etc
        # does not seem worth it at the moment
        # raw_tab = krows[0]['sql_table'] + '_raw'
        raw_tab = krows[0]['sql_table']
        raw_fields = sql_get_cols('spotid', raw_tab)
        stmt = "SELECT * FROM %s" % raw_tab
        cursor.execute(stmt)
        # print cursor.rowcount
        raw_rows = cursor.fetchall()

        # add fields as column headings to sheet
        # [:] slice operator makes a shallow copy
        kerror_headings = kerror_headings_original[:]
        kerror_headings.append('key_values_CORRECTED')
        kerror_headings.extend(raw_fields)
        kerror_headings = [i for i in kerror_headings
            if i not in columns_to_skip]
        column_widths = []
        for c, f in enumerate(kerror_headings):
            sh_kerror.write(0, c, f, style_header)
            column_widths.append(len(f))

        for raw_row in raw_rows:
            rdict = dict(zip(raw_fields, raw_row))
            # Search for raw rows that match based on sourcefile, excel sheet etc
            for kdict in krows:
                # check to see if this row appears in kdict
                # Search for raw rows that match based on sourcefile, excel sheet etc
                if (
                    rdict['tab_serial'] == (kdict['tab_serial'])
                    ):
                    kdata = [i for i in kdict.items() if i[0] in kerror_headings_original]
                    kdata.extend(rdict.items())
                    kdata = [i for i in kdata if i[0] not in columns_to_skip]
                    kdata_sorted = sorted(kdata, key=lambda k: kerror_headings.index(k[0]))
                    kdata = [i[1] if i[0] not in columns_to_redact
                        else '--Web portal only--' for i in kdata_sorted]
                    # kdata = [i[1] for i in kdata_sorted]
                    kdata.insert(len(kerror_headings_original), '')
                    # found a row so incr row counter and write data
                    r += 1
                    for c, f in enumerate(kdata):
                        sh_kerror.write(r, c, str(f))
                        # update column widths where necessary
                        if len(str(f)) > column_widths[c]:
                            column_widths[c] = len(str(f))
                else:
                    continue

        # format data sheet with column widths up to max 32 chars
        for c, cwidth in enumerate(column_widths):
            if cwidth > 32:
                cwidth = 32
            sh_kerror.col(c).width = 256 * cwidth


    dvr_book.save('../sitecomms/outgoing/' + dvr_filename)


    if debug:
        sys.exit(1)
    #  ===============================================
    #  = Unpickle and write data and checks to excel =
    #  ===============================================
    dvr_sh_dict = {
        'idwide': 'patient',
        'idlong': 'timed'
    }





    pickles = ['idwide', 'idlong']
    # Loop through pickled data files
    for p in pickles:
        # Use the sql select statement from tspec to define the sort order
        tspec = tdict_lookup[p]
        sql_fields = extract_fields_from(tspec['sql_select'])

        # open the pickled data
        # CHANGED: 2012-09-18 - now works with a copy thereof 
        # (else would delete original during first loop)
        rows_dict = copy.deepcopy(pickle_rows[p])
        # loop through rows in rows_dict and delete row if not this sitecode
        for k, v in rows_dict.items():
            print sitecode, k
            if sitecode not in k:
                del(rows_dict[k])
        # check data exists
        if not len(rows_dict):
            print "ERROR: No data in %s for %s" % (p, sitename)
            sys.exit(1)

        # set up sheet names
        sh_data_name = 'DATA_' + dvr_sh_dict[p]
        sh_dvr_name = 'DVR_' + dvr_sh_dict[p]
        sh_chk_row = 0

        # add sheets
        sh_data = dvr_book.add_sheet(sh_data_name)
        sh_dvr = dvr_book.add_sheet(sh_dvr_name)
        # set up dvr column headers starting with primary key list
        pkeys = tspec['pkey']
        dvr_headers = list(pkeys)
        dvr_headers.extend(['field', 'field_label',
                'raw_value', 'labelled_value', 'validation_message', 'last_response'])
        for c, dvr_header in enumerate(dvr_headers):
            sh_dvr.write(sh_chk_row, c, dvr_header)
        # for c, h in enumerate(headings):
        #     sh_data.write(0, c, h)

        # Now sort your rows by their primary keys
        rows = rows_dict.values()
        fields = [i['field'] for i in rows[0]]
        for pkey in reversed(pkeys):
            sort_rows = []
            for row in rows:
                # make a field:value dictionary
                row_dict = dict(zip(fields, row))
                # use the dictionary to extract the primary key value
                pkey_val = row_dict[pkey]
                # create a tuple of this pkey_val and the row
                sort_rows.append((pkey_val, row))
            # print pkey
            # print fields
            # print [i[0]['raw'] for i in sort_rows]
            # use the first part of the tuple (the key) for a sort
            sort_rows.sort(key=lambda k: k[0])
            # relabel the sorted rows as 'rows' so loop can start again with next pkey val
            rows = [i[1] for i in sort_rows]

        # now loop through the rows of data and add to data, dvr sheets
        for r, row in enumerate(rows):
            row_raw, row_chks, row_items = {}, {}, {}
            # make row key start of list of items for dvr row
            row_key_dict = {item['field']: item['validated'] for item in row if item['field'] in pkeys}
            # make sure row key is sorted in same order as primary key list in tspec
            row_key = sorted(row_key_dict.items(), key=lambda k: pkeys.index(k[0]))
            # extract the second value (the key was used for the sort)
            row_key = [i[1] for i in row_key]
            row_key = ['UNTIMED' if isinstance(i, datetime.timedelta)
                            and i == datetime.timedelta(0, 61)
                            else str(i)
                            for i in row_key]
            # loop through fields
            for item in row:
                field_name = item['field']
                # set up a dictionary of items in the row keyed by field name
                # use this later when you come to write the data sheet
                row_items[field_name] = item
                field_label = fdict_lookup[field_name]['varlab']
                raw_val = item['raw']
                labelled_val = item['labelled']

                # set up row[raw] values (used later for entering data into data sheet)
                # remove PID and handle the untimed field
                if field_name in [
                    'dob', 'idnhs', 'icnno', 'idpcode', 'namef', 'namel']:
                    row_raw[field_name] = "--Web portal only--"
                elif (field_name == 'v_time' and
                    item['validated'] == datetime.timedelta(0, 61)):
                    row_raw[field_name] = 'UNTIMED'
                else:
                    row_raw[field_name] = labelled_val

                # extract checks and and row key for DVR table
                row_chks[field_name] = item['checks']
                if not item['checks']:
                    continue

                for chk in item['checks']:
                    sh_chk_row += 1
                    chk_name = chk['name']
                    validation_message = chk_msg_dict[chk_name]

                    # if the field has a dvr response then you'll need this
                    # print item.keys()
                    if 'dvr_response_code' in item:
                        last_response_code = item['dvr_response_code']
                        last_response = response_code_dict[last_response_code]
                        if last_response_code == 5:
                            last_response = item['dvr_response_note']
                        elif last_response_code == 3:
                            last_response = "%s (%s)" % (last_response,
                                item['dvr_response_value'])
                    else:
                        last_response, last_response_code = None, None

                    dvr_items = copy.deepcopy(row_key)
                    dvr_items.extend(
                        [field_name, field_label, raw_val,
                            labelled_val, validation_message, last_response])
                    # write the dvr_items to the dvr sheet
                    for c, val in enumerate(dvr_items):
                        sh_dvr.write(sh_chk_row, c, str(val), style_default)

            # now take your row dictionary and sort as per sql_select
            # returns a list of tuples
            raw_sorted = sorted(row_raw.items(), key=lambda k: sql_fields.index(k[0]))

            # if first row, then set up the headers for data and dvr
            # moving left to right across columns
            if r == 0:
                column_data_types = []
                column_widths = []
                for c, (kv_pair) in enumerate(raw_sorted):
                    sh_data.write(r, c, kv_pair[0], style_header)
                    # also monitor data types and column widths
                    column_data_types.append(type(kv_pair[1]))
                    column_widths.append(len(kv_pair[0]))

            for c, (fname, fvalue) in enumerate(raw_sorted):
                chks = row_chks[fname]
                item = row_items[fname]
                fvalue = str(fvalue)
                if len(fvalue) > column_widths[c]:
                    column_widths[c] = len(fvalue)
                if len(chks):
                    # write the data but the error formatting
                    sh_data.write(r + 1, c, fvalue, style_chk_warning)
                elif fvalue == 'None':
                    sh_data.write(r + 1, c, fvalue, style_missing)
                else:
                    # write the data with standard formatting
                    sh_data.write(r + 1, c, fvalue, style_default)
                # keep track of column data type and width

            # NOTE: 2012-09-03 - split seems OK but freeze panes does not work
            # Freeze panes
            # sh_data.panes_frozen = True
            # sh_data.remove_splits = True
            # sh_data.vert_split_pos = 2
            # sh_data.horz_split_pos = 1

        # add new_response column headings
        # print column_data_types
        # print column_widths
        # format dvr sheet with column widths up to max 32 chars
        for c, cwidth in enumerate(column_widths):
            if cwidth > 64:
                cwidth = 64
            sh_data.col(c).width = 256*cwidth
        for i in range(3):
            sh_data.col(len(dvr_headers) + i).set_style(style_response_cell)

        sh_dvr.write(0, len(dvr_headers) + 0, 'new_response_code', style_header)
        sh_dvr.write(0, len(dvr_headers) + 1, 'new_response_value', style_header)
        sh_dvr.write(0, len(dvr_headers) + 2, 'new_response_note', style_header)


        dvr_book.save('../sitecomms/outgoing/' + dvr_filename)


