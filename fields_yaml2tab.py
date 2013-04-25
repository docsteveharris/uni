#!/usr/local/bin/python
# CHANGED: 2012-08-12 - following homebrew install
# retired this path for now #!/usr/bin/python

# Take a list of field names in a yaml file
# and return a tab-delimited file containing field characteristics
# Adapted from label_stata_fr_yaml
# Takes 2 command line args
    # yaml formatted file of fields
    # yaml field dictionary
# ## Dependencies

# - YAML dictionary
# - YAML list of variables

# TODO: 2012-08-17 - push local myspot changes back to main myspot
# Use local version of myspot module
# You can switch back to using the main myspot module if you push local changes back


import yaml     # parse YAML
import re       # regular expressions
import sys      # file input / output
import os       # for the path module
import argparse
import copy     # deep copy of lists


# CHANGED: 2013-02-08 - for some reason the remove isn't working
# Doesn't seem to see the old path ... but inconsistent
# CHANGED: 2013-02-08 - changed back?!?
print sys.path

# sys.path.remove('/Users/steve/usr/local/lib')
# lib_usr_path = '/Users/steve/data/spot_early/local/lib_usr'
# sys.path.append(lib_usr_path)

if not '/Users/steve/usr/local/lib' in sys.path:
    sys.path.append('/Users/steve/usr/local/lib')

from myspot import get_yaml_dict
from myspot import check_file

parser = argparse.ArgumentParser(description =
    "Produce a tab-delimited file of field characteristics")
# Mandatory args (positional)
parser.add_argument("var_file", help=":YAML source file of fields")

parser.add_argument("-fmt", "--format",
    help=":output format for characteristics (default 'latex')",
    default='latex')

args = parser.parse_args()
parser.parse_args()

# define the regex to match filenames with yml extension
filename_target = re.compile(r"""\w+?\.yml""")
check_file(args.var_file, filename_target)
var_file = args.var_file
attribute_format = args.format

my_path = os.path.split(var_file)[0]

var_file_object = open(var_file, 'r')
my_vars = yaml.load(var_file_object.read())
# Strip out duplicates
my_vars = list(set(my_vars))

fdict = get_yaml_dict('field', return_type='dictionary', local_dict=True)

noformat_list = ['fname', 'stataformat']
format_list = ['tablerowlabel', 'unitlabel']
vallab_list = ['var_level', 'var_level_lab']
attribute_list = []
attribute_list.extend(noformat_list)
attribute_list.extend(format_list)
output_list = []
output = []
# CHANGED: 2013-01-25 - stata does not handle the quotes well
# output_list.append("""'""" + """'\t'""".join(attribute_list) + """'\n""")
column_headers = copy.deepcopy(attribute_list)
column_headers.extend(vallab_list)
output_list.append("""\t""".join(column_headers) + """\n""")
print output_list

for my_var in my_vars:
    print my_var
    new_row = []
    if my_var not in fdict:
        print "%s not found in field dictionary" % my_var
        continue
    for attribute in attribute_list:
        # check first for correctly formatted version
        if attribute in fdict[my_var] and attribute in format_list:
            if attribute_format == 'latex':
                # import pdb; pdb.set_trace()
                new_row.append(fdict[my_var][attribute]['latex'])
            else:
                new_row.append(fdict[my_var][attribute])
        elif attribute == "tablerowlabel":
            # NOTE: 2013-01-25 - use varlab if no tablerowlabel
            new_row.append(fdict[my_var]['varlab'])
        elif attribute == 'stataformat' and 'stataformat' not in fdict[my_var]:
            # define default formats based on sqltype if not otherwise specified
            if fdict[my_var]['sqltype'] in ['tinyint', 'smallint', 'int']:
                new_row.append("%9.0gc")
            elif fdict[my_var]['sqltype'] in ['float', 'decimal']:
                new_row.append("%9.2fc")
            else:
                new_row.append("")
        elif attribute in fdict[my_var]:
            new_row.append(fdict[my_var][attribute])
        elif 'vallab' not in fdict[my_var]:
            new_row.append("")
            new_row.append("")
        else:
            new_row.append("")

    # CHANGED: 2013-02-05 - now pulls a list of value labels
    # then runs through that list and makes a new row for each value label

    # first check that vallab exists in the field definition
    if 'vallab' not in fdict[my_var]:
        # append two empty columns for vallab key and value
        new_row.append("")
        new_row.append("")
        output_list.append("""\t""".join(new_row) + """\n""")
        continue
    vallab = fdict[my_var]['vallab']

    # then double check that it is numerically labelled
    vallab_is_not_int = 0
    for k in vallab.keys():
        if type(k) is not int:
            vallab_is_not_int = 1
    if vallab_is_not_int:
        # append two empty columns for vallab key and value
        new_row.append("")
        new_row.append("")
        output_list.append("""\t""".join(new_row) + """\n""")
        continue
    # now you should be safe to loop through all vallab keys
    for var_level, var_level_lab in vallab.items():
        # Append repeatedly
        # ... this means you will have multiple versions of this var
        # deep copy else you get a reference to the original list
        new_row_with_labels = copy.deepcopy(new_row)
        new_row_with_labels.append(str(var_level))
        new_row_with_labels.append(str(var_level_lab))
        output_list.append("""\t""".join(new_row_with_labels) + """\n""")

print output_list

output_file = my_path + "/_field_characteristics.txt"
output_object = open(output_file, 'w')
output_object.writelines(output_list)
print "Finished OK!"

