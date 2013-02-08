#!/usr/local/bin/python
# CHANGED: 2012-08-12 - following homebrew install
# retired this path for now #!/usr/bin/python

#  ## Label a stata data set from a YAML dictionary
# _________________________________________________
# CreatedBy:    Steve Harris

# CreatedAt:    120725
# ModifiedAt:   120725

# Filename: label_stata_fr_yaml.py
# Project:  phd

# ## Description

# Takes 2 command line arguements
# - name of the yaml file containing the variables to be labelled
# - name of the yaml file containing the dictionary
# Produces a stata do file called _label_data.do
# It is your responsibility to make sure you delete this file after use
# - else you risk this being run against another file given the non-unique name


# ## Dependencies

# - YAML dictionary
# - YAML list of variables

# ____

import yaml     # parse YAML
import re       # regular expressions
import sys      # file input / output
import string   # string functions
import os       # for the path module

# define the regex to match filenames with yml extension
filename_target = re.compile(r""".*/\w+?\.yml""")
print sys.argv
if  __name__ == '__main__':
    if (len(sys.argv) != 3
        or not filename_target.match(sys.argv[1])
        or not filename_target.match(sys.argv[2])):
        print """
        ERROR
        -----
        Usage: label_stata_fr_yaml "stata_vars.yml" "var_dictionary.yml"
        Produces a _label_data.do which will label your stata data
        """
        # Debugging
        # var_file = "/users/steve/data/phd/data/scratch/vars.yml"
        # dict_file = "/users/steve/data/phd/data/dictionary_fields.yml"
        quit()
    else:
        var_file= sys.argv[1]
        dict_file= sys.argv[2]

my_path = os.path.split(var_file)[0]

var_file_object = open(var_file, 'r')
dict_file_object = open(dict_file, 'r')

my_vars = yaml.load(var_file_object.read())
my_dict = yaml.load(dict_file_object.read())

dofile_initiate = """
label drop _all
label define truefalse 0 "False" 1 "True"
"""

dofile_lines = [dofile_initiate]

for my_var in my_vars:
    # NOTE: 2012-07-25 - python generator idea
    # see http://stackoverflow.com/questions/1756096/understanding-generators-in-python
    # kept failing because of iterator call: now working as comprehension

    fspecs = [item for item in my_dict if item["fname"] == my_var ]
    if len(fspecs) > 0:

        # first for variable labels
        fspec = fspecs[0]
        # print fspec
        if 'varlab' not in fspec:
            print "ERROR: variable label (varlab) not found for %s" % my_var
            # CHANGED: 2012-08-29 - no longer an error not to have varlab
            # sys.exit(1)
        else:
            lline = """label variable %s "%s"\n""" % (my_var, fspec["varlab"])
            dofile_lines.append(lline)

        # now for value labels
        if fspec.has_key('vallab'):

            value_labels = fspec['vallab']
            # need to see if this is an integer label or string
            v_keys = value_labels.keys()
            string_encoded = False
            for k in v_keys:
                if isinstance(k,str):
                    string_encoded = True

            # handle string encoded variables here
            if string_encoded:
                dofile_lines.append("""* Label and encode variable\n""")
                # iterate over items in dictionary - returns a key-value tuple
                for label in value_labels.iteritems():

                    lline = ("""replace %s = "%s" if %s == "%s"\n"""
                        % (my_var, label[1], my_var, str(label[0]).lower()))
                    dofile_lines.append(lline)
                # now encode the data
                lline = ("""encode %s, gen(%s_lbl) label(%s)\ndrop %s\nrename %s_lbl %s\n"""
                        % (my_var,my_var,my_var, my_var,my_var,my_var))
                dofile_lines.append(lline)

            # handle numerically coded labels here
            else:
                dofile_lines.append("""* Define and apply label\n""")
                # TODO: 2012-07-25 - need to make sure you don't duplicate label defs
                label_defn = ""
                for kv in value_labels.items():
                    new_label = """ %d "%s" """ % kv
                    label_defn = label_defn + new_label
                dofile_lines.append("label dir\n")
                lline = "label define %s %s\n" % (my_var, label_defn)
                dofile_lines.append(lline)
                lline = "label values %s %s\n" % (my_var, my_var)
                dofile_lines.append(lline)


dofile_file = my_path + "/_label_data.do"
dofile_object = open(dofile_file, 'w')
dofile_object.writelines(dofile_lines)
print dofile_lines
print "Finished OK!"

