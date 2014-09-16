#!/usr/bin/python
#  =====================================================
#  = Export field dictionary in humand readable format =
#  =====================================================
import sys      # file input / output
# TODO: 2012-08-17 - push local myspot changes back to main myspot
# Use local version of myspot module
# You can switch back to using the main myspot module if you push local changes back

sys.path.remove('/Users/steve/usr/local/lib')
sys.path.append('/Users/steve/data/spot_id/local/lib_usr')
import myspot
import mypy
import os

fdict_unsorted = myspot.get_yaml_dict('field', return_type='list', local_dict=True)
# print fdict[:2]
fdict = sorted(fdict_unsorted, key=lambda f: f['fname'].lower())

print len(fdict)

text_list = []

for f in fdict:
    if 'derived' in f:
        if f['derived']:
            continue
    if f['fname'][0] == '_':
        continue
    if 'vallab' in f:
        vallab = ["\n- Code and label\n"]
        for kv in f['vallab'].items():
            vallab.append( "    - %s:  %s  " % kv)
    else:
        vallab = []
    vallab = '\n'.join(vallab)

    try:
        new_text = """
#### %s

%s
%s
""" % (f['fname'], f['varlab'], vallab)
        text_list.append(new_text)
    except KeyError:
        print "ERROR: Unable to find all keys for %s" % f['fname']


text_out = ''.join(text_list)
print text_out

file_name = """/users/steve/desktop/field_dictionary.md"""
file_object = open(file_name, 'w')
file_object.writelines(text_out)

file_object = open(file_name, 'r')
