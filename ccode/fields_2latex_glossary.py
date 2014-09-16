#!/usr/local/bin/python
# ==========================================================
# = Export field dictionary into format for latex glossary =
# ==========================================================
# Created 130521
# Notes
# Change log


def get_yaml_dict():
    """
    Read in yaml file
    Return a Python data object
    - assumes that code is running in project dir which also contains above dir
    """
    path_to = '/Users/Steve/dropbox/nvalt/'
    dict_file = '@u thesis glossary and acronyms.yml'
    dict_file = path_to + dict_file

    # get field dictionary as python data structure
    with open(dict_file, 'r') as ffile:
        yaml_dict_as_list = yaml.load(ffile.read())

    return yaml_dict_as_list


def clean_for_latex(s):
    """
    Take a string and clean it up for use in latex
    """
    s = [l.replace('&', '\\&') for l in s]
    s = [l.replace('%', '\\%') for l in s]
    s = [l.replace("""'""", """'""") for l in s]
    return ''.join(s)


def md2latex(md):
    p = subprocess.Popen(['pandoc', '--from=markdown', '--to=latex'],
                         stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    return p.communicate(md)[0]

import subprocess
import sys      # file input / output
from unidecode import unidecode
import yaml
import myspot
import mypy
import os
import string

# =================================
# Step 3 - get the field dictionary
fdict = myspot.get_yaml_dict('field', return_type='dictionary', local_dict=False)
print len(fdict)

# Now define your own appendix - don't use latex glossary
# _______________________________________________________
entries = {}
for k, fentry in fdict.items():
    if 'definition' not in fentry:
        continue
    if fentry.get('source') is None:
        continue
    entry = []
    if 'tablerowlabel' in fentry:
        entry_heading = "## %s" % fentry['tablerowlabel']['latex']
    else:
        entry_heading = "## %s" % fentry['varlab']
    print entry_heading
    entry.append(entry_heading)
    entry_subheading = "### Field definition"
    entry.append(entry_subheading)
    # First clean the string of any odd characters
    entry_definition = fentry['definition'].encode('utf-8')
    entry.append(entry_definition)

    # Units
    if 'unitlabel' in fentry:
        # Use the latex specified label
        unit_label = fentry['unitlabel']['latex']
        # Replace double back slashes with single
        unit_label = string.replace(unit_label,u'\\\\',u'\\')
        unit_label = string.replace(unit_label,u'{\\texttimes}',u'\\texttimes ')
        print unit_label
        unit_label = '- Units: ' + unit_label + '\n'
        entry.append(unit_label)

    # Field level validation
    min, max, regex, legal, vallab = None, None, None, None, None
    if 'checks' in fentry:
        # Convert list of checks into dictionary
        check_dict = {i['type']: i for i in fentry['checks']}
        min = check_dict.get('min')
        max = check_dict.get('max')
        regex = check_dict.get('regex')
        legal = check_dict.get('legal')
    if 'vallab' in fentry:
        vallab = fentry.get('vallab')
    if min or max or regex or vallab:
        entry_subheading = "### Logical checks\n"
        entry.append(entry_subheading)

        # Legal values
        if vallab:
            entry_subheading = "- Legal values:"
            entry.append(entry_subheading)
            entry_legal = fentry['vallab'].values()
            entry_legal = '\n    - ' + '\n    - '.join(entry_legal)
            entry_legal = entry_legal + '\n'
            entry.append(entry_legal)

        # Range checks
        if min or max or regex:
            if min and max:
                range = "- Legal range: `%s` $\leq value \leq$ `%s`" % (min['value'], max['value'])
                entry.append(range)
            elif min:
                range = "- Legal range: $value \geq$ `%s`" % (min['value'])
                entry.append(range)
            elif max:
                range = "- Legal range: $value \leq$ `%s`" % (max['value'])
                entry.append(range)
            if regex:
                regex = "- Regular expression: `%s`" % (regex['value'])
                entry.append(regex)
    # TODO: 2013-07-04 - cross field level validation (use dictionary checks)



    end_of_entry = "\n"
    entry.append(end_of_entry)
    # Now join the lines of the entry together and append to the entries list
    # entries.append('\n'.join(entry))
    entries[entry_heading] = '\n'.join(entry)

# Sort the list of fields
entries_list = [entries[k] for k in sorted(entries)]

# print entries_list
md_out = '---\n\n'.join(entries_list)
md_out = '''

# (SPOT)light field definitions

\label{appendix:field_definitions}

''' + md_out
print md_out
latex_out = md2latex(md_out)

# =============================
# Step - produce the appendix
path_to = '/Users/steve/Data/phd/writing/Appendix/'
appendix_file = 'appendix_field_definitions.tex'
appendix_file = path_to + appendix_file

with open(appendix_file, 'w') as ffile:
    ffile.write(latex_out)
sys.exit(0)
# =============================================================
# Step 4 - loop through the field dictionary and append further

# Define as a dictionary --- then the key will be the label
glossary = {}
for k, fentry in fdict.items():
    entry = {}
    entry_items = {'name': 'varlab', 'description': 'definition'}
    for ke, ve in entry_items.items():
        if ve in fentry:
            # Now use pandoc to convert the entry to latex for the defintion
            if ke == 'description':
                # First clean the string of any odd characters
                clean = fentry[ve].encode('utf-8')
                # clean = unidecode(fentry[ve])
                entry[ke] = md2latex(clean)
            else:
                entry[ke] = clean_for_latex(fentry[ve])
    # Add to glossary where a definition has been made
    # Later expand this to permit entries just on acronyms?
    if len(entry) > 0 and 'description' in entry:
        glossary[k] = entry

# print len(glossary)
# print glossary
# Now produce the glossary entry
gentry_list = []
for label, kvpairs in glossary.items():
    kv_list = []
    for k, v in kvpairs.items():
        kv_list.append(u"%s = {%s}" % (k, v))
    option_list = ', '.join(kv_list)
    gentry = u"""\\newglossaryentry{%s}{%s}""" % (label, option_list)
    gentry_list.append(gentry)
    # print gentry

# =============================
# Step 5 - produce the glossary
# Now save this
path_to = '/Users/steve/Data/phd/writing/Appendix/'
dict_file = 'thesis_glossary.tex'
dict_file = path_to + dict_file

gloss_text = '\n'.join(gentry_list)
with open(dict_file, 'w') as ffile:
    ffile.write(gloss_text)
sys.exit(0)

# ====================================================
# Step 1 - get your running list from the nvAlt folder
gloss_yml = get_yaml_dict()

# ============================================
# Step 2 - create your glossary from this list
# Example: Mandatory arg 1 = label (must be unique)
# Example: Mandatory arg 2 = key value list
# Mandatory keys: name, description
# Specify type = acronym if defined for that purpose only
# \newglossaryentry{electrolyte}{name=electrolyte,
# description={solution able to conduct electric current}}
# For acronyms then define the 'first' key with the expansion and the 'text' key with the acronym

gloss_list = []
# Loop skips any incomplete entries
for entry in gloss_yml:
    entry_dict = {
        'acronym': '',
        'label': '',
        'item': '',
        'acronym_plural': '',
        'item_plural': ''
    }
    # Define the acronym: this will be the 'text' in the glossary entry
    if 'acronym' in entry:
        entry_dict['acronym'] = entry['acronym']
    else:
        print "WARNING: No acronym found for %s" % entry
        continue
    # Define the label using label if available else acronym
    if 'label' in entry:
        entry_dict['label'] = entry['label']
    else:
        entry_dict['label'] = entry['acronym']
    # Define the long name: this will be the 'first' in the glossary entry
    if 'item' in entry:
        entry_dict['item'] = entry['item']
    # Define the plural acronym: this will 'plural' in the glossary
    if 'acronym_plural' in entry:
        entry_dict['acronym_plural'] = entry['acronym_plural']
    # Define the long form plural: 'firstplural' in the glossary
    if 'item_plural' in entry:
        entry_dict['item_plural'] = entry['item_plural']

    entry_dict = {k:clean_for_latex(v) for k, v in entry_dict.items()}
    # print entry_dict
    # Optional keys
    option_dict = {
        "glsshortpluralkey": "acronym_plural",
        "glslongpluralkey": "item_plural"}
    option_list = []
    for k,v in option_dict.items():
        if entry_dict[v] != "":
            option_list.append(u"\\%s = %s" % (k, entry_dict[v]))

    entry_dict["option_list"] = "[" + ','.join(option_list) + "]"

    gloss_tex_entry = u"\\newacronym%(option_list)s{%(label)s}{%(acronym)s}{%(item)s}" % entry_dict
    gloss_list.append(gloss_tex_entry)
    print gloss_tex_entry

sys.exit(0)
# Code copied from export_dictionary.py

fdict_unsorted = myspot.get_yaml_dict('field', return_type='list', local_dict=False)
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

file_name = """/Users/steve/Data/phd/writing/Appendix/field_dictionary.md"""
file_object = open(file_name, 'w')
file_object.writelines(text_out)

# file_object = open(file_name, 'r')
