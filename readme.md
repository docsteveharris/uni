author: Steve Harris
date: 2014-09-16
subject: Shared academic resources

Readme
======
This repo should contain all academic resources that I am likely to need. Initially, I will copy `ccode` and `lib_phd` into here.

## lib_phd

Everything is YAML formatted. There is a tutorial on YAML [here](http://rhnh.net/2011/01/31/yaml-tutorial), but the important thing to remember is that `tabs` are forbidden, and spaces (2 per `tab`) are used instead.

- Data tables are defined in `dictionary_tables.yml`
- Field definitions are in `dictionary_fields.yml`
- Validation checks at the field level are also in `dictionary_fields.yml` but validation code at the record level is in `dictionary_checks.yml`
- Specific corrections are in `dictionary_corrections.yml`

These dictionaries are then used to import, index, and clean all data for the project.

## Shared code for (SPOT)light, (SPOT)id, and other analyses



Todo
====


Log
===
2014-09-16
- file created