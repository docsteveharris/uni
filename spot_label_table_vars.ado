*  ===========================================================================
*  = Code to label up the vars in the data-set using the standard dictionary =
*  ===========================================================================

/*
Dependencies
- fields_yaml2tab.py
Assumes
- a variable called varname
*/

cap program drop spot_label_table_vars
program spot_label_table_vars
	syntax
	tempfile original
	save `original', replace
	* First of all produce a YAML formatted list of variables
	file open myvars using ../data/scratch/vars.yml, text write replace
	local lastrow = _N
	forvalues i = 1/`lastrow' {
		local varname = varname[`i']
		di "- `varname'" _newline
		file write myvars "- `varname'" _newline
	}
	file close myvars
	qui compress
	/*
	Now run the python script to make a tab delimited file that contains
	- the original variable against which you can merge
	- plus
		- tablerowlabel
		- units
		- format
		- standard reporting method
	*/

	shell ../ccode/fields_yaml2tab.py ../data/scratch/vars.yml -fmt latex
	capture confirm file ../data/scratch/_field_characteristics.txt
	if _rc == 0 {
		tempfile 2merge working
		insheet using ../data/scratch/_field_characteristics.txt, ///
				names tab clear
		rename fname varname
		cap confirm string var var_level_lab
		if _rc {
			tostring var_level_lab, replace
			di as error "WARNING: Forced var_level_lab to string - check you have not lost any information"
		}
		cap drop v?
		save `working', replace
		// save the first time and merge in variable level data
		cap drop var_level
		cap drop var_level_lab
		duplicates drop varname, force
		save `2merge', replace
		use `original', clear
		merge m:1 varname using `2merge'
		cap drop attributes_found
		gen attributes_found = _m == 3
		drop _m
		save `original', replace
		// now merge in value label level data
		use `working', clear
		keep varname var_level var_level_lab
		save `2merge', replace
		use `original', clear
		merge m:1 varname var_level using `2merge'
		// CHANGED: 2013-02-17 - force var_level_lab to be string
		replace var_level_lab = "" if _m == 2 | var_level == -1
		drop if _m == 2
		drop _m

		// Use category labels for row labels
		shell rm ../data/scratch/_field_characteristics.txt
	}
	else {
		di as error "Error: Unable to label data"
		exit
	}

end

* Debugging and testing


spot_label_table_vars

