#!/usr/bin/perl
use strict;
use warnings;
use File::Copy;

# Notes
# stata outsheet places a period '.' in all missing fields
# sql LOAD DATA will force this into a zero in all numeric fields
# so use this routine to replace all '.' with '\N' which LOAD DATA will read as NULL
# NB: \N is only read as NULL if LOAD DATA does not explicity define field delimiters
# see http://dev.mysql.com/doc/refman/5.1/en/load-data.html

sub clean_file {
	my $input_file_name = shift;
	my $output_file_name = $input_file_name;
	
	# use the path to the input file to create a temp file called jj_temp.txt in the same dir
	$output_file_name =~ s{	\/(?!.*\/) 	# match a forward slash if there are no more after
							.*$			# and match the remainder of the string
							}
							{/jj_temp.txt}x;
	
	# open the input file
	open (INPUT_FILE_HANDLE, "$input_file_name") || die "Couldn't open $input_file_name\n";
	
	# delete any existing copies of the output file
	unlink ( $output_file_name ) ;
	
	# open the output file NB: the > operator means open and overwrite
	open (OUTPUT_FILE_HANDLE, ">$output_file_name") || die "Couldn't open $output_file_name\n";
	
	while (my $this_line = <INPUT_FILE_HANDLE>) {
		
		# replace all '.' with \N
		$this_line =~ s{\t\.(?=\t)}{\t\\N}g  		; 
		
		 # replace empty fields (i.e. consecutive tabs) with \N
		$this_line =~ s{\t(?=\t)}{\t\\N}g  	;
		
		print OUTPUT_FILE_HANDLE $this_line;
		
	}
	
	close INPUT_FILE_HANDLE;
	close OUTPUT_FILE_HANDLE;
	
	# delete the input file
	unlink ( $input_file_name ) || die "Couldn't delete $input_file_name\n";
	move($output_file_name,$input_file_name);
}

			
my $file_to_clean = '/Users/Steve/Projects/(SPOT)id/19. Statistical Analysis/Data/idwide.txt';

clean_file @ARGV;
print "Program completed OK!\n";
