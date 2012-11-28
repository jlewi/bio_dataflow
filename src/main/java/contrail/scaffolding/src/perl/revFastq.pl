#!/usr/bin/perl -w
 
use strict;
use warnings;

# TIGR Modules
use TIGR::Foundation;

my $tigr_tf = new TIGR::Foundation;
my $PRG = $tigr_tf->getProgramInfo('name');
my $REV="1.0";
my @DEPENDS=("TIGR::Foundation");

# help info
my $HELPTEXT = qq~
Program that ...

Usage: $PRG file [options]
	  
  INPUT:   
  
  file:	
  
  options:

	-h|help		- Print this help and exit;
	-V|version	- Print the version and exit;
	-depend		- Print the program and database dependency list;
	-debug <level>	- Set the debug <level> (0, non-debug by default); 
 
  OUTPUT:  
~;

my $MOREHELP = qq~
Return Codes:   0 - on success, 1 - on failure.
~;


###############################################################################
#
# Main program
#
###############################################################################

MAIN:
{
	# define variables
	my %options;
			
	# Configure TIGR Foundation
	$tigr_tf->setHelpInfo($HELPTEXT.$MOREHELP);
        $tigr_tf->setUsageInfo($HELPTEXT);
        $tigr_tf->setVersionInfo($REV);
        $tigr_tf->addDependInfo(@DEPENDS);
	
	# validate input parameters
	my $result = $tigr_tf->TIGR_GetOptions(
		"solexa"        =>      \$options{solexa}
	);
	$tigr_tf->printUsageInfoAndExit() if (!$result);	

	while(<>)
	{		
		# @SRR001351.1.1 E8YURXS01ECEBV.1 length=285		
		# TCAGGGGGGCAGTTACCTCGGAGCAAGGGCTGTGATCGAAGCATTGCAAAACAG
		# +SRR001351.1.1 E8YURXS01ECEBV.1 length=285
		# FFC000000CGFFFFFFFFFFFIIIIIIIIIIIIIIIIIIIIIIIIIIIFFFFF

		chomp;

		if($.%4==2) 
		{
			my $seq=$_;
			$seq=~tr/acgtACGT/tgcaTGCA/;

			my @seq=split //,$seq;
			@seq=reverse(@seq);

			$seq=join "",@seq;
			print $seq,"\n"; 
		}
		elsif( $.%4==0)
                {
			my $qlt=$_;

                        my @qlt=split //,$qlt;
                        @qlt=reverse(@qlt);

                        $qlt=join "",@qlt;
                        print $qlt,"\n";
                }
		else
                {
                        print $_,"\n";
                }
	}
			
	exit 0;
}
