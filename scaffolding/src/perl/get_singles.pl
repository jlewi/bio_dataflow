#!/usr/bin/perl

# Estimates singletons from assemblies

use Getopt::Long;
use strict;

# Location of bowtie
my $resultDir = `pwd`;
my $bowtie = `which bowtie`;
my $bowtiebuild = `which bowtie-build`;
chomp $bowtie;
chomp $bowtiebuild;
chomp $resultDir;

$resultDir .= "/";

print STDERR "Usage: get_singles.pl -reads READDIR -assembly ASMDIR [--threads NN]\n";
print STDERR "READDIR - location of raw reads\n";
print STDERR "ASMDIR - location of assembly output (in PGA format)\n";
print STDERR "--threads NN - use NN threads when running bowtie\n";
print STDERR "\nNOTE: if the output files called *.bout exist bowtie is not run\n";
print STDERR "\nNOTE: assumes data are in the PGA format as stored at the HMP DACC, i.e. the assembly directory contains a file named PREFIX.contigs.fa\n";

my $readdir = undef;
my $asmdir = undef;
my $threads = undef;
my $trim = 25;
my $suffix = "contigs.fa";
GetOptions(
    "reads=s" => \$readdir, # database
    "assembly=s" => \$asmdir,
    "trim=f" => \$trim,
    "threads=i" => \$threads,
    "suffix=s" => \$suffix);

if (! defined $readdir){
    die ("Please provide a directory where the reads are located\n");
}

if (! defined $asmdir) {
    die ("Please provide a directory where the assembly is located\n");
}


my $bowtie_param = "-v 1 -M 2";
if (defined $threads) {$bowtie_param .= " -p $threads";}

opendir(ASM, $asmdir) || die ("Cannot open $asmdir: $!\n");
my @files = grep {/^.*\.$suffix$/} readdir(ASM);
closedir(ASM);

printf STDERR "The assembly file is $files[0]\n";
# build bowtie index
system "$bowtie-build $asmdir/$files[0] $resultDir/IDX";

opendir(RDS, $readdir) || die ("Cannot open $readdir: $!\n");
my @rds = grep {/^.*_[12].*\.fastq.*/} readdir(RDS);
closedir(RDS);

foreach my $file (@rds){
    # first trim to 25bp
    # TODO(jeremy@lewi.us): We shouldn't need to trim to 25 bp because BuildBambusInput should
    # already take care of this.
    if ($file =~ m/SRR/) { next; }
    $file =~ /(.*_[12].*)\.fastq.*/;

    my $prefix = $1;
printf STDERR "Processing file $file with prefix $prefix\n";
    open(TRIM, ">$resultDir/$prefix.trim.fastq") || die ("Cannot open $resultDir/$prefix.trim.fastq: $!\n");

    if ($file =~ /.*\.gz/){
	open(IN, "gzcat $readdir/$file |") || die ("Cannot open $readdir/$file: $!\n");
    } elsif ($file =~ /.*\.bz2/){
	open(IN, "bzip2 -dc $readdir/$file |") || die ("Cannot open $readdir/$file: $!\n");
    } else {
	open(IN, "$readdir/$file") || die ("Cannot open $readdir/$file: $!\n");
    }

    while(<IN>){
	if ($. % 2 == 0){
	    print TRIM substr($_, 0, 25), "\n";
	} else {
	    print TRIM;
	}
    }
    close(IN);
    close(TRIM);
    # run bowtie
    system("$bowtie $bowtie_param $resultDir/IDX $resultDir/$prefix.trim.fastq > $resultDir/$prefix.bout");
    unlink("$resultDir/$prefix.trim.fastq");
}

print stderr "DONE RUNNING BOWTIE, BUILDING UNMAPPED READS\n";
die;
