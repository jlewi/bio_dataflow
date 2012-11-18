#!/bin/sh

set -e
set -xv

mkdir tmp_rhodobacter
cd tmp_rhodobacter

curl -O http://gage.cbcb.umd.edu/data/Rhodobacter_sphaeroides/Data.original/frag_1.fastq.gz
gsutil cp frag_1.fastq.gz gs://contrail/rhodobacter/reads/fragment/frag_1.fastq.gz

curl -O http://gage.cbcb.umd.edu/data/Rhodobacter_sphaeroides/Data.original/frag_2.fastq.gz
gsutil cp frag_2.fastq.gz gs://contrail/rhodobacter/reads/fragment/frag_2.fastq.gz

curl -O http://gage.cbcb.umd.edu/data/Rhodobacter_sphaeroides/Data.original/shortjump_1.fastq.gz
gsutil cp shortjump_1.fastq.gz gs://contrail/rhodobacter/reads/jump1/shortjump_1.fastq.gz

curl -O http://gage.cbcb.umd.edu/data/Rhodobacter_sphaeroides/Data.original/shortjump_2.fastq.gz
gsutil cp shortjump_2.fastq.gz gs://contrail/rhodobacter/reads/jump1/shortjump_2.fastq.gz
