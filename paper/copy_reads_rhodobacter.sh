#!/bin/sh

set -e

curl -O http://gage.cbcb.umd.edu/data/Rhodobacter_sphaeroides/Data.original/frag_1.fastq.gz
gsutil cp frag_1.fastq.gz gs://contrail/rhodobacter/reads/fragment

curl -O http://gage.cbcb.umd.edu/data/Rhodobacter_sphaeroides/Data.original/frag_2.fastq.gz
gsutil cp frag_2.fastq.gz gs://contrail/rhodobacter/reads/fragment

curl -O http://gage.cbcb.umd.edu/data/Rhodobacter_sphaeroides/Data.original/shortjump_1.fastq.gz
gsutil cp shortjump_1.fastq.gz gs://contrail/rhodobacter/reads/jump1

curl -O http://gage.cbcb.umd.edu/data/Rhodobacter_sphaeroides/Data.original/shortjump_2.fastq.gz
gsutil cp shortjump_2.fastq.gz gs://contrail/rhodobacter/reads/jump1
