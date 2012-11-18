#!/bin/sh

set -e
set -xv

mkdir tmp_staph
cd tmp_staph

curl -O http://gage.cbcb.umd.edu/data/Staphylococcus_aureus/Data.original/frag_1.fastq.gz
gsutil cp frag_1.fastq.gz gs://contrail/staph/reads/fragment/frag_1.fastq.gz

curl -O http://gage.cbcb.umd.edu/data/Staphylococcus_aureus/Data.original/frag_2.fastq.gz
gsutil cp frag_2.fastq.gz gs://contrail/staph/reads/fragment/frag_2.fastq.gz

curl -O http://gage.cbcb.umd.edu/data/Staphylococcus_aureus/Data.original/shortjump_1.fastq.gz
gsutil cp shortjump_1.fastq.gz gs://contrail/staph/reads/jump1/shortjump_1.fastq.gz

curl -O http://gage.cbcb.umd.edu/data/Staphylococcus_aureus/Data.original/shortjump_2.fastq.gz
gsutil cp shortjump_2.fastq.gz gs://contrail/staph/reads/jump1/shortjump_2.fastq.gz

