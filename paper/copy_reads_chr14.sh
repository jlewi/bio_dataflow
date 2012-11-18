#!/bin/sh

set -e
set -xv

mkdir tmp_chr14
cd tmp_chr14

curl -O http://gage.cbcb.umd.edu/data/Hg_chr14/Data.original/frag_1.fastq.gz
gsutil cp frag_1.fastq.gz gs://contrail/chr14/reads/fragment/frag_1.fastq.gz

curl -O http://gage.cbcb.umd.edu/data/Hg_chr14/Data.original/frag_2.fastq.gz
gsutil cp frag_2.fastq.gz gs://contrail/chr14/reads/fragment/frag_2.fastq.gz

curl -O http://gage.cbcb.umd.edu/data/Hg_chr14/Data.original/shortjump_1.fastq.gz
gsutil cp shortjump_1.fastq.gz gs://contrail/chr14/reads/jump1/shortjump_1.fastq.gz

curl -O http://gage.cbcb.umd.edu/data/Hg_chr14/Data.original/shortjump_2.fastq.gz
gsutil cp shortjump_2.fastq.gz gs://contrail/chr14/reads/jump1/shortjump_2.fastq.gz

curl -O http://gage.cbcb.umd.edu/data/Hg_chr14/Data.original/longjump_1.fastq.gz
gsutil cp longjump_1.fastq.gz gs://contrail/chr14/reads/fosmid1/longjump_1.fastq.gz

curl -O http://gage.cbcb.umd.edu/data/Hg_chr14/Data.original/longjump_2.fastq.gz
gsutil cp longjump_2.fastq.gz gs://contrail/chr14/reads/fosmid1/longjump_2.fastq.gz
