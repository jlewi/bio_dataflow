#!/bin/sh

set -e

for i in SRR067787 SRR067789 SRR067780 SRR067791 SRR067793 SRR067784 SRR067785 SRR067792 SRR067577 SRR067579 SRR067578 
do
  echo "Copying fragment library $i"
  gsutil cp gs://ncbi-sra/$i*.fastq.gz gs://contrail/human/reads/fragment
done

for i in SRR067771 SRR067777 SRR067781 SRR067776
do
  echo "Copying jump1 library $i"
  gsutil cp gs://ncbi-sra/$i*.fastq.gz gs://contrail/human/reads/jump1
done

for i in SRR067773 SRR067779 SRR067778 SRR067786
do
  echo "Copying jump2 library $i"
  gsutil cp gs://ncbi-sra/$i*.fastq.gz gs://contrail/human/reads/jump2
done

for i in SRR068214 SRR068211 
do
  echo "Copying fosmid1 library $i"
  gsutil cp gs://ncbi-sra/$i*.fastq.gz gs://contrail/human/reads/fosmid1
done

for i in SRR068335 
do
  echo "Copying fosmid2 library $i"
  gsutil cp gs://ncbi-sra/$i*.fastq.gz gs://contrail/human/reads/fosmid2
done

