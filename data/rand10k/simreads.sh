#!/bin/sh

if [ ! -r rand10k.fa ]
then
  echo "Simulating genome"
  perl -e 'srand(1234); print ">rand10k\n"; @D=split //,"ACGT"; for (1...10000){print $D[int(rand(4))];} print "\n"' \
    | fold > rand10k.fa
fi

if [ ! -r pairs.1.fq ]
then
  echo "Simulating pairs"
  ~/build/packages/dwgsim/dwgsim -e 0.001-0.02 -E 0.001-0.02 -S 2 -d 180 -s 20 -C 30 -1 100 -2 100 -r 0 rand10k.fa pairs

  fastq_rename -prefix pair -renum -suffix '/1' pairs.bwa.read1.fastq > pairs.1.fq
  fastq_rename -prefix pair -renum -suffix '/2' pairs.bwa.read2.fastq > pairs.2.fq

  rm *.fastq
fi



if [ ! -r mates2k.1.fq ]
then
  echo "Simulating 2k mates"
  ~/build/packages/dwgsim/dwgsim -e 0.001-0.02 -E 0.001-0.02 -S 2 -d 2000 -s 200 -C 30 -1 50 -2 50 -r 0 rand10k.fa mates2k

  fastq_rename -prefix mate -renum -suffix '/1' mates2k.bwa.read1.fastq | fastx_reverse_complement > mates2k.1.fq
  fastq_rename -prefix mate -renum -suffix '/2' mates2k.bwa.read2.fastq | fastx_reverse_complement > mates2k.2.fq

  rm *.fastq
fi



