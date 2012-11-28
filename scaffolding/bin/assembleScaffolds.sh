#!/bin/sh

# You can specify the paths for AMOS here.
# The script will try to locate the needed binaries using which.
AMOS=/fs/sz-user-supported/Linux-x86_64/packages/AMOS-3.0.1/bin/
if [ ! -e $AMOS/bank-transact ]; then
   AMOS=`which bank-transact`
   AMOS=`dirname $AMOS`
fi

MACHINE=`uname`
PROC=`uname -p`
SCRIPT_PATH=$0
SCRIPT_PATH=`dirname $SCRIPT_PATH`
JAVA_PATH=$SCRIPT_PATH:.

echo "Configuration summary:"
echo "AMOS: $AMOS"
echo "Java: $JAVA_PATH"

WORKDIR=$1
PREFIX=$2
UTGCTG=$3
SUFFIX=$4

echo "Working Directory: $WORKDIR"
echo "Orignal reads directory: $UTGCTG"
echo "Prefix: $PREFIX"
echo "Suffic: $SUFFIX"

# Use bowtie to align the contigs to the reads.
echo java -cp $JAVA_PATH BuildBambusInput $WORKDIR $UTGCTG $SUFFIX $PREFIX.libSize $PREFIX.$SUFFIX
time java -cp $JAVA_PATH BuildBambusInput $WORKDIR $UTGCTG $SUFFIX $PREFIX.libSize $PREFIX.$SUFFIX

# Load the aligned contigs into the amos bank.
echo $AMOS/toAmos_new -s $PREFIX.$SUFFIX.fasta -m $PREFIX.$SUFFIX.library -c $PREFIX.$SUFFIX.contig -b $PREFIX.bnk
time $AMOS/toAmos_new -s $PREFIX.$SUFFIX.fasta -m $PREFIX.$SUFFIX.library -c $PREFIX.$SUFFIX.contig -b $PREFIX.bnk

# run bambus
echo "Running bambus"
echo $AMOS/goBambus2 $PREFIX.bnk ${PREFIX}_output clk bundle reps,"-noPathRepeats" orient,"-maxOverlap 500 -redundancy 0" 2fasta printscaff
time $AMOS/goBambus2 $PREFIX.bnk ${PREFIX}_output clk bundle reps,"-noPathRepeats" orient,"-maxOverlap 500 -redundancy 0" 2fasta printscaff

# finally process the output
java -cp $JAVA_PATH:. SizeFasta ${PREFIX}_output.contigs.fasta > lens
java -cp $JAVA_PATH:. SplitFastaByLetter ${PREFIX}_output.scaffold.fasta NNN > ${PREFIX}_output.scfContigs.fasta
cat $PREFIX.scaff.dot |grep position |awk '{print $1}' > scfContigs
java -cp $JAVA_PATH:. SubFile scfContigs lens 0 -1 true |awk '{if ($NF >= 1000) print $1" 1 "$NF" "$1}' |sort -nk1 > degContigs
java -cp $JAVA_PATH:. SubFasta degContigs ${PREFIX}_output.contigs.fasta > ${PREFIX}_output.degens.fasta
mv ${PREFIX}_output.contigs.fasta ${PREFIX}_output.unitigs.fasta
mv ${PREFIX}_output.scaffold.fasta ${PREFIX}_output.noDegens.scaffold.fasta
mv ${PREFIX}_output.scfContigs.fasta ${PREFIX}_output.noDegens.contig.fasta
cat ${PREFIX}_output.noDegens.contig.fasta ${PREFIX}_output.degens.fasta > ${PREFIX}_output.contigs.fasta
cat ${PREFIX}_output.noDegens.scaffold.fasta ${PREFIX}_output.degens.fasta > ${PREFIX}_output.scaffold.fasta

