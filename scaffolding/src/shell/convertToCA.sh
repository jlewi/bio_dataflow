#!/bin/sh
AMOS=/fs/sz-user-supported/Linux-x86_64/packages/AMOS-3.0.1/bin/
if [ ! -e $AMOS/bank-transact ]; then
   AMOS=`which bank-transact`
   AMOS=`dirname $AMOS`
fi
CA=/fs/szdevel/core-cbcb-software/Linux-x86_64/packages/wgs-6.1//Linux-amd64/bin/
if [ ! -e $CA/gatekeeper ]; then
   CA=`which gatekeeper`
   CA=`dirname $CA`
fi

MACHINE=`uname`
PROC=`uname -p`
SCRIPT_PATH=$0
SCRIPT_PATH=`dirname $SCRIPT_PATH`
CA_TERMINATOR=$SCRIPT_PATH/../$MACHINE-$PROC/bin/terminator
JAVA_PATH=$SCRIPT_PATH:.

echo "Configuration summary:"
echo "AMOS: $AMOS"
echo "CA: $CA"
echo "Terminator: $CA_TERMINATOR"
echo "Java: $JAVA_PATH"

WORKDIR=$2
PREFIX=$3
UTGCTG=$4
RUNBAMBUS=$5
SUFFIX=$6

# run the assembler
if [ -e $WORKDIR/5-consensus ]; then
   echo "Using CA assembly in $WORKDIR";
else
   (time runCA -s ./parallel.spec -d . -p genome doOverlapBasedTrimming=0 ovlOverlapper=ovl bogBadMateDepth=1000 unitigger=bog stopAfter=unitigger *.frg) > runCA.log 2>&1
   GENOME=`cat $WORKDIR/4-unitigger/unitigger.err |grep "Computed genome_size" |awk -F "=" '{print $2}'`
   BASES=`$CA/gatekeeper -dumpfragments -tabular genome.gkpStore|awk '{if ($3 != 0 && $7 != 1) SUM+=$10-1; print SUM}'|tail -n 1`
   THRESHOLD=`java -cp $JAVA_PATH estimateMateThreshold $GENOME $BASES |grep Threshold |awk '{print $2}'` 
echo "Using threshold $THRESHOLD"
   rm $WORKDIR/4-unitigger/unitigger.success
   (time runCA -s ./parallel.spec -d . -p genome doOverlapBasedTrimming=0 ovlOverlapper=ovl bogBadMateDepth=$THRESHOLD unitigger=bog stopAfter=utgcns *.frg) >> runCA.log 2>&1
fi
if [ -e $WORKDIR/9-terminator ]; then
   echo "CA Ran to completion, using final output";
   ln -s -f $WORKDIR/9-terminator/$PREFIX.asm $WORKDIR/$PREFIX.asm
   ln -s -f $WORKDIR/9-terminator/$PREFIX.$SUFFIX $WORKDIR/$PREFIX.$SUFFIX
elif [ -e $WORKDIR/$PREFIX.asm ]; then
   echo "CA finished and output files are ready";
else
   $CA_TERMINATOR -g $WORKDIR/$PREFIX.gkpStore -t $WORKDIR/$PREFIX.tigStore 2 -o $WORKDIR/$PREFIX
   $CA/asmOutputFasta -p $WORKDIR/$PREFIX -D -C -S < $WORKDIR/$PREFIX.asm 
fi

# support both CA and mapping (allpaths or CA) 
# if we need to map as well
if [ $1 == 0 ]; then
   time java -cp $JAVA_PATH buildBambusInput $WORKDIR $UTGCTG $SUFFIX $PREFIX.libSize
   time $AMOS/toAmos_new -s $PREFIX.$SUFFIX.fasta -m $PREFIX.$SUFFIX.library -c $PREFIX.$SUFFIX.contig -b $PREFIX.bnk 
else
   time $CA/gatekeeper -dumpfrg -allreads -format2 $WORKDIR/$PREFIX.gkpStore > asm.frgs
   if [ $UTGCTG == 0 ]; then
      echo "time $AMOS/toAmos_new -f asm.frgs -a $WORKDIR/$PREFIX.asm -U -b $PREFIX.bnk"
      if [ -e $PREFIX.libSize ]; then
         echo "Including extra mates from $PREFIX.mates"
         time java -cp $JAVA_PATH addMissingMates $PREFIX.libSize $WORKDIR/$PREFIX $CA
         time $AMOS/toAmos_new -f asm.frgs -m $PREFIX.mates -a $WORKDIR/$PREFIX.asm -U -b $PREFIX.bnk
      else
         time $AMOS/toAmos_new -f asm.frgs -a $WORKDIR/$PREFIX.asm -U -b $PREFIX.bnk
      fi
   else
      time $AMOS/toAmos_new -f asm.frgs -a $WORKDIR/9-terminator/$PREFIX.asm -C -S -b $PREFIX.bnk
      #remove CA-generated links and scaffolds
      rm -rf $PREFIX.bnk/CTE.*
      rm -rf $PREFIX.bnk/CTL.*
      rm -rf $PREFIX.bnk/SCF.*
   fi
fi
echo "Running bambus"
# run bambus
if [ -z "${RUNBAMBUS+x}" ] || [ $RUNBAMBUS == 1 ]; then
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
fi
