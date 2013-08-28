#!/bin/sh

#run wordcount.jar
filename=$1
#DocNum=$(hadoop fs -cat MakeMatrix_phrase2/transdata0/p*|wc --line)
DocNum=$(hadoop fs -cat $1/transdata0/p*|wc --line)
echo $DocNum
hadoop fs -rmr $filename/Matrix_tfidf*
hadoop jar hadoop/hadoop_sample/hadoop.jar hadoop_sample.TFIDF_hadoop temp0 $DocNum $filename
