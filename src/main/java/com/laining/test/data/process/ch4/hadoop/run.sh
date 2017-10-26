export APP_JAR=/Users/laining/workspace/process/target/process-0.0.1-SNAPSHOT.jar
JOBPATH=/leftJoin
DIRNAME=$(cd `dirname $0`;pwd)
INPUTPATH=$(cd `dirname $DIRNAME`;pwd)
INPUT1=$JOBPATH/input1/
INPUT2=$JOBPATH/input2/
OUTPUT=$JOBPATH/output/
PHASE1OUT=$JOBPATH/phase1/
$HADOOP_HOME/bin/hadoop fs -rm -r $JOBPATH
$HADOOP_HOME/bin/hadoop fs -mkdir $JOBPATH
$HADOOP_HOME/bin/hadoop fs -mkdir $INPUT1
$HADOOP_HOME/bin/hadoop fs -mkdir $INPUT2
cd $INPUTPATH
$HADOOP_HOME/bin/hadoop fs -put user_sample_input.txt $INPUT1
$HADOOP_HOME/bin/hadoop fs -put transaction_sample_input.txt $INPUT2
PROG=com.laining.test.data.process.ch4.hadoop.LeftJoinDriver
PROG1=com.laining.test.data.process.ch4.hadoop.LocationCountDriver

$HADOOP_HOME/bin/hadoop jar $APP_JAR $PROG -f $INPUT1,$INPUT2 -o $PHASE1OUT

TOPN=10
$HADOOP_HOME/bin/hadoop jar $APP_JAR $PROG1 -f $PHASE1OUT -o $OUTPUT


