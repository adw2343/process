export APP_JAR=/Users/laining/workspace/process/target/process-0.0.1-SNAPSHOT.jar
JOBPATH=/topN
DIRNAME=$(cd `dirname $0`;pwd)
INPUTPATH=$(cd `dirname $DIRNAME`;pwd)
INPUT=$JOBPATH/input/
OUTPUT=$JOBPATH/output/
$HADOOP_HOME/bin/hadoop fs -rm -r $JOBPATH
$HADOOP_HOME/bin/hadoop fs -mkdir $JOBPATH
$HADOOP_HOME/bin/hadoop fs -mkdir $INPUT
cd $INPUTPATH
$HADOOP_HOME/bin/hadoop fs -put sample_input.txt $INPUT
PROG=com.laining.test.data.process.ch3.uniquekey.hadoop.TopNDriver
TOPN=20
$HADOOP_HOME/bin/hadoop jar $APP_JAR $PROG $TOPN $INPUT $OUTPUT
