export APP_JAR=/Users/laining/workspace/process/target/process-0.0.1-SNAPSHOT.jar
export SPARK_HOME=/usr/local/spark
DIRNAME=$(cd `dirname $0`;pwd)
INPUTPATH=$(cd `dirname $DIRNAME`;pwd)
PROG=com.laining.test.data.process.ch3.duplicatedkey.spark.DuplicatedKeyTopN
INPUT=$INPUTPATH/sample_input.txt
OUTPUT=/data/duplicatedTopN/
$SPARK_HOME/bin/spark-submit --class $PROG --master local[4] $APP_JAR -f $INPUT -n 10 -o $OUTPUT