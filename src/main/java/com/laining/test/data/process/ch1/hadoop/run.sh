export APP_JAR=/Users/laining/workspace/process/target/process-0.0.1-SNAPSHOT.jar
INPUT=/secondary_sort/input
OUTPUT=/secondary_sort/output
$HADOOP_HOME/bin/hadoop fs -rm -r $OUTPUT
PROG=com.laining.test.data.process.ch1.hadoop.SecondarySortDriver
$HADOOP_HOME/bin/hadoop jar $APP_JAR $PROG $INPUT $OUTPUT
