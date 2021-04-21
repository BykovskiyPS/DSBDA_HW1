hdfs dfs -rm -r input
hdfs dfs -rm -r output
hdfs dfs -mkdir input
hdfs dfs -put input input
hadoop jar target/lab1-1.0-SNAPSHOT-jar-with-dependencies.jar input output metricNames 5 s
