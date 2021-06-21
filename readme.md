# Scala Maven project to monitor a folder in HDFS in real time such that any new file in the folder will be processed.

To copy standalone jar from HDFS to EMR FS:
`hadoop fs -copyToLocal /user/s3806186/assignment2Scala-0.0.1-SNAPSHOT.jar .`

To run task 2 using standalone jar:
`spark-submit --class edu.rmit.cosc2367.s3806186.assignment2Scala.NetworkWordCount --master yarn --deploy-mode client assignment2Scala-0.0.1-SNAPSHOT.jar hdfs:///user/s3806186/input hdfs:///user/s3806186/task2/subtaska hdfs:///user/s3806186/task2/subtaskb hdfs:///user/s3806186/task2/subtaskc`

