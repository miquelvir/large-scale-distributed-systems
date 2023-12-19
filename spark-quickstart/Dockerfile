FROM apache/spark

# copy the jar into the container
COPY /target/spark-quickstart-1.0-SNAPSHOT-fat.jar job.jar

# submit (run) the spark job
CMD ["/opt/spark/bin/spark-submit", "--class", "SumSample", "job.jar"]
