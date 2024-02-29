# csv-to-delta-ingester

### Requirements:
- Pyspark: 3.3.1
- Delta Lake: 2.1.0 (DeltaLake 1.2.1 is not compatible with our Pyspark version)
- pyyaml: 5.4.1

### Usage:
Run the following commands in your terminal to:
- ```pytest``` : run all tests
- ```docker-compose up --build``` : build and start Docker containers defined in the docker-compose.yml file

**PS:** you should see a message in the console indicating that the container has exited with code 0. 
- Navigate to ```localhost:18080``` in your web browser: Monitor The Spark job Logs/Traces

### Home Task - Data Engineer - 2
1- Spark Job designed and coded

2- Run the job from a Container:
  - Spark Job Dockerfile produced
  - Add SparkHistoryServer container:
      * Difficulty: Couldn't use image 'gcr.io/spark-operator/spark:v2.4.0' hosted on (GCR) because I do not have a Google Cloud account.
      * Solution: Used image 'rangareddy1988/spark-history-server' as a Spark history server

3- Solution's Diagram on AWS:

![solution diagram 4](https://github.com/aghassen/csv-to-delta-ingester/assets/96908558/ef700a3f-2a74-4fbe-887e-9e407617e0ca)

