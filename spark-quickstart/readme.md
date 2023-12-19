# spark-quickstart

This project has the basics to get started running Spark.

## Requirements

- You have Java installed
```bash
java -version
```

- You have Maven installed
```bash
mvn -version 
```

## Setup
1. Download and install [Docker Desktop](https://www.docker.com/products/docker-desktop/)


## Running locally
1. Package the Java applicationa nd dependencies in a `.jar`:
```bash
mvn package
```

2. Submit to Spark
```bash
docker run -it  $(docker build -q .)
```

> [!TIP]
> `docker build -q .` reads the `Dockerfile` file 
> and builds a Docker image with your `.jar` and Spark.
> Then, `docker run -it` runs it.

## FAQ
### How to change what class is run?
In the file `Dockerfile`, 
change the parameter after `"--class"` 
to the name of the class you want to run.
