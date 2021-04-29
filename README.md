WaterGridSense4.0 Analytics flink jobs
======================================

This repository contains the source code for all Flink jobs that have been developed for and are used within the scope of the [WaterGridSense4.0](https://www.dos.tu-berlin.de/menue/research/watergridsense_40/) project. The jobs are to be submitted to the Flink instance of a running deployment of the [WaterGridSense4.0 Analytics Platform](https://github.com/dos-group/water-analytics-cluster).

## Directory structure

Each job is fully contained within it's own subdirectory. Some general class definitions that are used by all jobs are contained in the `watergridsense_jobs.utils` package.

    .
    ├── utils         # General utilities and base classes used by all jobs
    ├── TestJob       # Simple job that reads from and writes to kafka and cassandra
    ├── StorageJob    # Reads enriched sensor data and parameter updates and stores them to 
    ├── EnrichmentJob # Enriches sensor data with attributes read from Kafka. Republishes to Kafka but does not store to cassandra.
    ├── NeighborJob   # Computes the deviation of a sensor value from the mean of its neighbors and raises alarms.
    ├── ArimaJob      # Uses ARIMA to detect anomalies in sensor streams (WIP)

## Configuration

The jobs are configured by adjusting the settings in each respective `.properties` file contained in `*/src/main/resources/`. In particular, please make sure to replace all capitalized placeholders with their correct values. Furthermore, make sure to place the truststore needed for connecting to cassandra under `*/src/main/resources/cassandra.truststore.jks`.

All other parameters are pre-configured to match the [WaterGridSense4.0 cluster deployment](https://github.com/dos-group/water-analytics-cluster).

## Building the jobs

To build all jobs, just use `make`:

```
make
```

After the compilation was successful, you can find the jar files for each flink job in the respective `target/` folder. Submit them to flink either via the [rest interface](https://ci.apache.org/projects/flink/flink-docs-stable/ops/rest_api.html) or via the [command line interface](https://ci.apache.org/projects/flink/flink-docs-stable/deployment/cli.html).