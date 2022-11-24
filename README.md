# Data Lake

by Vicente Lizana

## Project summary

In the context of Sparkify growth into big data, the need for a Data Lake has become evident.
Digging into the MapReduce paradigm, Apache Spark was selected for the ELT task, in particular
the Elastic MapReduce (EMR) implementation from AWS.

The input for the ELT process is transactional data stored in S3 buckets in JSON format,
which are read onto the Spark cluster and transformed into the analytical tables in a
distributed fashion. The output is also stored on S3 in parquet format, its columnar
nature allows for more efficient queries because rarely the full dataset is used to
answer analytical questions.

## How to run the scripts

The `etl.py` must be run with access to the Spark cluster master node.

## Files on the repository

- `etl.py`: Script to transform the analytical data through Spark.
- `dl.cfg.tpl`: Template for the configuration file with the AWS user credentials.

