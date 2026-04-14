# AGENTS.md

## Project goal
Build a Hadoop + Spark website similarity detection and recommendation system.

## Tech stack
- Python
- PySpark
- Hadoop / HDFS
- Spark SQL / DataFrame API preferred

## Priorities
1. Working MVP first
2. Clear project structure
3. Readable code
4. Easy to demo in class
5. Then optimize performance

## Rules
- Do not overwrite unrelated files
- Explain assumptions before implementing
- Prefer simple and teachable solutions
- Keep scripts modular
- Update README when adding functionality
- Provide runnable commands
- Avoid unnecessary dependencies

## Done criteria
- Can read edge list input
- Can compute pairwise website similarity using inverted-index-style candidate generation
- Can output Top-K similar websites per site
- Can run locally with Spark
- Includes README and sample data

## Runtime environment

This project is intended to run on a Linux cluster, not just on a local machine.

Environment constraints:
- OS: Linux
- Java: JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
- Hadoop: HADOOP_HOME=/home/pastorale/hadoop
- Hadoop conf: HADOOP_CONF_DIR=/home/pastorale/hadoop/etc/hadoop
- Spark: SPARK_HOME=/home/pastorale/spark
- Python: use python3
- Spark jobs must support both local mode and YARN mode
- HDFS commands must use `hdfs dfs`
- Input and output paths should support both local filesystem paths and HDFS paths
- Prefer `spark-submit` examples over IDE-only execution
- The project will eventually be tested on a Linux Hadoop/Spark cluster

Environment exports:
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_HOME=/home/pastorale/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export SPARK_HOME=/home/pastorale/spark
export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3

Implementation rules:
- Do not assume only local mode.
- README must include Linux cluster usage.
- Provide `spark-submit` commands for both:
  - local[*]
  - --master yarn
- Provide HDFS upload/download examples using `hdfs dfs -put`, `hdfs dfs -get`, `hdfs dfs -ls`
- Do not hardcode Windows or macOS paths.
- Do not rely on IDE-specific launch settings.
- Keep file paths relative unless HDFS paths are explicitly required.