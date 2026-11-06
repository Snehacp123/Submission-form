# Flink with the connectors the hourly-counts job needs.
FROM flink:1.19.1-scala_2.12-java11

# Postgres CDC source, JDBC sink, and the Postgres JDBC driver.
ADD --chmod=644 https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-postgres-cdc/3.2.1/flink-sql-connector-postgres-cdc-3.2.1.jar /opt/flink/lib/
ADD --chmod=644 https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.2.0-1.19/flink-connector-jdbc-3.2.0-1.19.jar /opt/flink/lib/
ADD --chmod=644 https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar /opt/flink/lib/

# Checkpoint directory, shared by the JobManager and TaskManager via a volume.
RUN mkdir -p /opt/flink/checkpoints && chown flink:flink /opt/flink/checkpoints
