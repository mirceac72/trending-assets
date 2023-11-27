# Apache Beam Transformations to identify trending assets

## Relative strength index (RSI)

The [Relative Strength Index] (https://en.wikipedia.org/wiki/Relative_strength_index) measures the speed and change of a
value over a period of time.

An Apache Beam transformation to calculate the RSI of a given asset over a period of time can be found in the `transform` 
folder. The transformation is implemented in the `RSITransform` class. 

## Commands

Build and run the tests:
```bash
gradle build
```

## Test the pipeline locally

The `resources` contains sample avro files that can be used to test the pipeline. The file were generated using the
using [avro-tools-1.11.3.jar](https://mvnrepository.com/artifact/org.apache.avro/avro-tools) from the corresponding jsonl files and using the `asset-value.avsc` avro schema.

An example of the command used to generate the avro files is:
```bash
java -jar ./avro-tools-1.11.3.jar fromjson\
 --schema-file resources/asset-value.avsc\
 resources/asset-value-increasing-mixed.jsonl\
 > resources/asset-value-increasing-mixed.avro
```

The file `1-asset-rsi-table.sql` contains the SQL command to create the `asset_rsi` table in Clickhouse.

### Test pipeline using input from FILE and output to CONSOLE

```bash
gradle run
```

```bash
gradle run --args='--inputFile=resources/asset-value-mixed-5m.avro --durationUnit=minutes --durationUnitNumber=5'
```

### Test pipeline using input from FILE and output to Clickhouse

```bash
gradle run --args='--inputFile=resources/asset-value-increasing.avro --outputType=clickhouse --tableName=asset_rsi'
```
This command used the default `jdbcUrl` parameter value which is `jdbc:clickhouse://localhost:8123/default?user=default&password=`
Specify a value for the `jdbcUrl` parameter if you want to use a different database or a different user.

For example, the next command uses a database named `asset_db` and a user named `default` with no password
```bash
gradle run --args='--inputFile=resources/asset-value-increasing-mixed.avro --outputType=clickhouse --tableName=asset_rsi --jdbcUrl=jdbc:clickhouse://localhost:8123/asset_db?user=default&password='
```

### Test pipeline using input from Kafka and output to Clickhouse (Work in progress)

Launch the containers
```
docker compose up
```

Launch an interactive terminal on the Kafka broker

```bash
docker container exec -it broker /bin/bash
```

Create the `asset-value` topic

```bash
kafka-topics --create --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --topic asset-value
```

List topics

```bash
kafka-topics --list --bootstrap-server localhost:9092
```

Define schema for the `asset-value` topic.

```bash
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" --data '{"schema": "{\"type\":\"record\",\"name\":\"AssetValue\",\"namespace\":\"com.example\",\"fields\":[{\"name\":\"timestamp\",\"type\":\"string\"},{\"name\":\"asset\",\"type\":\"string\"}, {\"name\":\"value\",\"type\":\"string\"}]}"}' http://schema-registry:8081/subjects/assets-value-value/versions
```

List subjects in schema registry
```bash
curl -X GET http://schema-registry:8081/subjects
```

Find the network of a container (broker)
```
docker inspect broker -f "{{json .NetworkSettings.Networks }}"
```

Remove all containers and images
```
docker rm -vf $(docker ps -aq)
docker rmi -f $(docker images -aq)
```
