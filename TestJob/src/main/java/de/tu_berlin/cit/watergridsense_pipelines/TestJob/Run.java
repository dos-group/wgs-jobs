package de.tu_berlin.cit.watergridsense_jobs.TestJob;

import java.util.Date;
import java.util.Properties;
import org.apache.log4j.Logger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;

import de.tu_berlin.cit.watergridsense_jobs.utils.*;

public class Run {

    private static final Logger LOG = Logger.getLogger(Run.class);

    public static void main(String[] args) throws Exception {

        Properties props = FileReader.GET.read("test_job.properties", Properties.class);
        ParameterTool parameters = ParameterTool.fromArgs(args);

        // setup cassandra keyspace and tables if they dont exist 
        String cassandraHosts = props.getProperty("cassandra.hosts");
        int cassandraPort = Integer.parseInt(props.getProperty("cassandra.port"));
        String keySpace = props.getProperty("cassandra.keyspace");
        String username = props.getProperty("cassandra.user");
        String password = props.getProperty("cassandra.password");
        
        // setup cassandra cluster builder
        ClusterBuilder builder = new ClusterBuilder() {

            @Override
            protected Cluster buildCluster(Builder builder) {
                return builder
                    .addContactPoints(cassandraHosts.split(","))
                    .withPort(cassandraPort)
                    .withCredentials(username, password)
                    .build();
            }
        };
        
        // default behavior is to seed the db
        if (parameters.has("seed") || parameters.has("fresh")) {
            // setup cassandra
            if (parameters.has("fresh")) {
                Cassandra.execute(
                "DROP KEYSPACE IF EXISTS " + keySpace + ";",
                    builder);
            }

            Cassandra.execute(
                "CREATE KEYSPACE IF NOT EXISTS " + keySpace + " WITH replication = {" +
                    "'class':'SimpleStrategy', " +
                    "'replication_factor':3};",
                builder);

            Cassandra.execute(
                "CREATE TABLE IF NOT EXISTS " + keySpace + ".measurements(" +
                    "sensorid TEXT, " +
                    "timestamp TIMESTAMP, " +
                    "type TEXT, " +
                    "unit TEXT, " +
                    "location TEXT, " +
                    "rawValue DOUBLE, " +
                    "conValue DOUBLE, " +
                    "PRIMARY KEY (sensorid, timestamp)) " +
                    "WITH CLUSTERING ORDER BY (timestamp DESC);",
                builder);

            Cassandra.execute(
                "CREATE TABLE IF NOT EXISTS " + keySpace + ".parameters(" +
                    "sensorid TEXT, " +
                    "timestamp TIMESTAMP, " +
                    "parameter TEXT, " +
                    "value TEXT, " +
                    "PRIMARY KEY (sensorid, parameter, timestamp)) " +
                    "WITH CLUSTERING ORDER BY (parameter ASC, timestamp DESC);",
                builder);

            Cassandra.execute(
                "CREATE TABLE IF NOT EXISTS " + keySpace + ".alarms(" +
                    "sensorid TEXT, " +
                    "timestamp TIMESTAMP, " +
                    "message TEXT, " +
                    "PRIMARY KEY (sensorid, timestamp)) " +
                    "WITH CLUSTERING ORDER BY (timestamp DESC);",
                builder);
            
            Cassandra.close();
        }

        // kafka configuration
        Properties kafkaConsumerProps = new Properties();
        kafkaConsumerProps.setProperty("bootstrap.servers", props.getProperty("kafka.brokers")); // Broker default host:port
        kafkaConsumerProps.setProperty("group.id", props.getProperty("kafka.consumer.group"));   // Consumer group ID
        kafkaConsumerProps.setProperty("auto.offset.reset", "latest");                           // Always read topic from start
        Properties kafkaProducerProps = new Properties();
        kafkaProducerProps.setProperty("bootstrap.servers", props.getProperty("kafka.brokers")); // Broker default host:port
        kafkaProducerProps.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,"900000");
        kafkaProducerProps.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG,"my-transaction");
        kafkaProducerProps.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        kafkaProducerProps.setProperty(ProducerConfig.ACKS_CONFIG, "0");
        kafkaProducerProps.setProperty(ProducerConfig.RETRIES_CONFIG, "0");
        kafkaProducerProps.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        kafkaProducerProps.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1000");
        kafkaProducerProps.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");

        // Start configurations ****************************************************************************************

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // setup RocksDB state backend
        String fileBackend = props.getProperty("hdfs.host") + props.getProperty("hdfs.path");
        RocksDBStateBackend backend = new RocksDBStateBackend(fileBackend, true);
        env.setStateBackend((StateBackend) backend);

        // start a checkpoint based on configuration property
        int checkpointInterval = Integer.parseInt(props.getProperty("flink.checkpointInterval"));
        env.enableCheckpointing(checkpointInterval);

        // set mode to exactly-once (this is the default)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // checkpoints have to complete within one minute, or are discarded
        env.getCheckpointConfig().setCheckpointTimeout(300000);

        // enable externalized checkpoints which are deleted after job cancellation
        env.getCheckpointConfig().enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);

        // allow job recovery fallback to checkpoint when there is a more recent savepoint
        env.getCheckpointConfig().setPreferCheckpointForRecovery(true);

        // configure parallelism (done on job submission)
        // env.setParallelism(Integer.parseInt(props.getProperty("cassandra.partitions")));

        // End configurations ******************************************************************************************

        // configure event-time and watermarks
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000L);

        DataStream<SensorData> rawSensorDataStream;
        DataStream<ParamData> paramUpdateDataStream;

        FlinkKafkaConsumer<SensorData> rawConsumer =
            new FlinkKafkaConsumer<>(
                props.getProperty("kafka.topic.raw"),
                new SensorDataEventSchema(),
                kafkaConsumerProps);

        FlinkKafkaConsumer<ParamData> paramUpdateConsumer =
            new FlinkKafkaConsumer<>(
                props.getProperty("kafka.topic.updates"),
                new ParamDataEventSchema(),
                kafkaConsumerProps);

        // assign a timestamp extractor to the consumer
        //rawConsumer.assignTimestampsAndWatermarks(new SensorDataTSExtractor(Time.seconds(60)));

        // create kafka sensor measurement stream
        rawSensorDataStream = env.addSource(rawConsumer)
                .setParallelism(Integer.parseInt(props.getProperty("kafka.partitions")));

        // create kafka parameter update stream
        paramUpdateDataStream = env.addSource(paramUpdateConsumer)
            .setParallelism(Integer.parseInt(props.getProperty("kafka.partitions")));

        // log and "fake enrich" sensor data stream
        DataStream<SensorData> enrichedSensorDataStream =
            rawSensorDataStream.map((MapFunction<SensorData, SensorData>) sensorData -> {
                sensorData.location="HERE";
                sensorData.type="WGS_DEV";
                sensorData.conValue=sensorData.rawValue;
                LOG.info(sensorData.toString());
                return sensorData;
            });

        // log parameter updates stream
        DataStream<ParamData> processedUpdateDataStream =
            paramUpdateDataStream.map((MapFunction<ParamData, ParamData>) paramData -> {
            LOG.info(paramData.toString());
            return paramData;
        });
        
        // write enriched sensor data stream to broker
        FlinkKafkaProducer<SensorData> richProducer =
            new FlinkKafkaProducer<>(
                props.getProperty("kafka.topic.rich"),
                (KafkaSerializationSchema<SensorData>) (sensorData, aLong) -> {
                    return new ProducerRecord<>(props.getProperty("kafka.topic.rich"), sensorData.toString().getBytes());
                },
                kafkaProducerProps,
                Semantic.EXACTLY_ONCE);

        // write back to kafka output topic
        enrichedSensorDataStream.addSink(richProducer);

        // convert stream of enriched sensor data to tuple for cassandra sink
        DataStream<Tuple7<String, Date, String, String, String, Double, Double>> tupleEnrichedSensorStream =
            enrichedSensorDataStream.map((MapFunction<SensorData, Tuple7<String, Date, String, String, String, Double, Double>>) SensorData::toTuple);

        // write to archive
        CassandraSink.addSink(tupleEnrichedSensorStream)
            .setClusterBuilder(builder)
            .setQuery(
                "INSERT INTO " + keySpace + ".measurements(sensorid, timestamp, type, location, rawValue, conValue) " +
                "VALUES (?, ?, ?, ?, ?, ?);")
            .build();

        env.execute("WGS Enrichment - TestJob");
    }
}
