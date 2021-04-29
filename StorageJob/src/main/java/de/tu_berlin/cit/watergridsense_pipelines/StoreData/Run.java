package de.tu_berlin.cit.watergridsense_jobs.StorageJob;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.JdkSSLOptions;
import com.datastax.driver.core.Cluster.Builder;

import java.io.IOException;
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
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
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import de.tu_berlin.cit.watergridsense_jobs.utils.*;

public class Run {

    private static final Logger LOG = Logger.getLogger(Run.class);

    public static void main(String[] args) throws Exception {

        BasicConfigurator.configure(); // configure logger
        Properties props = FileReader.GET.read("storage_job.properties", Properties.class);
        ParameterTool parameters = ParameterTool.fromArgs(args);
        String cassandraHosts = props.getProperty("cassandra.hosts");
        int cassandraPort = Integer.parseInt(props.getProperty("cassandra.port"));
        String keySpace = props.getProperty("cassandra.keyspace");
        String username = props.getProperty("cassandra.user");
        String password = props.getProperty("cassandra.password");

        // Prepare storage ****************************************************************************************

        // setup cassandra cluster builder for sinks
        ClusterBuilder builder = new ClusterBuilder() {

            @Override
            protected Cluster buildCluster(Builder builder) {
                SSLContext sslcontext = null;
                try {
                    InputStream is = Run.class.getResourceAsStream("/cassandra.truststore.jks");
                    KeyStore keystore = KeyStore.getInstance("jks");
                    char[] pwd = props.getProperty("cassandra.truststore.pass").toCharArray();
                    keystore.load(is, pwd);
            
                    TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                    tmf.init(keystore);
                    TrustManager[] tm = tmf.getTrustManagers();
            
                    sslcontext = SSLContext.getInstance("TLS");
                    sslcontext.init(null, tm, null);
                } catch (KeyStoreException kse) {
                    LOG.error(kse.getMessage(), kse);
                } catch (CertificateException e) {
                    LOG.error(e.getMessage(), e);
                } catch (NoSuchAlgorithmException e) {
                    LOG.error(e.getMessage(), e);
                } catch (KeyManagementException e) {
                    LOG.error(e.getMessage(), e);
                } catch (IOException e) {
                    LOG.error(e.getMessage(), e);
                }
        
                JdkSSLOptions sslOptions = JdkSSLOptions.builder()
                    .withSSLContext(sslcontext)
                    .build();

                return builder
                    .addContactPoints(cassandraHosts.split(","))
                    .withPort(cassandraPort)
                    .withCredentials(username, password)
                    .withSSL(sslOptions)
                    .build();
            }
        };

        // delete old data if fresh start is requested
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

        // kafka configuration
        Properties kafkaConsumerProps = new Properties();
        kafkaConsumerProps.setProperty("bootstrap.servers", props.getProperty("kafka.brokers")); // Broker default host:port
        kafkaConsumerProps.setProperty("group.id", props.getProperty("kafka.consumer.group"));   // Consumer group ID
        if (parameters.has("latest")) {
            kafkaConsumerProps.setProperty("auto.offset.reset", "latest");                          
        }
        else {
            kafkaConsumerProps.setProperty("auto.offset.reset", "earliest");
        }
        Properties kafkaProducerProps = new Properties();
        kafkaProducerProps.setProperty("bootstrap.servers", props.getProperty("kafka.brokers")); // Broker default host:port
        kafkaProducerProps.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,"900000");
        kafkaProducerProps.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5");
        kafkaProducerProps.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG,UUID.randomUUID().toString());
        kafkaProducerProps.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        kafkaProducerProps.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        kafkaProducerProps.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1000");
        kafkaProducerProps.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.disableOperatorChaining();

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
        //env.setParallelism(numPartitions);

        // configure event-time and watermarks
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000L);

        // Setup input streams ******************************************************************************************

        FlinkKafkaConsumer<SensorData> richConsumer =
            new FlinkKafkaConsumer<>(
                props.getProperty("kafka.topic.rich"),
                new SensorDataEventSchema(),
                kafkaConsumerProps);

        FlinkKafkaConsumer<ParamData> parameterConsumer =
            new FlinkKafkaConsumer<>(
                props.getProperty("kafka.topic.updates"),
                new ParamDataEventSchema(),
                kafkaConsumerProps);

        FlinkKafkaConsumer<DataAlert> alarmConsumer =
            new FlinkKafkaConsumer<>(
                props.getProperty("kafka.topic.alarms"),
                new DataAlertEventSchema(),
                kafkaConsumerProps);

        // assign a timestamp extractor to the consumer
        richConsumer.assignTimestampsAndWatermarks(new SensorDataTSExtractor(Time.seconds(60)));
        parameterConsumer.assignTimestampsAndWatermarks(new ParamDataTSExtractor(Time.seconds(60)));
        //alarmConsumer.assignTimestampsAndWatermarks(new ParamDataTSExtractor(Time.seconds(60)));

        // create kafka sensor measurement stream
        DataStream<SensorData> richSensorDataStream = env
            .addSource(richConsumer)
            .name("Kafka enriched sensor data consumer")
            .setParallelism(Integer.parseInt(props.getProperty("kafka.partitions")));

        // create kafka parameter update stream
        DataStream<ParamData> parameterDataStream = env
            .addSource(parameterConsumer)
            .name("Kafka parameter update consumer")
            .setParallelism(Integer.parseInt(props.getProperty("kafka.partitions")));

        // create kafka alarms stream
        DataStream<DataAlert> alarmDataStream = env
            .addSource(alarmConsumer)
            .name("Kafka alarm consumer")
            .setParallelism(Integer.parseInt(props.getProperty("kafka.partitions")));

        // Write to sinks ******************************************************************************************

        // write enriched sensor data stream to cassandra archive
        DataStream<Tuple7<String, Date, String, String, String, Double, Double>> tupleEnrichedSensorStream = richSensorDataStream
            .map((MapFunction<SensorData, Tuple7<String, Date, String, String, String, Double, Double>>) SensorData::toTuple);
        CassandraSink.addSink(tupleEnrichedSensorStream)
            .setClusterBuilder(builder)
            .setQuery(
                "INSERT INTO " + keySpace + ".measurements(sensorid, timestamp, type, unit, location, rawValue, conValue) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS;")
            .build()
            .setParallelism(1);

        // write parameter update stream to cassandra archive
        DataStream<Tuple4<String, Date, String, String>> tupleParameterStream = parameterDataStream
            .map((MapFunction<ParamData, Tuple4<String, Date, String, String>>) ParamData::toTuple);
        CassandraSink.addSink(tupleParameterStream)
            .setClusterBuilder(builder)
            .setQuery(
                "INSERT INTO " + keySpace + ".parameters(sensorid, timestamp, parameter, value) " +
                "VALUES (?, ?, ?, ?) IF NOT EXISTS;")
            .build()
            .setParallelism(1);

        // store alerts in `alarms` table
        DataStream<Tuple3<String, Date, String>> tupleAlarmStream = alarmDataStream
            .map((MapFunction<DataAlert, Tuple3<String, Date, String>>) DataAlert::toTuple);
        CassandraSink.addSink(tupleAlarmStream)
            .setClusterBuilder(builder)
            .setQuery(
                "INSERT INTO " + keySpace + ".alarms(sensorid, timestamp, message) " +
                "VALUES (?, ?, ?) IF NOT EXISTS;")
            .build()
            .setParallelism(1);

        env.execute("WGS4_StorageJob");
    }
}
