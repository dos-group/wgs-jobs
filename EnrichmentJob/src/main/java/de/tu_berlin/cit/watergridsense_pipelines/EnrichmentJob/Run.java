package de.tu_berlin.cit.watergridsense_jobs.EnrichmentJob;

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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.JdkSSLOptions;
import com.datastax.driver.core.Cluster.Builder;
import com.google.gson.JsonObject;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import de.tu_berlin.cit.watergridsense_jobs.utils.*;

public class Run {

    private static final Logger LOG = Logger.getLogger(Run.class);

    public static void main(String[] args) throws Exception {

        BasicConfigurator.configure(); // configure logger
        Properties props = FileReader.GET.read("enrichment_job.properties", Properties.class);
        ParameterTool parameters = ParameterTool.fromArgs(args);

        // cassandra general config
        // TODO handle non-ssl cassandra connections?
        String cassandraHosts = props.getProperty("cassandra.hosts");
        int cassandraPort = Integer.parseInt(props.getProperty("cassandra.port"));
        String keySpace = props.getProperty("cassandra.keyspace");
        String username = props.getProperty("cassandra.user");
        String password = props.getProperty("cassandra.password");

        // setup cassandra cluster builder for keyspace creation
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
        
        // Initialize Cassandra ****************************************************************

        // program parameter "initialize" creates keyspace and table
        if (parameters.has("initialize")) {
            Cassandra.execute(
            "CREATE KEYSPACE IF NOT EXISTS " + keySpace + " WITH replication = {" +
                    "'class':'SimpleStrategy', " +
                    "'replication_factor':3};",
                builder);

            // setup cassandra
            Cassandra.execute(
                "CREATE TABLE IF NOT EXISTS " + keySpace + ".parameters(" +
                        "sensorid TEXT, " +
                        "timestamp TIMESTAMP, " +
                        "parameter TEXT, " +
                        "value TEXT, " +
                        "PRIMARY KEY (sensorid, parameter, timestamp)) " +
                        "WITH CLUSTERING ORDER BY (parameter ASC, timestamp DESC);",
                builder);
        }

        // Configure connections ****************************************************************

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

        // Configure Flink ****************************************************************

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // simplify visual presentation of execution graph
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

        // configure event-time and watermarks
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000L);

        // Setup input streams ****************************************************************

        DataStream<SensorData> sensorDataStream;

        // create kafka sensor measurement stream
        FlinkKafkaConsumer<JsonObject> rawConsumer =
            new FlinkKafkaConsumer<>(
                props.getProperty("kafka.topic.raw"),
                new RawDataEventSchema(),
                kafkaConsumerProps);
        //rawConsumer.assignTimestampsAndWatermarks(new SensorDataTSExtractor(Time.seconds(60)));
        sensorDataStream = env
            .addSource(rawConsumer)
            .name("Kafka raw sensor data consumer")
            .flatMap(new ParsingFunction())
            .name("Sensor data parsing function");

        // create kafka parameter update stream
        FlinkKafkaConsumer<ParamData> paramUpdateConsumer =
            new FlinkKafkaConsumer<>(
                props.getProperty("kafka.topic.updates"),
                new ParamDataEventSchema(),
                kafkaConsumerProps);
        //paramUpdateConsumer.assignTimestampsAndWatermarks(new ParamDataTSExtractor(Time.seconds(60)));
        DataStream<ParamData> paramUpdateDataStream = env
            .addSource(paramUpdateConsumer)
            .name("Kafka parameter update consumer")
            .keyBy(ParamData::getSensorId);

        // Enrichment ****************************************************************

        // connect and enrich sensor data with parameter data
        DataStream<SensorData> enrichedSensorDataStream = sensorDataStream
            .keyBy(SensorData::getSensorId)
            .connect(paramUpdateDataStream)
            .flatMap(new EnrichmentFunction())
            .name("Enrichment Function");

        // Write to sinks ****************************************************************
        
        FlinkKafkaProducer<SensorData> richProducer =
            new FlinkKafkaProducer<>(
                props.getProperty("kafka.topic.rich"),
                (KafkaSerializationSchema<SensorData>) (sensorData, aLong) -> {
                    return new ProducerRecord<>(props.getProperty("kafka.topic.rich"), sensorData.toString().getBytes());
                },
                kafkaProducerProps,
                Semantic.EXACTLY_ONCE);

        // write back to kafka output topic
        enrichedSensorDataStream
            .addSink(richProducer)
            .name("Kafka enriched sensor data sink");

        // store updates in `parameters` table
        // Note: Cassandra sinks have parallelism=1 to avoid session exhaustion
        DataStream<Tuple4<String, Date, String, String>> archiveParamUpdateStream =
            paramUpdateDataStream
            .map((MapFunction<ParamData, Tuple4<String, Date, String, String>>) ParamData::toTuple)
            .setParallelism(1)
            .name("Cassandra archive parameters");
        CassandraSink.addSink(archiveParamUpdateStream)
            .setClusterBuilder(builder)
            .setQuery(
                "INSERT INTO " + keySpace + ".parameters(sensorid, timestamp, parameter, value) " +
                "VALUES (?, ?, ?, ?);")
            .build()
            .setParallelism(1);    

        env.execute("WGS4_EnrichmentJob");
    }
}
