package de.tu_berlin.cit.watergridsense_jobs.ArimaJob;

import de.tu_berlin.cit.watergridsense_jobs.utils.*;

import com.workday.insights.timeseries.arima.Arima;
import com.workday.insights.timeseries.arima.struct.ForecastResult;
import com.workday.insights.timeseries.arima.struct.ArimaParams;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;

import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;

public class Run {

    private static final Logger LOG = Logger.getLogger(Run.class);

    public static void main(String[] args) throws Exception {

        Properties props = FileReader.GET.read("arima_job.properties", Properties.class);
        ParameterTool parameters = ParameterTool.fromArgs(args);

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

        // Start configurations ****************************************************************************************

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // setup RocksDB state backend
        String fileBackend = props.getProperty("hdfs.host") + props.getProperty("hdfs.path");
        RocksDBStateBackend backend = new RocksDBStateBackend(fileBackend, true);
        env.setStateBackend(backend);

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

        // Setup input streams ****************************************************************

        DataStream<SensorData> richSensorDataStream;

        // create kafka sensor measurement stream
        FlinkKafkaConsumer<SensorData> richConsumer =
            new FlinkKafkaConsumer<>(
                props.getProperty("kafka.topic.rich"),
                new SensorDataEventSchema(),
                kafkaConsumerProps);
        richConsumer.assignTimestampsAndWatermarks(new SensorDataTSExtractor(Time.seconds(60)));
        richSensorDataStream = env
            .addSource(richConsumer)
            .name("Kafka sensor data consumer")
            .keyBy(SensorData::getSensorId);

        // create kafka alarm stream
        FlinkKafkaProducer<DataAlert> alarmProducer =
            new FlinkKafkaProducer<>(
                props.getProperty("kafka.topic.alarms"),
                (KafkaSerializationSchema<DataAlert>) (DataAlert, aLong) -> {
                    return new ProducerRecord<>(props.getProperty("kafka.topic.alarms"), DataAlert.toString().getBytes());
                },
                kafkaProducerProps,
                Semantic.EXACTLY_ONCE);

        // configure event-time and watermarks
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000L);

        // Continue to look for suspicious behaviour on the fully enhanced data
        DataStream<DataAlert> alarmStream = richSensorDataStream
            .keyBy(SensorData::getSensorId)
            // For the 5m interval, this 2h window will contain 24 values, prediction uses one less
            // Run prediction for every value, so do 5m window size
            .window(SlidingEventTimeWindows.of(Time.hours(2), Time.minutes(5)))
            .allowedLateness(Time.minutes(3))
            .process(new ArimaProcessWindowFunction());

        // write alerts to appropriate topic
        alarmStream.addSink(alarmProducer);

        env.execute("WGS Enrichment - ArimaJob");
    }

    private static class ArimaProcessWindowFunction extends ProcessWindowFunction<SensorData, DataAlert, Long, TimeWindow> {
        /**
         * Processes a completed window of values by using all values except the last to predict the last one
         * If the last value does not fall within the prediction confidence interval, an alert is raised.
         *
         * @param key The key that the windows were filtered by, for us, this is the sensor id.
         * @param context Window context.
         * @param elements The collected SensorData iterator.
         * @param out Where we put our alerts such that the window data stream emits them.
         */
        @Override
        public void process(String key, Context context, Iterable<SensorData> elements, Collector<DataAlert> out) throws Exception {
            ArrayList<Double> list = new ArrayList<>();
            Date lastTimestamp = null;
            for(SensorData elem : elements) {
                list.add(elem.conValue);
                lastTimestamp = elem.getTimestamp();
            }

            if (lastTimestamp == null || list.size() <= 1) {
                return;
            }

            Double last = list.remove(list.size() - 1);

            double[] array = list.stream().mapToDouble(Double::doubleValue).toArray();
            ArimaParams params = new ArimaParams(1, 1, 1, 1, 0, 0, 0);
            ForecastResult result = Arima.forecast_arima(array, 1, params);

            double lower = result.getForecastLowerConf()[0];
            double higher = result.getForecastUpperConf()[0];

            if (last < lower || last > higher) {
                out.collect(new DataAlert(key, lastTimestamp, last, lower, higher));
            }
        }
    }
}
