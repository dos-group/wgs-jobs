package de.tu_berlin.cit.watergridsense_jobs.NeighborJob;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import de.tu_berlin.cit.watergridsense_jobs.utils.*;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.Collection;

public class NeighborFunction extends RichFlatMapFunction<SensorData, DataAlert> {

    private static final Logger LOG = Logger.getLogger(Run.class);
    private transient Map<Long, Map<Long, Double>> neighborhoods;
    private double threshold = 10.0; // threshold for mean deviation

    @Override
    public void open(final Configuration config) throws Exception {
        // configure logger
        BasicConfigurator.configure();
        neighborhoods = new HashMap<Long, Map<Long, Double>>();
    }
    
    @Override
    public void close() throws Exception {
        super.close();
    }

    // TODO this is garbo, doesn't make sense at all
    @Override
    public void flatMap(SensorData sensorData, Collector<DataAlert> out) throws Exception {
        String sensorid = sensorData.getSensorId();
        double sensorValue = sensorData.conValue;

        // create new map if first sensor from this cell
        if (!neighborhoods.containsKey(sensorid)) {
            // I am lost v~~>_>~~v
            Map<String, Double> cell = new HashMap<String, Double>();
            cell.put(sensorid, sensorValue);
        }

        // get all values from cell neighbors and compute mean
        Map<Long, Double> cell = neighborhoods.get(sensorid);
        Collection<Double> cellValues = cell.values();
        double[] cellValueArray = new double[cellValues.size()];
        int i = 0;
        for (Double oneValue : cellValues) {
            cellValueArray[i] = oneValue.doubleValue();
            i = i + 1;
        }
        //double[] cellValueArray = cellValues.toArray(new double[0]);
        double cellMean = mean(cellValueArray);
        double difference = Math.abs(cellMean - sensorValue);
        // create alert if value is further away from mean than defined threshold
        if (difference > threshold) {
            out.collect(new DataAlert(sensorid, new Timestamp(System.currentTimeMillis()), sensorValue, cellMean - threshold, cellMean + threshold));
        }
    }

    // custom mean function because java.math mean is overshoot
    public static double mean(double[] m) {
        double sum = 0;
        for (int i = 0; i < m.length; i++) {
            sum += m[i];
        }
        return sum / m.length;
    }
}