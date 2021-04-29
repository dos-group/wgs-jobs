package de.tu_berlin.cit.watergridsense_jobs.EnrichmentJob;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import de.tu_berlin.cit.watergridsense_jobs.utils.*;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

public class ParsingFunction extends RichFlatMapFunction<JsonObject, SensorData> {

    /**
     *
     */
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = Logger.getLogger(EnrichmentFunction.class);

    @Override
    public void flatMap(JsonObject rawData, Collector<SensorData> out) throws Exception {
    
        String deviceId;
        Date timestamp;
        Double value;
        
        try {
            // WaterGridSense SenML package
            if (rawData.has("n"))  {
                String sensorId = rawData.get("n").getAsString().toLowerCase();
                timestamp = new Date(rawData.get("t").getAsLong()*1000L);
                value = rawData.get("v").getAsDouble();
                out.collect(new SensorData(sensorId, timestamp, value));
            }
            // SCS Atlas package (SENSARE Project)
            else if (rawData.has("externalDeviceId"))  {
                deviceId = rawData.get("externalDeviceId").getAsString().toLowerCase();
                timestamp = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").parse(rawData.get("timestamp").getAsString());

                for (Map.Entry<String, JsonElement> entry : rawData.getAsJsonObject("decoded").entrySet()) {
                    if(entry.getKey() != "Interrupt_status") {
                        String sensorId = deviceId + "-" + entry.getKey();
                        value = entry.getValue().getAsDouble();
                        out.collect(new SensorData(sensorId, timestamp, value));
                    }
                }
            }
            // TTN package
            else if (rawData.has("hardware_serial")) {
                deviceId = rawData.get("hardware_serial").getAsString().toLowerCase();
                timestamp = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").parse(rawData.get("timestamp").getAsString());

                for (Map.Entry<String, JsonElement> entry : rawData.getAsJsonObject("payload_fields").entrySet()) {
                    String sensorId = deviceId + "-" + entry.getKey();
                    value = entry.getValue().getAsDouble();
                    out.collect(new SensorData(sensorId, timestamp, value));
                }
            }
            else {
                LOG.error("Unknown json format of sensor packet: " + rawData.toString());
            }       
        }
        catch (Exception ex) {
            LOG.error("Parsing error: " + ex.getMessage());
        }
    }
}