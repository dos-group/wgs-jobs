package de.tu_berlin.cit.watergridsense_jobs.utils;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import org.apache.flink.api.java.tuple.Tuple7;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * Contains a complete measurement package as defined in the Interface Control Document
 * cf. https://docs.google.com/document/d/1ssITjH8VvDnouHIaYjMFwnYGCXWZr6u0P4UVSTdn8JU/edit?ts=5d35b501
 */
public class SensorData {

    private static class UnixTimestampDeserializer extends JsonDeserializer<Date> {

        @Override
        public Date deserialize(JsonParser parser, DeserializationContext context) throws IOException {

            String unixTimestamp = parser.getText().trim();
            return new Date(TimeUnit.SECONDS.toMillis(Long.parseLong(unixTimestamp)));
        }
    }

    @JsonProperty("n")
    public String sensorId;
    @JsonDeserialize(using = UnixTimestampDeserializer.class)
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss", timezone="UTC")
    @JsonProperty("t")
    public Date timestamp;
    @JsonProperty("v")
    public double rawValue;
    @JsonProperty("g")
    public String location = "UNKNOWN";
    @JsonProperty("ty")
    public String type = "UNKNOWN";
    @JsonProperty("u")
    public String unit = "UNKNOWN";
    @JsonProperty("c")
    public double conValue = -99D;
    @JsonProperty("r")
    public long gridCell = 999;

    public SensorData() { }

    public SensorData(String sensorId, Date timestamp, double rawValue) {
        this.sensorId = sensorId;
        this.timestamp = timestamp;
        this.rawValue = rawValue;
    }

    public Tuple7<String, Date, String, String, String, Double, Double> toTuple() {
        return new Tuple7<>(sensorId, timestamp, type, location, unit, rawValue, conValue);
    }

    public String toString() {
        return
            "{ " +
            "\"n\":  \"" + sensorId + "\", " +
            "\"t\": " + timestamp.getTime() + ", " +
            "\"v\": " + rawValue + ", " +
            "\"g\": \"" + location + "\", " +
            "\"r\": \"" + gridCell + "\", " +
            "\"ty\": \"" + type + "\", " +
            "\"u\": \"" + unit + "\", " +
            "\"c\": " + conValue +
            " }";
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public String getSensorId() {
        return sensorId;
    }

    public Double getValue() {
        return rawValue;
    }

    public Double getConValue() {
        return conValue;
    }

    public String getSensorLocation() {
        return location;
    }

    public long getGridCell() {
        return gridCell;
    }
}
