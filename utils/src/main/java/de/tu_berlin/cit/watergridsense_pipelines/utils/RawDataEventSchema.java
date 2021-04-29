package de.tu_berlin.cit.watergridsense_jobs.utils;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.log4j.Logger;

// This event schema reads arbitrary json-encoded sensor data packets from input Strings
// and produces JsonObjects for further processing
public class RawDataEventSchema implements DeserializationSchema<JsonObject>, SerializationSchema<JsonObject> {

    private static final Logger LOG = Logger.getLogger(SensorDataEventSchema.class);

    private static JsonObject fromString(String line) {
        try {
            JsonObject jsonLine = JsonParser.parseString(line).getAsJsonObject();
            return jsonLine;
        }
        catch (Exception ex) {
            LOG.error("Parsing error: " + ex.getMessage());
            LOG.error("Original data: " + line);
        }
        return null;
    }

    @Override
    public JsonObject deserialize(byte[] message) {
        return fromString(new String(message));
    }

    @Override
    public boolean isEndOfStream(JsonObject nextElement) {
        return false;
    }

    @Override
    public byte[] serialize(JsonObject element) {
        return element.toString().getBytes();
    }

    @Override
    public TypeInformation<JsonObject> getProducedType() {
        return TypeExtractor.getForClass(JsonObject.class);
    }
}

