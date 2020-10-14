package org.hiveeyes.kafka;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

/**
 * The <a href="https://docs.lenses.io/4.0/integrations/connectors/stream-reactor/sources/mqttsourceconnector/">
 *     stream-reactor mqttsourceconnector</a>) adds MQTT topic information into key structure.
 * This transformer extracts topology names from the topic name and adds them to the kafka messages payload.
 * Currently, topology hierarchy names are hardcoded and expected hierarchy depth is 5 (as used in hiveeyes.org)
 * @param <R>
 */
public abstract class Mqtt2KafkaTopologyExtractor<R extends ConnectRecord<R>> implements Transformation<R> {

    private static Logger log = LoggerFactory.getLogger(Mqtt2KafkaTopologyExtractor.class);

    private static final String FIELD_NAME_REALM = "topology_hierarchy_realm";
    private static final String FIELD_NAME_NETWORK = "topology_hierarchy_network";
    private static final String FIELD_NAME_GATEWAY = "topology_hierarchy_gateway";
    private static final String FIELD_NAME_NODE = "topology_hierarchy_node";
    private static final String FIELD_NAME_PAYLOAD_FORMAT = "topology_hierarchy_payload_format";

    public static final ConfigDef CONFIG_DEF = new ConfigDef();

    private static final String PURPOSE = "HiveEyesTransformer";
    private Cache<Schema, Schema> schemaUpdateCache;

    public R apply(R record) {
        if (operatingValue(record) == null) {
            log.info("operatingValue(record) == null");
            return record;
        }
        // schemaless records are not supported
        return applyWithSchema(record);
    }

    private R applyWithSchema(R record) {
        final Struct value = requireStruct(operatingValue(record), PURPOSE + " value");
        final Struct key = requireStruct(record.key(), PURPOSE + " key");

        Schema updatedSchema = schemaUpdateCache.get(value.schema());
        if (updatedSchema == null) {
            updatedSchema = makeUpdatedSchema(value.schema());
            schemaUpdateCache.put(value.schema(), updatedSchema);
        }

        final Struct updatedValue = new Struct(updatedSchema);

        Object topicValue = key.get("topic");
        String[] hierarchyNames;
        if (topicValue == null) {
            log.warn("topic is null");
            hierarchyNames = new String[] {"", "", "", ""};
        } else {
            String topicName = topicValue.toString();
            hierarchyNames = topicName.split("/");
            // TODO: configurable hierarchy names
            if (hierarchyNames.length != 5) {
                throw new ConnectException("topic name does not contain 5 hierarchy levels: " + topicName);
            }
        }

        for (Field field : value.schema().fields()) {
            updatedValue.put(field.name(), value.get(field));
        }

        updatedValue.put(FIELD_NAME_REALM, hierarchyNames[0]);
        updatedValue.put(FIELD_NAME_NETWORK, hierarchyNames[1]);
        updatedValue.put(FIELD_NAME_GATEWAY, hierarchyNames[2]);
        updatedValue.put(FIELD_NAME_NODE, hierarchyNames[3]);
        updatedValue.put(FIELD_NAME_PAYLOAD_FORMAT, hierarchyNames[4]);

        return newRecord(record, updatedSchema, updatedValue);
    }

    private Schema makeUpdatedSchema(Schema schema) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

        for (Field field : schema.fields()) {
            builder.field(field.name(), field.schema());
        }

        builder.field(FIELD_NAME_REALM, Schema.STRING_SCHEMA);
        builder.field(FIELD_NAME_NETWORK, Schema.STRING_SCHEMA);
        builder.field(FIELD_NAME_GATEWAY, Schema.STRING_SCHEMA);
        builder.field(FIELD_NAME_NODE, Schema.STRING_SCHEMA);
        builder.field(FIELD_NAME_PAYLOAD_FORMAT, Schema.STRING_SCHEMA);

        return builder.build();
    }


    public ConfigDef config() {
        return CONFIG_DEF;
    }

    public void close() {

    }

    @Override
    public void configure(Map<String, ?> props) {
        schemaUpdateCache = new SynchronizedCache<Schema, Schema>(new LRUCache<Schema, Schema>(16));
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static class Value<R extends ConnectRecord<R>> extends Mqtt2KafkaTopologyExtractor<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }

    }
}
