# hiveeyes-mqtt-kafka-connect-transform

As the <a href="https://docs.lenses.io/4.0/integrations/connectors/stream-reactor/sources/mqttsourceconnector/"> stream-reactor mqtt source connector</a> adds MQTT topic names to key struct, the Kafka Connect transform _Mqtt2KafkaTopologyExtractor_ extracts <a href="https://getkotori.org/docs/handbook/mqttkit.html">Kotori topology names</a> from the given topic name and adds them to the kafka message payload.
