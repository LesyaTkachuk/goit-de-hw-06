from kafka.admin import KafkaAdminClient
from kafka import KafkaProducer, KafkaConsumer
from conf.configs import kafka_config, kafka_topic_config
import json

# create Kafka client
admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
)


# create Kafka Producer
def create_producer():

    return KafkaProducer(
        bootstrap_servers=kafka_config["bootstrap_servers"],
        security_protocol=kafka_config["security_protocol"],
        sasl_mechanism=kafka_config["sasl_mechanism"],
        sasl_plain_username=kafka_config["username"],
        sasl_plain_password=kafka_config["password"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


# create Kafka Consumer
def create_consumer(group_id):

    return KafkaConsumer(
        bootstrap_servers=kafka_config["bootstrap_servers"],
        security_protocol=kafka_config["security_protocol"],
        sasl_mechanism=kafka_config["sasl_mechanism"],
        sasl_plain_username=kafka_config["username"],
        sasl_plain_password=kafka_config["password"],
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        # key_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",  # to start reading from the beginning
        enable_auto_commit=True,  # automatically commit message reading
        group_id=group_id,  # consumers group id
    )


# "sensor_data_consumers_group"
kafka_topics = {
    "topic_building_sensors": f"{kafka_topic_config['user_name']}_building_sensors",
    "topic_kafka_alerts": f"{kafka_topic_config['user_name']}_kafka_alerts",
}
