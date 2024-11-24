from kafka.admin import NewTopic
from conf.configs import kafka_topic_config
from kafka_admin import admin_client, kafka_topics
from color_constants import GREEN, RED, LIGHT_RED, LIGHT_GREEN, END

# define new topics
topic_building_sensors = kafka_topics["topic_building_sensors"]
topic_kafka_alerts = kafka_topics["topic_kafka_alerts"]



# create new topics
new_topic_building_sensors = NewTopic(
    name=topic_building_sensors,
    num_partitions=kafka_topic_config["num_partions"],
    replication_factor=kafka_topic_config["replication_factor"],
)

new_topic_kafka_alerts = NewTopic(
    name=topic_kafka_alerts,
    num_partitions=kafka_topic_config["num_partions"],
    replication_factor=kafka_topic_config["replication_factor"],
)


# create new topics
def main():
    try:
        admin_client.create_topics(
            new_topics=[
                new_topic_building_sensors,
                new_topic_kafka_alerts
            ],
            validate_only=False,
        )
        print(
            f"{GREEN}Topics {LIGHT_GREEN}{topic_building_sensors}, {new_topic_kafka_alerts}{END} {GREEN}created successfully.{END}"
        )
    except Exception as e:
        print(f"{LIGHT_RED}Error creating topics:{END} {RED}{e}{END} ")

    # to check if topics are created
    [
        print(LIGHT_GREEN, topic, END)
        for topic in admin_client.list_topics()
        if kafka_topic_config["user_name"] in topic
    ]


if __name__ == "__main__":
    main()
