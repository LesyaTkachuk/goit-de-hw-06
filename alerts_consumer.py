from kafka_admin import create_consumer, kafka_topics
from color_constants import GREEN, YELLOW,BLUE, RED, END

# create Kafka Consumer
alerts_consumer = create_consumer("alerts_consumers_group-hw-06")

# get topic name for subscription
topic_kafka_alerts = kafka_topics["topic_kafka_alerts"]



def main():
    # subscribe to topic_temperature_alerts and topic_humidity_alerts
    alerts_consumer.subscribe([topic_kafka_alerts])
    print(f"{GREEN}Subscribed to {topic_kafka_alerts}{END}")

    try:
        for message in alerts_consumer:
            print(
                f"{BLUE}Received alert: {YELLOW}{message.value['message']}, {BLUE}Temperature: {message.value['avg_temperature']}, Humidity: {message.value['avg_humidity']}, Time: {message.value['timestamp']}{END}"
            )

    except KeyboardInterrupt:
        print(f"{RED}Keyboard interrupt received. Exiting...{END}")
    except Exception as e:
        print(f"{RED}Error receiving alert: {e}{END}")
    finally:
        alerts_consumer.close()


if __name__ == "__main__":
    main()
