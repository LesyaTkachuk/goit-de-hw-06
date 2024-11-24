from kafka_admin import create_producer, kafka_topics
from color_constants import GREEN, RED, YELLOW, END
import uuid
import time
import random

# create Kafka Producer
sensor_data_producer = create_producer()

# define topic name
topic_building_sensors = kafka_topics["topic_building_sensors"]


# simulate the operation of a building sensors
def main():
    # define sensor id
    sensor_id = str(uuid.uuid4())

    for i in range(100):
        # generate data (temperature and humidity)
        data = {
            "sensor_id": sensor_id,
            "temperature": random.randint(0, 50),
            "humidity": random.randint(5, 90),
            "obtained_at": time.time(),
        }

        try:
            # send sensor data into the topic
            sensor_data_producer.send(
                topic=topic_building_sensors, key=sensor_id, value=data
            )
            sensor_data_producer.flush()
            print(
                f"{GREEN}Sensor {YELLOW}{sensor_id} {GREEN}sent the sensor data to topic {topic_building_sensors} successfully.{END}"
            )
            time.sleep(2)
        except KeyboardInterrupt:
            print(f"{RED}Keyboard interrupt received. Exiting...{END}")
            break
        except Exception as e:
            print(
                f"{RED}Error sending sensor data to topic {topic_building_sensors}: {e}{END}"
            )

    sensor_data_producer.close()


if __name__ == "__main__":
    main()
