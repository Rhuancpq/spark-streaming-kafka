from essential_generators import DocumentGenerator
from kafka import KafkaProducer
from kafka.errors import KafkaError
import time


if __name__ == "__main__":
    producer = KafkaProducer(bootstrap_servers=["kafka:9092"])
    gen = DocumentGenerator()

    while True:
        try:
            message = gen.sentence()
            producer.send("data-topic", message.encode("utf-8"))
            print("Sent: ", message)
            time.sleep(0.5)
        except KafkaError as e:
            print(e)
            break
