# kafka/producer/base_producer.py

import time
from datetime import datetime
from kafka import KafkaProducer
import json


class RealtimeReplayProducer:
    def __init__(self, topic, bootstrap_servers, speed_factor=3600):
        self.topic = topic
        self.speed_factor = speed_factor

        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

        self.prev_event_time = None

    def parse_time(self, value):
        return datetime.fromisoformat(value)

    def send_event(self, event, event_time_field):
        event_time = self.parse_time(event[event_time_field])

        if self.prev_event_time:
            delta = (event_time - self.prev_event_time).total_seconds()
            if delta > 0:
                time.sleep(delta / self.speed_factor)

        self.producer.send(self.topic, value=event)
        self.prev_event_time = event_time
