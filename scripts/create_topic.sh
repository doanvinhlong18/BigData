#!/bin/bash

kafka-topics \
  --bootstrap-server kafka:9092 \
  --create \
  --topic nyc_taxi_trips \
  --partitions 3 \
  --replication-factor 1

kafka-topics \
  --bootstrap-server kafka:9092 \
  --create \
  --topic nyc_weather_hourly \
  --partitions 1 \
  --replication-factor 1
