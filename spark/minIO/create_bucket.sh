#!/bin/bash

mc alias set local http://minio:9000 minioadmin minioadmin

mc mb -p local/bronze
mc mb -p local/silver
mc mb -p local/gold
