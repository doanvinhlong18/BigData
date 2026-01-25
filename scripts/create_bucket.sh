#!/bin/sh

mc alias set local http://minio:9000 minioadmin minioadmin

mc mb local/bronze
mc mb local/silver

mc policy set public local/bronze
mc policy set public local/silver
