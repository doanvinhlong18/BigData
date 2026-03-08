#!/bin/bash

set -e

MASTER="spark://spark-master:7077"
CONTAINER="spark-master"

EXECUTOR_MEM="1G"
EXECUTOR_CORES="1"
DRIVER_MEM="1G"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}╔═══════════════════════════════════════════════╗${NC}"
echo -e "${YELLOW}║   SUBMITTING SILVER LAYER STREAMING JOBS      ║${NC}"
echo -e "${YELLOW}╔═══════════════════════════════════════════════╗${NC}"
echo ""

###############################################################################
# 1. REQUEST CLEAN
###############################################################################
echo -e "${GREEN}[1/3] Submitting request_clean (stream-stream join)...${NC}"

docker exec $CONTAINER \
/opt/spark/bin/spark-submit \
  --master $MASTER \
  --deploy-mode client \
  --executor-memory $EXECUTOR_MEM \
  --executor-cores $EXECUTOR_CORES \
  --total-executor-cores $EXECUTOR_CORES \
  --driver-memory $DRIVER_MEM \
  --conf "spark.sql.streaming.stateStore.providerClass=org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider" \
  --conf "spark.sql.streaming.stateStore.minDeltasForSnapshot=10" \
  --conf "spark.sql.shuffle.partitions=4" \
  --packages io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  /opt/spark/app/spark/jobs/request_bronze_to_silver.py &

REQUEST_PID=$!
echo -e "${GREEN}✓ request_clean submitted (PID: $REQUEST_PID)${NC}"
echo ""

sleep 5

###############################################################################
# 2. WEATHER CLEAN
###############################################################################
echo -e "${GREEN}[2/3] Submitting weather_clean (streaming deduplication)...${NC}"

docker exec $CONTAINER \
/opt/spark/bin/spark-submit \
  --master $MASTER \
  --deploy-mode client \
  --executor-memory 1G \
  --executor-cores $EXECUTOR_CORES \
  --total-executor-cores  $EXECUTOR_CORES \
  --driver-memory $DRIVER_MEM \
  --conf "spark.sql.streaming.stateStore.providerClass=org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider" \
  --conf "spark.sql.streaming.stateStore.minDeltasForSnapshot=10" \
  --conf "spark.sql.shuffle.partitions=4" \
  --packages io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  /opt/spark/app/spark/jobs/weather_bronze_to_silver.py &

WEATHER_PID=$!
echo -e "${GREEN}✓ weather_clean submitted (PID: $WEATHER_PID)${NC}"
echo ""

sleep 5

###############################################################################
# 3. TAXI CLEAN
###############################################################################
echo -e "${GREEN}[3/3] Submitting taxi_clean (streaming deduplication)...${NC}"

docker exec $CONTAINER \
/opt/spark/bin/spark-submit \
  --master $MASTER \
  --deploy-mode client \
  --executor-memory $EXECUTOR_MEM \
  --executor-cores $EXECUTOR_CORES \
  --total-executor-cores $EXECUTOR_CORES \
  --driver-memory $DRIVER_MEM \
  --conf "spark.sql.streaming.stateStore.providerClass=org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider" \
  --conf "spark.sql.streaming.stateStore.minDeltasForSnapshot=10" \
  --conf "spark.sql.shuffle.partitions=4" \
  --packages io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  /opt/spark/app/spark/jobs/taxi_bronze_to_silver.py &

TAXI_PID=$!
echo -e "${GREEN}✓ taxi_clean submitted (PID: $TAXI_PID)${NC}"
echo ""

###############################################################################
# SUMMARY
###############################################################################
echo -e "${YELLOW}═══════════════════════════════════════════════${NC}"
echo -e "${GREEN}✅ All Silver streaming jobs submitted successfully!${NC}"
echo -e "${YELLOW}═══════════════════════════════════════════════${NC}"
echo ""

echo "Monitor jobs at: http://localhost:8080"
echo ""