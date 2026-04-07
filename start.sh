#!/bin/bash
set -e

echo "Starting Flink jobmanager..."
/opt/flink/bin/jobmanager.sh start-foreground &
JOBMANAGER_PID=$!

echo "Waiting for Postgres..."
until pg_isready -h postgres -p 5432 -U postgres; do
  sleep 2
done

echo "Running init.sql..."
PGPASSWORD=postgres psql -h postgres -U postgres -d postgres -f /opt/src/init.sql

echo "Waiting for Flink jobmanager to be ready..."
until curl -s http://localhost:8081/overview > /dev/null; do
  sleep 2
done

echo "Submitting Flink jobs..."
/opt/flink/bin/flink run --python /opt/src/job/trade_agg.py &
/opt/flink/bin/flink run --python /opt/src/job/ob_agg.py &

wait $JOBMANAGER_PID