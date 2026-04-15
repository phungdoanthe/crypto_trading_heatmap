#!/bin/bash

echo "Waiting for Flink..."
sleep 20

echo "Run job ob_aggregate_tumbling_window"
/opt/flink/bin/flink run -py /opt/src/job/ob_aggregate_tumbling_window.py --pyFiles /opt/src -d

echo "Run job trade_aggregate_tumbling_window"
/opt/flink/bin/flink run -py /opt/src/job/trade_aggregate_tumbling_window.py --pyFiles /opt/src -d

echo "Done!"

tail -f /dev/null