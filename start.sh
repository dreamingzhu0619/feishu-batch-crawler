#!/bin/bash
# 启动服务
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PY="$SCRIPT_DIR/venv/bin/python"

echo "启动 batch_creator (端口 5001)..."
nohup $PY "$SCRIPT_DIR/batch_creator.py" > "$SCRIPT_DIR/creator.log" 2>&1 &
echo "  PID: $!"

echo "启动 batch_import (端口 8888)..."
nohup $PY "$SCRIPT_DIR/batch_import.py" > "$SCRIPT_DIR/import.log" 2>&1 &
echo "  PID: $!"

echo ""
echo "服务已启动！"
echo "  批量创建: http://$(hostname -I | awk '{print $1}'):5001"
echo "  导入看板: http://$(hostname -I | awk '{print $1}'):8888/dashboard"
echo ""
echo "查看日志: tail -f $SCRIPT_DIR/creator.log $SCRIPT_DIR/import.log"
echo "停止服务: bash $SCRIPT_DIR/stop.sh"
