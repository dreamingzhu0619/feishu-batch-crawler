#!/bin/bash
# 停止服务
echo "停止 batch_creator..."
pkill -f "batch_creator.py" 2>/dev/null && echo "  已停止" || echo "  未运行"

echo "停止 batch_import..."
pkill -f "batch_import.py" 2>/dev/null && echo "  已停止" || echo "  未运行"
