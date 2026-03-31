#!/bin/bash
# 一键部署脚本 - 安装依赖 + 创建虚拟环境 + 启动服务
set +e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV_DIR="$SCRIPT_DIR/venv"

echo "======================================"
echo " 一键部署 - 飞书批量导入工具"
echo "======================================"
echo ""

# ── 1. 系统依赖 ──
echo -e "▶ ${YELLOW}安装系统依赖...${NC}"
apt update -qq
apt install -y python3 python3-pip python3-venv python3.12-venv curl 2>/dev/null || \
apt install -y python3 python3-pip python3-venv curl
echo -e "  ${GREEN}[OK]${NC} 系统依赖安装完成"
echo ""

# ── 2. 虚拟环境 ──
echo -e "▶ ${YELLOW}创建虚拟环境...${NC}"
if [ -d "$VENV_DIR" ]; then
    echo "  已存在，删除旧环境..."
    rm -rf "$VENV_DIR"
fi
python3 -m venv "$VENV_DIR"
echo -e "  ${GREEN}[OK]${NC} 虚拟环境创建完成: $VENV_DIR"
echo ""

# ── 3. Python 依赖 ──
echo -e "▶ ${YELLOW}安装 Python 依赖...${NC}"
"$VENV_DIR/bin/pip" install --upgrade pip -q
"$VENV_DIR/bin/pip" install flask requests httpx fastapi uvicorn -q
echo -e "  ${GREEN}[OK]${NC} Python 依赖安装完成"
echo ""

# ── 4. 验证 ──
echo -e "▶ ${YELLOW}验证安装...${NC}"
FAIL=0
for pkg in flask requests httpx fastapi uvicorn; do
    if "$VENV_DIR/bin/python" -c "import $pkg" 2>/dev/null; then
        ver=$("$VENV_DIR/bin/python" -c "import $pkg; print(getattr($pkg, '__version__', 'ok'))" 2>/dev/null)
        echo -e "  ${GREEN}[OK]${NC} $pkg ($ver)"
    else
        echo -e "  ${RED}[FAIL]${NC} $pkg"
        ((FAIL++))
    fi
done
echo ""

if [ $FAIL -gt 0 ]; then
    echo -e "${RED}有 $FAIL 个包安装失败，请检查错误信息。${NC}"
    exit 1
fi

echo "======================================"
echo -e " ${GREEN}部署完成！${NC}"
echo "======================================"
echo ""
echo " 启动命令："
echo "   batch_creator (端口 5001):"
echo "     nohup $VENV_DIR/bin/python $SCRIPT_DIR/batch_creator.py > $SCRIPT_DIR/creator.log 2>&1 &"
echo ""
echo "   batch_import (端口 8888):"
echo "     nohup $VENV_DIR/bin/python $SCRIPT_DIR/batch_import.py > $SCRIPT_DIR/import.log 2>&1 &"
echo ""
echo " 或者直接运行: bash $SCRIPT_DIR/start.sh"
