#!/bin/bash
# 环境检查脚本 - 检查运行 batch_creator.py 和 batch_import.py 所需的一切
set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

PASS=0
FAIL=0
WARN=0

ok()   { echo -e "  ${GREEN}[OK]${NC}   $1"; ((PASS++)); }
fail() { echo -e "  ${RED}[FAIL]${NC} $1"; ((FAIL++)); }
warn() { echo -e "  ${YELLOW}[WARN]${NC} $1"; ((WARN++)); }

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "======================================"
echo " 环境检查 - 飞书批量导入工具"
echo "======================================"
echo ""

# ── 1. Python ──
echo "▶ Python 环境"
if command -v python3 &>/dev/null; then
    PY_VER=$(python3 --version 2>&1)
    ok "python3 已安装: $PY_VER"
else
    fail "python3 未安装 → apt install python3"
fi

if command -v pip3 &>/dev/null; then
    ok "pip3 已安装"
else
    warn "pip3 未安装 → apt install python3-pip (或用 venv)"
fi

if python3 -m venv --help &>/dev/null; then
    ok "python3-venv 可用"
else
    fail "python3-venv 不可用 → apt install python3-venv"
fi
echo ""

# ── 2. 虚拟环境 & Python 依赖 ──
echo "▶ Python 依赖包"
VENV_DIR="$SCRIPT_DIR/venv"
if [ -d "$VENV_DIR" ] && [ -f "$VENV_DIR/bin/python" ]; then
    ok "虚拟环境已创建: $VENV_DIR"
    PY="$VENV_DIR/bin/python"
else
    warn "虚拟环境未创建 → python3 -m venv venv && source venv/bin/activate && pip install flask requests httpx fastapi uvicorn"
    PY="python3"
fi

PACKAGES=("flask" "requests" "httpx" "fastapi" "uvicorn")
for pkg in "${PACKAGES[@]}"; do
    if $PY -c "import $pkg" 2>/dev/null; then
        ver=$($PY -c "import $pkg; print(getattr($pkg, '__version__', 'ok'))" 2>/dev/null)
        ok "$pkg ($ver)"
    else
        fail "$pkg 未安装 → pip install $pkg"
    fi
done
echo ""

# ── 3. 数据文件 ──
echo "▶ 数据文件"
for f in "feishu_companies.json" "url输出结果.md"; do
    fpath="$SCRIPT_DIR/$f"
    if [ -f "$fpath" ]; then
        size=$(du -h "$fpath" | cut -f1)
        ok "$f ($size)"
    else
        warn "$f 不存在 (非必须，可通过前端上传CSV替代)"
    fi
done
echo ""

# ── 4. 脚本文件 ──
echo "▶ 脚本文件"
for f in "batch_creator.py" "batch_import.py"; do
    if [ -f "$SCRIPT_DIR/$f" ]; then
        ok "$f 存在"
    else
        fail "$f 缺失!"
    fi
done
echo ""

# ── 5. 网络连通性 ──
echo "▶ 网络连通性"

check_url() {
    local name="$1" url="$2"
    code=$(curl -s -o /dev/null -w "%{http_code}" --connect-timeout 5 "$url" 2>/dev/null || echo "000")
    if [ "$code" != "000" ]; then
        ok "$name ($url) → HTTP $code"
    else
        fail "$name ($url) → 无法连接"
    fi
}

check_url "AI爬虫API" "http://192.168.253.10:9097/api/companies?page=1&page_size=1"
check_url "天眼查API"  "http://192.168.253.10:8083/tianyancha/open/ic?keyword=test"
echo ""

# ── 6. 端口占用 ──
echo "▶ 端口占用检查"
for port in 5001 8888; do
    if ss -tlnp 2>/dev/null | grep -q ":$port " || netstat -tlnp 2>/dev/null | grep -q ":$port "; then
        warn "端口 $port 已被占用"
    else
        ok "端口 $port 可用"
    fi
done
echo ""

# ── 7. batch_import.py 中的 PLATFORM_BASE_URL 检查 ──
echo "▶ 配置检查"
if grep -q "localhost:6666" "$SCRIPT_DIR/batch_import.py" 2>/dev/null; then
    warn "batch_import.py 中 PLATFORM_BASE_URL 仍为 localhost:6666，可能需要改为实际地址"
else
    ok "batch_import.py PLATFORM_BASE_URL 已配置"
fi
echo ""

# ── 汇总 ──
echo "======================================"
echo -e " 结果: ${GREEN}$PASS 通过${NC} / ${RED}$FAIL 失败${NC} / ${YELLOW}$WARN 警告${NC}"
echo "======================================"

if [ $FAIL -gt 0 ]; then
    echo -e "\n${RED}有 $FAIL 项检查未通过，请先修复再启动服务。${NC}"
    exit 1
elif [ $WARN -gt 0 ]; then
    echo -e "\n${YELLOW}有 $WARN 项警告，建议检查后再启动。${NC}"
    exit 0
else
    echo -e "\n${GREEN}所有检查通过，可以启动服务！${NC}"
    exit 0
fi
