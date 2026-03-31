#!/usr/bin/env python3
"""
批量创建爬虫任务工具
支持：内置解析 url输出结果.md / 前端上传 CSV
实时进度推送：Server-Sent Events (SSE)
"""

import csv
import io
import json
import os
import re
import time
from urllib.parse import quote, urlparse

import requests
from flask import Flask, Response, jsonify, request, stream_with_context

app = Flask(__name__)

# ── 配置 ──────────────────────────────────────────────
MD_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "url输出结果.md")
COMPANY_API = "http://192.168.253.10:9097/api/companies"
TIANYANCHA_API = "http://192.168.253.10:8083/tianyancha/open/ic"
REQUEST_TIMEOUT = 10
SLEEP_BETWEEN_COMPANIES = 3
SLEEP_AFTER_TIANYANCHA = 2

# 全局：当前加载的公司列表（由 load-default 或 upload 写入）
loaded_companies = []
# 全局：停止标志
stop_flag = False


# ── MD 文件解析 ───────────────────────────────────────
def parse_md_file(filepath):
    """解析 url输出结果.md，返回 [{company_name, url}, ...]"""
    results = []
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()

    # 按公司分块：匹配 "公司: xxx  (共 N 个职位)"
    blocks = re.split(r"={10,}\n", content)
    company_name = None
    for block in blocks:
        # 尝试匹配公司行
        m = re.match(r"公司:\s*(.+?)\s+\(共\s*\d+\s*个职位\)", block.strip())
        if m:
            company_name = m.group(1).strip()
            continue
        # 如果上一个块解析到了公司名，这个块应该包含 URL
        if company_name:
            if company_name.lower() == "null":
                company_name = None
                continue
            # 提取第一条 URL
            url_match = re.search(r"https?://\S+", block)
            if url_match:
                full_url = url_match.group(0)
                listing_url = extract_listing_url(full_url)
                results.append({
                    "company_name": company_name,
                    "url": listing_url,
                })
            company_name = None
    return results


def extract_listing_url(detail_url):
    """从职位详情 URL 截取到招聘列表页。
    例：https://nio.jobs.feishu.cn/index/position/xxx/detail
      → https://nio.jobs.feishu.cn/index
    例：https://campus.dewu.com/index/position/xxx/detail
      → https://campus.dewu.com/index
    策略：找到 /position 前面的部分作为列表页。
    """
    # 找 /position 的位置
    idx = detail_url.find("/position")
    if idx != -1:
        return detail_url[:idx]
    # 回退：取到路径第一段
    parsed = urlparse(detail_url)
    path_parts = parsed.path.strip("/").split("/")
    if path_parts:
        return f"{parsed.scheme}://{parsed.netloc}/{path_parts[0]}"
    return f"{parsed.scheme}://{parsed.netloc}"


# ── CSV 解析 ──────────────────────────────────────────
def parse_csv(file_content):
    """解析 CSV 内容，返回 [{company_name, url, channel}, ...]"""
    results = []
    reader = csv.DictReader(io.StringIO(file_content))
    for row in reader:
        name = (row.get("company_name") or "").strip()
        url = (row.get("url") or "").strip()
        channel = (row.get("channel") or "").strip()
        if name:
            results.append({"company_name": name, "url": url, "channel": channel})
    return results


# ── 获取已有公司数据（去重用） ────────────────────────
def fetch_existing_companies():
    """分页拉取所有已有公司，返回 (url_set, name_set)"""
    urls = set()
    names = set()
    page = 1
    page_size = 100
    while True:
        try:
            resp = requests.get(
                COMPANY_API,
                params={"page": page, "pageSize": page_size},
                timeout=REQUEST_TIMEOUT,
            )
            data = resp.json()
            items = data.get("data", {}).get("items", [])
            if not items:
                break
            for item in items:
                u = (item.get("url") or "").strip()
                n = (item.get("name") or "").strip()
                if u:
                    urls.add(u)
                if n:
                    names.add(n)
            total = data.get("data", {}).get("total", 0)
            if page * page_size >= total:
                break
            page += 1
        except Exception:
            break
    return urls, names


# ── 天眼查查询 ───────────────────────────────────────
def query_tianyancha(company_name):
    """调天眼查 API，返回 (alias, credit_code, city) 或 (None, None, None)"""
    for attempt in range(2):  # 最多重试 1 次
        try:
            resp = requests.get(
                TIANYANCHA_API,
                params={"keyword": company_name},
                timeout=REQUEST_TIMEOUT,
            )
            data = resp.json()
            if data.get("code") in (200, 0):
                result = data.get("data", {}).get("result", {}) or {}
                alias = (result.get("alias") or "").strip()
                credit_code = (result.get("creditCode") or "").strip()
                city = (result.get("city") or "").strip()
                return alias or None, credit_code or None, city or None
            return None, None, None
        except Exception:
            if attempt == 0:
                time.sleep(1)
    return None, None, None


# ── 创建公司任务 ──────────────────────────────────────
def create_company(name, company_name, credit_code, url):
    """调 POST /api/companies 创建任务，返回 (success, message, company_id)"""
    payload = {
        "name": name,
        "company_name": company_name,
        "credit_code": credit_code or "",
        "url": url,
        "site_type": "auto",
        "job_url_example": "",
        "description": "",
    }
    for attempt in range(2):
        try:
            resp = requests.post(
                COMPANY_API,
                json=payload,
                timeout=REQUEST_TIMEOUT,
            )
            if resp.status_code in (200, 201):
                # 尝试从返回中提取公司 id
                company_id = None
                try:
                    resp_data = resp.json()
                    company_id = resp_data.get("data", {}).get("id") or resp_data.get("id")
                except Exception:
                    pass
                return True, "创建成功", company_id
            return False, f"HTTP {resp.status_code}: {resp.text[:200]}", None
        except Exception as e:
            if attempt == 0:
                time.sleep(1)
            else:
                return False, f"请求异常: {str(e)}", None
    return False, "未知错误", None


# ── SSE 批量处理 ──────────────────────────────────────
def process_batch(companies):
    """生成器：逐个处理公司并 yield SSE 事件"""
    total = len(companies)
    if total == 0:
        yield sse_event("done", {"total": 0, "success": 0, "skipped": 0, "degraded": 0, "failed": 0})
        return

    # 1. 获取已有数据
    yield sse_event("status", {"message": "正在获取已有公司数据（去重用）..."})
    existing_urls, existing_names = fetch_existing_companies()
    yield sse_event("status", {
        "message": f"已加载 {len(existing_urls)} 个 URL、{len(existing_names)} 个名称用于去重"
    })

    stats = {"success": 0, "skipped": 0, "degraded": 0, "failed": 0}

    for i, company in enumerate(companies):
        if stop_flag:
            yield sse_event("stopped", {"message": "已手动停止", "processed": i, **stats})
            return

        idx = i + 1
        cname = company["company_name"]
        url = company.get("url") or ""
        channel = company.get("channel") or ""
        result = {
            "index": idx,
            "total": total,
            "company_name": cname,
            "name": "",
            "credit_code": "",
            "url": url,
            "channel": channel,
            "status": "",
            "status_type": "",
            "message": "",
            "company_id": None,
        }

        # a. URL 去重（仅在 url 非空时检查）
        if url and url in existing_urls:
            result["status"] = "已跳过（重复）"
            result["status_type"] = "skipped"
            result["message"] = "URL 已存在"
            stats["skipped"] += 1
            yield sse_event("result", result)
            time.sleep(SLEEP_BETWEEN_COMPANIES)
            continue

        # b. 天眼查
        yield sse_event("status", {"message": f"[{idx}/{total}] 正在查询天眼查: {cname}"})
        alias, credit_code, city = query_tianyancha(cname)
        time.sleep(SLEEP_AFTER_TIANYANCHA)

        name = alias if alias else cname
        # name 冲突自动加城市后缀
        if name in existing_names:
            if alias and city:
                name_with_city = f"{alias}\uff08{city}\uff09"  # 用中文括号
                if name_with_city not in existing_names:
                    name = name_with_city
                else:
                    name = cname  # 还冲突，用全称
            else:
                name = cname  # 无 city 信息，用全称

        result["name"] = name
        result["credit_code"] = credit_code or ""

        # c. 创建任务
        yield sse_event("status", {"message": f"[{idx}/{total}] 正在创建: {name}"})
        success, msg, company_id = create_company(name, cname, credit_code, url)

        if success:
            result["company_id"] = company_id
            if not alias:
                result["status"] = "降级创建"
                result["status_type"] = "degraded"
                result["message"] = "天眼查未查到，使用原始公司名"
                stats["degraded"] += 1
            else:
                result["status"] = "创建成功"
                result["status_type"] = "success"
                result["message"] = msg
                stats["success"] += 1
            # 加入去重集合
            if url:
                existing_urls.add(url)
            existing_names.add(name)
        else:
            result["status"] = "创建失败"
            result["status_type"] = "error"
            result["message"] = msg
            stats["failed"] += 1

        yield sse_event("result", result)
        time.sleep(SLEEP_BETWEEN_COMPANIES)

    yield sse_event("done", {
        "total": total,
        **stats,
    })


def sse_event(event_type, data):
    """格式化 SSE 事件"""
    return f"event: {event_type}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"


# ── API 路由 ──────────────────────────────────────────
@app.route("/api/load-default")
def load_default():
    """加载内置 md 文件的解析结果"""
    global loaded_companies
    if not os.path.exists(MD_FILE):
        return jsonify({"error": f"文件不存在: {MD_FILE}"}), 404
    loaded_companies = parse_md_file(MD_FILE)
    return jsonify({
        "count": len(loaded_companies),
        "companies": loaded_companies,
    })


@app.route("/api/upload", methods=["POST"])
def upload_csv():
    """上传 CSV 文件"""
    global loaded_companies
    if "file" not in request.files:
        return jsonify({"error": "未选择文件"}), 400
    f = request.files["file"]
    if not f.filename.endswith(".csv"):
        return jsonify({"error": "仅支持 CSV 文件"}), 400
    content = f.read().decode("utf-8-sig")
    loaded_companies = parse_csv(content)
    if not loaded_companies:
        return jsonify({"error": "CSV 解析结果为空，请检查格式（需包含 company_name 列）"}), 400
    return jsonify({
        "count": len(loaded_companies),
        "companies": loaded_companies,
    })


@app.route("/api/stop", methods=["POST"])
def stop_batch():
    """停止批量创建"""
    global stop_flag
    stop_flag = True
    return jsonify({"ok": True})


@app.route("/api/start")
def start_batch():
    """SSE 流：启动批量创建"""
    global stop_flag
    stop_flag = False
    if not loaded_companies:
        return jsonify({"error": "请先加载数据（预览 MD 或上传 CSV）"}), 400

    def generate():
        yield from process_batch(loaded_companies)

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@app.route("/api/retry", methods=["POST"])
def retry_single():
    """重跑单条创建"""
    body = request.get_json(force=True)
    name = (body.get("name") or "").strip()
    company_name = (body.get("company_name") or "").strip()
    credit_code = (body.get("credit_code") or "").strip()
    url = (body.get("url") or "").strip()
    if not name or not url:
        return jsonify({"error": "name 和 url 必填"}), 400
    success, msg, company_id = create_company(name, company_name or name, credit_code, url)
    if success:
        return jsonify({"ok": True, "message": msg, "company_id": company_id})
    return jsonify({"ok": False, "message": msg}), 500


GENERATE_API_TEMPLATE = "http://192.168.253.10:9097/api/companies/{id}/generate"

# 全局：generate 停止标志
generate_stop_flag = False


@app.route("/api/generate", methods=["POST"])
def generate_companies():
    """SSE 流：串行调用 generate API"""
    global generate_stop_flag
    generate_stop_flag = False
    body = request.get_json(force=True)
    ids = body.get("ids", [])
    if not ids:
        return jsonify({"error": "ids 不能为空"}), 400

    def gen():
        total = len(ids)
        for i, cid in enumerate(ids):
            if generate_stop_flag:
                yield sse_event("generate_stopped", {"message": "已手动停止", "processed": i})
                return
            idx = i + 1
            yield sse_event("generate_progress", {
                "index": idx,
                "total": total,
                "company_id": cid,
                "status": "generating",
                "message": f"[{idx}/{total}] 正在 generate: {cid}",
            })
            try:
                resp = requests.post(
                    GENERATE_API_TEMPLATE.format(id=cid),
                    json={"verbose": True, "timeout": 10800},
                    timeout=600,  # generate 可能很慢
                )
                if resp.status_code in (200, 201):
                    yield sse_event("generate_progress", {
                        "index": idx,
                        "total": total,
                        "company_id": cid,
                        "status": "success",
                        "message": f"[{idx}/{total}] generate 成功: {cid}",
                    })
                else:
                    yield sse_event("generate_progress", {
                        "index": idx,
                        "total": total,
                        "company_id": cid,
                        "status": "error",
                        "message": f"[{idx}/{total}] generate 失败: HTTP {resp.status_code}",
                    })
            except Exception as e:
                yield sse_event("generate_progress", {
                    "index": idx,
                    "total": total,
                    "company_id": cid,
                    "status": "error",
                    "message": f"[{idx}/{total}] generate 异常: {str(e)[:100]}",
                })
        yield sse_event("generate_done", {"total": total})

    return Response(
        stream_with_context(gen()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@app.route("/api/generate/stop", methods=["POST"])
def stop_generate():
    """停止 generate"""
    global generate_stop_flag
    generate_stop_flag = True
    return jsonify({"ok": True})


@app.route("/api/template")
def download_template():
    """下载 CSV 模板"""
    content = "company_name,url,channel\n示例公司A,https://example.jobs.feishu.cn/index,\n示例公司B,,moka\n示例公司C,https://campus.example.com/index,boss\n"
    return Response(
        content,
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=template.csv"},
    )


# ── 前端页面 ──────────────────────────────────────────
@app.route("/")
def index():
    return HTML_PAGE


HTML_PAGE = r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>批量创建爬虫任务</title>
<style>
  :root {
    --bg: #f5f7fa;
    --card: #fff;
    --border: #e2e8f0;
    --text: #1a202c;
    --text2: #4a5568;
    --green: #48bb78;
    --green-bg: #f0fff4;
    --yellow: #ecc94b;
    --yellow-bg: #fffff0;
    --red: #f56565;
    --red-bg: #fff5f5;
    --gray: #a0aec0;
    --gray-bg: #f7fafc;
    --blue: #4299e1;
    --blue-bg: #ebf8ff;
    --purple: #805ad5;
    --purple-bg: #faf5ff;
  }
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    background: var(--bg);
    color: var(--text);
    line-height: 1.6;
    padding: 24px;
  }
  .container { max-width: 1200px; margin: 0 auto; }
  h1 {
    font-size: 24px;
    font-weight: 700;
    margin-bottom: 24px;
    display: flex;
    align-items: center;
    gap: 12px;
  }
  .card {
    background: var(--card);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 24px;
    margin-bottom: 20px;
  }
  .card h2 {
    font-size: 16px;
    font-weight: 600;
    margin-bottom: 16px;
    color: var(--text2);
  }
  .source-tabs {
    display: flex;
    gap: 12px;
    margin-bottom: 16px;
  }
  .source-tab {
    padding: 8px 20px;
    border: 2px solid var(--border);
    border-radius: 8px;
    cursor: pointer;
    font-size: 14px;
    font-weight: 500;
    background: var(--card);
    transition: all 0.2s;
  }
  .source-tab:hover { border-color: var(--blue); color: var(--blue); }
  .source-tab.active {
    border-color: var(--blue);
    background: var(--blue-bg);
    color: var(--blue);
  }
  .source-panel { display: none; }
  .source-panel.active { display: block; }
  .btn {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 10px 20px;
    border: none;
    border-radius: 8px;
    font-size: 14px;
    font-weight: 600;
    cursor: pointer;
    transition: all 0.2s;
  }
  .btn:disabled { opacity: 0.5; cursor: not-allowed; }
  .btn-primary { background: var(--blue); color: #fff; }
  .btn-primary:hover:not(:disabled) { background: #3182ce; }
  .btn-success { background: var(--green); color: #fff; }
  .btn-success:hover:not(:disabled) { background: #38a169; }
  .btn-danger { background: var(--red); color: #fff; }
  .btn-danger:hover:not(:disabled) { background: #e53e3e; }
  .btn-purple { background: var(--purple); color: #fff; }
  .btn-purple:hover:not(:disabled) { background: #6b46c1; }
  .btn-outline {
    background: transparent;
    border: 2px solid var(--border);
    color: var(--text2);
  }
  .btn-outline:hover:not(:disabled) { border-color: var(--blue); color: var(--blue); }
  .btn-sm { padding: 4px 12px; font-size: 12px; }
  .btn-group { display: flex; gap: 12px; flex-wrap: wrap; align-items: center; }
  .upload-area {
    border: 2px dashed var(--border);
    border-radius: 8px;
    padding: 32px;
    text-align: center;
    cursor: pointer;
    transition: all 0.2s;
  }
  .upload-area:hover { border-color: var(--blue); background: var(--blue-bg); }
  .upload-area input { display: none; }
  .upload-hint { font-size: 13px; color: var(--text2); margin-top: 8px; }
  .progress-wrap {
    display: none;
    margin-bottom: 16px;
  }
  .progress-wrap.visible { display: block; }
  .progress-bar-outer {
    height: 8px;
    background: #edf2f7;
    border-radius: 4px;
    overflow: hidden;
  }
  .progress-bar-inner {
    height: 100%;
    width: 0%;
    background: linear-gradient(90deg, var(--blue), var(--green));
    border-radius: 4px;
    transition: width 0.3s;
  }
  .progress-text {
    font-size: 13px;
    color: var(--text2);
    margin-top: 6px;
  }
  .stats {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
    gap: 12px;
    margin-bottom: 16px;
  }
  .stat-item {
    padding: 16px;
    border-radius: 8px;
    text-align: center;
  }
  .stat-item .num { font-size: 28px; font-weight: 700; }
  .stat-item .label { font-size: 12px; margin-top: 4px; }
  .stat-success { background: var(--green-bg); color: #276749; }
  .stat-degraded { background: var(--yellow-bg); color: #975a16; }
  .stat-skipped { background: var(--gray-bg); color: #4a5568; }
  .stat-failed { background: var(--red-bg); color: #c53030; }
  .stat-total { background: var(--blue-bg); color: #2b6cb0; }
  .status-msg {
    font-size: 13px;
    color: var(--text2);
    padding: 8px 12px;
    background: var(--gray-bg);
    border-radius: 6px;
    margin-bottom: 12px;
    display: none;
  }
  .status-msg.visible { display: block; }
  .table-wrap {
    overflow-x: auto;
    border: 1px solid var(--border);
    border-radius: 8px;
  }
  table {
    width: 100%;
    border-collapse: collapse;
    font-size: 13px;
  }
  th {
    background: #f7fafc;
    font-weight: 600;
    text-align: left;
    padding: 10px 14px;
    border-bottom: 2px solid var(--border);
    white-space: nowrap;
  }
  td {
    padding: 10px 14px;
    border-bottom: 1px solid var(--border);
    vertical-align: top;
  }
  tr:last-child td { border-bottom: none; }
  .badge {
    display: inline-block;
    padding: 2px 10px;
    border-radius: 12px;
    font-size: 12px;
    font-weight: 600;
    white-space: nowrap;
  }
  .badge-success { background: var(--green-bg); color: #276749; }
  .badge-degraded { background: var(--yellow-bg); color: #975a16; }
  .badge-skipped { background: var(--gray-bg); color: #718096; }
  .badge-error { background: var(--red-bg); color: #c53030; }
  .badge-generate { background: var(--purple-bg); color: var(--purple); }
  .url-cell {
    max-width: 300px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .url-cell a { color: var(--blue); text-decoration: none; }
  .url-cell a:hover { text-decoration: underline; }
  .fail-section {
    display: none;
    margin-top: 16px;
  }
  .fail-section.visible { display: block; }
  .fail-section h3 {
    color: var(--red);
    font-size: 15px;
    margin-bottom: 12px;
  }
  .fail-item {
    background: var(--red-bg);
    border: 1px solid #fed7d7;
    border-radius: 8px;
    padding: 12px 16px;
    margin-bottom: 8px;
  }
  .fail-item .fail-name { font-weight: 600; }
  .fail-item .fail-reason { font-size: 13px; color: #c53030; margin-top: 4px; }
  .preview-table { margin-top: 12px; }
  /* 编辑输入框 */
  .edit-input {
    border: 1px solid var(--border);
    border-radius: 4px;
    padding: 3px 6px;
    font-size: 12px;
    width: 100%;
    min-width: 80px;
  }
  .edit-input:focus { outline: none; border-color: var(--blue); }
  /* Generate 区域 */
  .generate-section {
    display: none;
    margin-top: 16px;
    padding: 16px;
    background: var(--purple-bg);
    border: 1px solid #e9d8fd;
    border-radius: 8px;
  }
  .generate-section.visible { display: block; }
  .generate-section h3 {
    color: var(--purple);
    font-size: 15px;
    margin-bottom: 12px;
  }
  .generate-progress {
    margin-top: 12px;
    font-size: 13px;
    color: var(--text2);
  }
  .generate-log {
    margin-top: 8px;
    max-height: 200px;
    overflow-y: auto;
    font-size: 12px;
    background: #fff;
    border: 1px solid #e9d8fd;
    border-radius: 6px;
    padding: 8px 12px;
  }
  .generate-log-item { padding: 2px 0; }
  .generate-log-item.gen-success { color: #276749; }
  .generate-log-item.gen-error { color: #c53030; }
  .generate-log-item.gen-running { color: var(--purple); }
</style>
</head>
<body>
<div class="container">
  <h1>批量创建爬虫任务</h1>

  <!-- 数据源 -->
  <div class="card">
    <h2>数据源</h2>
    <div class="source-tabs">
      <div class="source-tab active" onclick="switchTab('md')">内置 MD 文件</div>
      <div class="source-tab" onclick="switchTab('csv')">上传 CSV</div>
    </div>
    <div id="panel-md" class="source-panel active">
      <p style="font-size:14px;color:var(--text2);margin-bottom:12px;">
        从 <code>url输出结果.md</code> 解析公司及招聘列表页 URL
      </p>
      <div class="btn-group">
        <button class="btn btn-primary" onclick="loadDefault()" id="btn-load">加载并预览</button>
      </div>
    </div>
    <div id="panel-csv" class="source-panel">
      <div class="btn-group" style="margin-bottom:12px;">
        <a href="/api/template" class="btn btn-outline">下载 CSV 模板</a>
      </div>
      <div class="upload-area" id="upload-area" onclick="document.getElementById('csv-input').click()">
        <div style="font-size:14px;font-weight:500;">点击上传 CSV 文件</div>
        <div class="upload-hint">格式：company_name, url（可选）, channel（可选）</div>
        <input type="file" id="csv-input" accept=".csv" onchange="uploadCSV(this)">
      </div>
    </div>
  </div>

  <!-- 预览 -->
  <div class="card" id="preview-card" style="display:none">
    <h2>预览 (<span id="preview-count">0</span> 家公司)</h2>
    <div class="table-wrap preview-table">
      <table>
        <thead><tr><th>#</th><th>公司名称</th><th>URL</th><th>渠道</th></tr></thead>
        <tbody id="preview-body"></tbody>
      </table>
    </div>
    <div style="margin-top:16px;display:flex;align-items:center;gap:16px;flex-wrap:wrap;">
      <button class="btn btn-success" onclick="startBatch()" id="btn-start">开始创建</button>
      <button class="btn btn-outline" onclick="stopBatch()" id="btn-stop" style="display:none;border-color:var(--red);color:var(--red);">停止</button>
      <label style="display:flex;align-items:center;gap:6px;font-size:14px;cursor:pointer;user-select:none;">
        <input type="checkbox" id="auto-generate" checked>
        <span>创建完成后自动 Generate</span>
      </label>
    </div>
  </div>

  <!-- 进度 -->
  <div class="card" id="progress-card" style="display:none">
    <h2>执行进度</h2>
    <div id="status-msg" class="status-msg"></div>
    <div class="progress-wrap visible">
      <div class="progress-bar-outer"><div class="progress-bar-inner" id="progress-bar"></div></div>
      <div class="progress-text" id="progress-text">0 / 0</div>
    </div>
    <div class="stats" id="stats">
      <div class="stat-item stat-total"><div class="num" id="stat-total">0</div><div class="label">总数</div></div>
      <div class="stat-item stat-success"><div class="num" id="stat-success">0</div><div class="label">成功</div></div>
      <div class="stat-item stat-degraded"><div class="num" id="stat-degraded">0</div><div class="label">降级创建</div></div>
      <div class="stat-item stat-skipped"><div class="num" id="stat-skipped">0</div><div class="label">已跳过</div></div>
      <div class="stat-item stat-failed"><div class="num" id="stat-failed">0</div><div class="label">失败</div></div>
    </div>
    <div class="table-wrap">
      <table>
        <thead><tr>
          <th style="width:30px;"><input type="checkbox" id="check-all" onchange="toggleCheckAll(this)" title="全选"></th>
          <th>#</th><th>公司全称</th><th>简称</th><th>信用代码</th><th>URL</th><th>渠道</th><th>状态</th><th>备注</th><th>操作</th>
        </tr></thead>
        <tbody id="result-body"></tbody>
      </table>
    </div>

    <!-- Generate 区域 -->
    <div class="generate-section" id="generate-section">
      <h3>Generate 爬虫配置</h3>
      <div class="btn-group">
        <button class="btn btn-purple btn-sm" onclick="generateSelected()">Generate 选中</button>
        <button class="btn btn-purple btn-sm" onclick="generateAll()">一键全部 Generate</button>
        <button class="btn btn-outline btn-sm" onclick="stopGenerate()" id="btn-gen-stop" style="display:none;border-color:var(--red);color:var(--red);">停止 Generate</button>
        <span id="gen-selected-count" style="font-size:13px;color:var(--text2);"></span>
      </div>
      <div class="generate-progress" id="generate-progress" style="display:none;">
        <div class="progress-bar-outer"><div class="progress-bar-inner" id="gen-progress-bar"></div></div>
        <div class="progress-text" id="gen-progress-text">0 / 0</div>
      </div>
      <div class="generate-log" id="generate-log" style="display:none;"></div>
    </div>

    <!-- 失败列表 -->
    <div class="fail-section" id="fail-section">
      <h3>创建失败列表</h3>
      <div id="fail-list"></div>
    </div>
  </div>
</div>

<script>
let companies = [];
let stats = { success: 0, degraded: 0, skipped: 0, failed: 0 };
let processed = 0;
let eventSource = null;
// 存储成功创建的公司 {company_id, name, ...}
let createdCompanies = [];
let generateES = null;

function switchTab(type) {
  document.querySelectorAll('.source-tab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.source-panel').forEach(p => p.classList.remove('active'));
  if (type === 'md') {
    document.querySelectorAll('.source-tab')[0].classList.add('active');
    document.getElementById('panel-md').classList.add('active');
  } else {
    document.querySelectorAll('.source-tab')[1].classList.add('active');
    document.getElementById('panel-csv').classList.add('active');
  }
}

function showPreview(data) {
  companies = data;
  document.getElementById('preview-count').textContent = data.length;
  const tbody = document.getElementById('preview-body');
  tbody.innerHTML = '';
  data.forEach((c, i) => {
    const tr = document.createElement('tr');
    const urlDisplay = c.url ? `<a href="${esc(c.url)}" target="_blank">${esc(c.url)}</a>` : '-';
    tr.innerHTML = `<td>${i+1}</td><td>${esc(c.company_name)}</td><td class="url-cell">${urlDisplay}</td><td>${esc(c.channel || '')}</td>`;
    tbody.appendChild(tr);
  });
  document.getElementById('preview-card').style.display = '';
}

async function loadDefault() {
  const btn = document.getElementById('btn-load');
  btn.disabled = true;
  btn.textContent = '加载中...';
  try {
    const resp = await fetch('/api/load-default');
    const data = await resp.json();
    if (data.error) { alert(data.error); return; }
    showPreview(data.companies);
  } catch (e) {
    alert('加载失败: ' + e.message);
  } finally {
    btn.disabled = false;
    btn.textContent = '加载并预览';
  }
}

async function uploadCSV(input) {
  if (!input.files.length) return;
  const formData = new FormData();
  formData.append('file', input.files[0]);
  try {
    const resp = await fetch('/api/upload', { method: 'POST', body: formData });
    const data = await resp.json();
    if (data.error) { alert(data.error); return; }
    showPreview(data.companies);
  } catch (e) {
    alert('上传失败: ' + e.message);
  }
  input.value = '';
}

function startBatch() {
  if (!companies.length) { alert('请先加载数据'); return; }
  // 重置
  stats = { success: 0, degraded: 0, skipped: 0, failed: 0 };
  processed = 0;
  createdCompanies = [];
  document.getElementById('result-body').innerHTML = '';
  document.getElementById('fail-list').innerHTML = '';
  document.getElementById('fail-section').classList.remove('visible');
  document.getElementById('generate-section').classList.remove('visible');
  document.getElementById('progress-card').style.display = '';
  document.getElementById('stat-total').textContent = companies.length;
  document.getElementById('btn-start').disabled = true;
  document.getElementById('btn-stop').style.display = '';
  document.getElementById('check-all').checked = false;
  updateProgress(0, companies.length);

  // SSE
  eventSource = new EventSource('/api/start');
  eventSource.addEventListener('status', (e) => {
    const d = JSON.parse(e.data);
    const el = document.getElementById('status-msg');
    el.textContent = d.message;
    el.classList.add('visible');
  });
  eventSource.addEventListener('result', (e) => {
    const d = JSON.parse(e.data);
    processed++;
    addResultRow(d);
    updateStats(d.status_type);
    updateProgress(processed, d.total);
    if (d.status_type === 'error') addFailItem(d);
  });
  eventSource.addEventListener('done', (e) => {
    eventSource.close();
    const el = document.getElementById('status-msg');
    el.textContent = '全部处理完成！';
    batchFinished();
  });
  eventSource.addEventListener('stopped', (e) => {
    eventSource.close();
    const d = JSON.parse(e.data);
    document.getElementById('status-msg').textContent = '已手动停止，已处理 ' + d.processed + ' 家';
    batchFinished();
  });
  eventSource.onerror = () => {
    eventSource.close();
    document.getElementById('status-msg').textContent = '连接中断';
    batchFinished();
  };
}

function addResultRow(d) {
  const tbody = document.getElementById('result-body');
  const tr = document.createElement('tr');
  tr.id = 'row-' + d.index;
  const badgeCls = {success:'badge-success',degraded:'badge-degraded',skipped:'badge-skipped',error:'badge-error'}[d.status_type]||'';

  const isSuccess = (d.status_type === 'success' || d.status_type === 'degraded');
  const isError = d.status_type === 'error';
  const companyId = d.company_id || '';

  // 复选框：成功的行才有
  let checkboxHtml = '';
  if (isSuccess && companyId) {
    checkboxHtml = `<input type="checkbox" class="gen-check" data-id="${esc(String(companyId))}" data-name="${esc(d.name)}" onchange="updateGenCount()">`;
    createdCompanies.push({ id: companyId, name: d.name, index: d.index });
  }

  // 操作列
  let actionHtml = '';
  if (isError) {
    actionHtml = `<button class="btn btn-primary btn-sm" onclick="editRow(this, ${d.index})">编辑</button>`;
  }

  const urlHtml = d.url ? `<a href="${esc(d.url)}" target="_blank">${esc(d.url)}</a>` : '-';
  tr.innerHTML = `
    <td>${checkboxHtml}</td>
    <td>${d.index}</td>
    <td>${esc(d.company_name)}</td>
    <td class="name-cell">${esc(d.name)}</td>
    <td class="cc-cell" style="font-family:monospace;font-size:12px;">${esc(d.credit_code)}</td>
    <td class="url-cell">${urlHtml}</td>
    <td>${esc(d.channel || '')}</td>
    <td class="status-cell"><span class="badge ${badgeCls}">${esc(d.status)}</span></td>
    <td class="msg-cell" style="font-size:12px;color:var(--text2);">${esc(d.message)}</td>
    <td class="action-cell">${actionHtml}</td>`;

  // 存储原始数据到 tr
  tr.dataset.companyName = d.company_name || '';
  tr.dataset.name = d.name || '';
  tr.dataset.creditCode = d.credit_code || '';
  tr.dataset.url = d.url || '';
  tr.dataset.channel = d.channel || '';

  tbody.appendChild(tr);
  tr.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
}

function editRow(btn, idx) {
  const tr = document.getElementById('row-' + idx);
  if (!tr) return;
  const nameCell = tr.querySelector('.name-cell');
  const ccCell = tr.querySelector('.cc-cell');
  const actionCell = tr.querySelector('.action-cell');

  const origName = tr.dataset.name;
  const origCC = tr.dataset.creditCode;

  nameCell.innerHTML = `<input class="edit-input" id="edit-name-${idx}" value="${esc(origName)}">`;
  ccCell.innerHTML = `<input class="edit-input" id="edit-cc-${idx}" value="${esc(origCC)}">`;
  actionCell.innerHTML = `
    <button class="btn btn-success btn-sm" onclick="retryRow(${idx})">重跑</button>
    <button class="btn btn-outline btn-sm" onclick="cancelEdit(${idx})" style="margin-top:4px;">取消</button>`;
}

function cancelEdit(idx) {
  const tr = document.getElementById('row-' + idx);
  if (!tr) return;
  const nameCell = tr.querySelector('.name-cell');
  const ccCell = tr.querySelector('.cc-cell');
  const actionCell = tr.querySelector('.action-cell');
  nameCell.textContent = tr.dataset.name;
  ccCell.textContent = tr.dataset.creditCode;
  actionCell.innerHTML = `<button class="btn btn-primary btn-sm" onclick="editRow(this, ${idx})">编辑</button>`;
}

async function retryRow(idx) {
  const tr = document.getElementById('row-' + idx);
  if (!tr) return;

  const name = document.getElementById('edit-name-' + idx).value.trim();
  const creditCode = document.getElementById('edit-cc-' + idx).value.trim();
  const companyName = tr.dataset.companyName;
  const url = tr.dataset.url;

  if (!name) { alert('简称不能为空'); return; }

  const actionCell = tr.querySelector('.action-cell');
  actionCell.innerHTML = '<span style="font-size:12px;color:var(--text2);">重跑中...</span>';

  try {
    const resp = await fetch('/api/retry', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name, company_name: companyName, credit_code: creditCode, url }),
    });
    const data = await resp.json();
    if (data.ok) {
      tr.dataset.name = name;
      tr.dataset.creditCode = creditCode;
      tr.querySelector('.name-cell').textContent = name;
      tr.querySelector('.cc-cell').textContent = creditCode;
      tr.querySelector('.status-cell').innerHTML = '<span class="badge badge-success">重跑成功</span>';
      tr.querySelector('.msg-cell').textContent = data.message;
      // 加复选框
      const companyId = data.company_id;
      if (companyId) {
        const firstTd = tr.querySelector('td');
        firstTd.innerHTML = `<input type="checkbox" class="gen-check" data-id="${esc(String(companyId))}" data-name="${esc(name)}" onchange="updateGenCount()">`;
        createdCompanies.push({ id: companyId, name, index: idx });
      }
      actionCell.innerHTML = '';
      // 更新统计
      stats.failed--;
      stats.success++;
      document.getElementById('stat-success').textContent = stats.success;
      document.getElementById('stat-failed').textContent = stats.failed;
      updateGenSection();
    } else {
      tr.querySelector('.msg-cell').textContent = data.message;
      actionCell.innerHTML = `<button class="btn btn-primary btn-sm" onclick="editRow(this, ${idx})">编辑</button>`;
    }
  } catch (e) {
    tr.querySelector('.msg-cell').textContent = '请求异常: ' + e.message;
    actionCell.innerHTML = `<button class="btn btn-primary btn-sm" onclick="editRow(this, ${idx})">编辑</button>`;
  }
}

function addFailItem(d) {
  document.getElementById('fail-section').classList.add('visible');
  const list = document.getElementById('fail-list');
  const div = document.createElement('div');
  div.className = 'fail-item';
  div.innerHTML = `<div class="fail-name">${esc(d.company_name)}</div><div class="fail-reason">${esc(d.message)}</div>`;
  list.appendChild(div);
}

function updateStats(type) {
  if (type in stats) stats[type]++;
  document.getElementById('stat-success').textContent = stats.success;
  document.getElementById('stat-degraded').textContent = stats.degraded;
  document.getElementById('stat-skipped').textContent = stats.skipped;
  document.getElementById('stat-failed').textContent = stats.failed;
}

function updateProgress(current, total) {
  const pct = total > 0 ? Math.round(current / total * 100) : 0;
  document.getElementById('progress-bar').style.width = pct + '%';
  document.getElementById('progress-text').textContent = `${current} / ${total} (${pct}%)`;
}

async function stopBatch() {
  document.getElementById('btn-stop').disabled = true;
  await fetch('/api/stop', { method: 'POST' });
}

function batchFinished() {
  document.getElementById('btn-start').disabled = false;
  document.getElementById('btn-stop').style.display = 'none';
  document.getElementById('btn-stop').disabled = false;
  updateGenSection();
  // 自动 Generate
  if (document.getElementById('auto-generate').checked) {
    const ids = getAllIds();
    if (ids.length > 0) {
      const el = document.getElementById('status-msg');
      el.textContent = '创建完成，自动开始 Generate...';
      setTimeout(() => startGenerate(ids), 500);
    }
  }
}

function updateGenSection() {
  if (createdCompanies.length > 0) {
    document.getElementById('generate-section').classList.add('visible');
  }
  updateGenCount();
}

function toggleCheckAll(el) {
  document.querySelectorAll('.gen-check').forEach(cb => { cb.checked = el.checked; });
  updateGenCount();
}

function updateGenCount() {
  const checked = document.querySelectorAll('.gen-check:checked').length;
  const total = document.querySelectorAll('.gen-check').length;
  document.getElementById('gen-selected-count').textContent = `已选 ${checked} / ${total}`;
}

function getSelectedIds() {
  const ids = [];
  document.querySelectorAll('.gen-check:checked').forEach(cb => {
    ids.push(cb.dataset.id);
  });
  return ids;
}

function getAllIds() {
  const ids = [];
  document.querySelectorAll('.gen-check').forEach(cb => {
    ids.push(cb.dataset.id);
  });
  return ids;
}

function generateSelected() {
  const ids = getSelectedIds();
  if (!ids.length) { alert('请先勾选要 generate 的公司'); return; }
  startGenerate(ids);
}

function generateAll() {
  const ids = getAllIds();
  if (!ids.length) { alert('没有可 generate 的公司'); return; }
  startGenerate(ids);
}

function startGenerate(ids) {
  const log = document.getElementById('generate-log');
  const progress = document.getElementById('generate-progress');
  log.innerHTML = '';
  log.style.display = 'block';
  progress.style.display = 'block';
  document.getElementById('btn-gen-stop').style.display = '';
  document.getElementById('gen-progress-bar').style.width = '0%';
  document.getElementById('gen-progress-text').textContent = `0 / ${ids.length}`;

  // 用 POST + EventSource 不直接支持, 改用 fetch + ReadableStream
  generateES = new AbortController();
  fetch('/api/generate', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ ids }),
    signal: generateES.signal,
  }).then(resp => {
    const reader = resp.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';

    function read() {
      reader.read().then(({ done, value }) => {
        if (done) {
          document.getElementById('btn-gen-stop').style.display = 'none';
          return;
        }
        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n');
        buffer = lines.pop(); // 最后一行可能不完整
        for (const line of lines) {
          if (line.startsWith('data: ')) {
            const jsonStr = line.slice(6);
            try {
              const d = JSON.parse(jsonStr);
              handleGenerateEvent(d, ids.length);
            } catch(e) {}
          }
        }
        read();
      });
    }
    read();
  }).catch(e => {
    if (e.name !== 'AbortError') {
      addGenLog('连接异常: ' + e.message, 'gen-error');
    }
    document.getElementById('btn-gen-stop').style.display = 'none';
  });
}

function handleGenerateEvent(d, total) {
  if (d.index !== undefined) {
    const pct = total > 0 ? Math.round(d.index / total * 100) : 0;
    document.getElementById('gen-progress-bar').style.width = pct + '%';
    document.getElementById('gen-progress-text').textContent = `${d.index} / ${total} (${pct}%)`;
  }
  if (d.status === 'generating') {
    addGenLog(d.message, 'gen-running');
  } else if (d.status === 'success') {
    addGenLog(d.message, 'gen-success');
    // 更新表格行的 generate 状态
    markGenerated(d.company_id, true);
  } else if (d.status === 'error') {
    addGenLog(d.message, 'gen-error');
    markGenerated(d.company_id, false);
  }
  if (d.total !== undefined && d.index === undefined) {
    // generate_done
    addGenLog('全部 Generate 完成！', 'gen-success');
    document.getElementById('btn-gen-stop').style.display = 'none';
  }
}

function markGenerated(companyId, success) {
  // 找到对应复选框并标注
  document.querySelectorAll('.gen-check').forEach(cb => {
    if (cb.dataset.id === String(companyId)) {
      const tr = cb.closest('tr');
      if (tr) {
        const statusCell = tr.querySelector('.status-cell');
        if (success) {
          statusCell.innerHTML += ' <span class="badge badge-generate">已Generate</span>';
        } else {
          statusCell.innerHTML += ' <span class="badge badge-error">Generate失败</span>';
        }
      }
    }
  });
}

function addGenLog(msg, cls) {
  const log = document.getElementById('generate-log');
  const div = document.createElement('div');
  div.className = 'generate-log-item ' + (cls || '');
  div.textContent = msg;
  log.appendChild(div);
  log.scrollTop = log.scrollHeight;
}

async function stopGenerate() {
  if (generateES) { generateES.abort(); generateES = null; }
  await fetch('/api/generate/stop', { method: 'POST' });
  document.getElementById('btn-gen-stop').style.display = 'none';
  addGenLog('已手动停止 Generate', 'gen-error');
}

function esc(s) {
  if (!s) return '';
  const d = document.createElement('div');
  d.textContent = s;
  return d.innerHTML;
}
</script>
</body>
</html>
"""

# ── 启动 ──────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 50)
    print("  批量创建爬虫任务工具")
    print("  http://localhost:8899")
    print("=" * 50)
    app.run(host="0.0.0.0", port=8899, debug=False, threaded=True)
