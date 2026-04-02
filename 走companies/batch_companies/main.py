#!/usr/bin/env python3
"""
批量创建公司工具 - Companies 多批次版
支持从 MongoDB 渠道加载 / CSV 上传，完整工作流：查天眼查 → 创建公司 → 生成爬虫 → 执行爬虫
"""

import asyncio
import csv
import io
import json
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse
from uuid import uuid4

import httpx
from fastapi import FastAPI, UploadFile, File, Request
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

app = FastAPI(title="批量创建公司")
app.mount("/static", StaticFiles(directory=Path(__file__).parent / "static"), name="static")

# ── 配置 ──────────────────────────────────────────────
MONGO_URI = "mongodb://admin:ZG8uY0zzDuAqtGin@192.168.253.10:27017/"
DB_NAME = "company_one"
COMPANIES_API = "http://192.168.253.10:9097/api/companies"
TIANYANCHA_API = "http://192.168.253.10:8083/tianyancha/open/ic"
EXECUTIONS_API = "http://192.168.253.10:9097/api/executions"
PLATFORMS = ["mokahr", "dayi", "beisen", "feishu"]
CONCURRENCY_TYC = 3           # 天眼查查询并发（不走后端DB，可稍高）
CONCURRENCY_CREATE = 2        # 创建公司并发（写DB，需严格控制）
CONCURRENCY_GENERATE = 1      # 生成爬虫并发（最重操作，串行执行）
CONCURRENCY_CRAWL = 2         # 执行爬虫并发
SLEEP_AFTER_TYC = 1           # 天眼查查询后等待（秒）
SLEEP_AFTER_CREATE = 3        # 创建公司后等待（秒）
SLEEP_AFTER_GENERATE_START = 5  # 发起生成后等待（秒）
SLEEP_AFTER_CRAWL_START = 3   # 发起执行后等待（秒）
POLL_INTERVAL_GENERATE = 10   # 生成爬虫轮询间隔（秒）
POLL_INTERVAL_CRAWL = 10      # 执行爬虫轮询间隔（秒）
BATCHES_DIR = Path(__file__).parent / "batches"


# ── URL 反推规则（复用 export_companies.py 逻辑） ─────────

def derive_url_mokahr(job_url, _code):
    marker = "#/job/"
    idx = job_url.find(marker)
    if idx != -1:
        return job_url[:idx] + "#/"
    marker2 = "#/"
    idx2 = job_url.find(marker2)
    if idx2 != -1:
        return job_url[:idx2] + "#/"
    return job_url


def derive_url_dayi(job_url, _code):
    parsed = urlparse(job_url)
    path = parsed.path
    if path.startswith("/wecruit/"):
        path = path[len("/wecruit"):]
    pb_idx = path.find("/pb")
    if pb_idx != -1:
        base = path[:pb_idx]
    else:
        base = path
    return f"{parsed.scheme}://{parsed.netloc}{base}/pb/index.html#/social"


def derive_url_beisen(job_url, company_code):
    parsed = urlparse(job_url)
    host = parsed.netloc
    return f"{parsed.scheme}://{host}/social/jobs"


def derive_url_feishu(job_url, _code):
    marker = "/position"
    idx = job_url.find(marker)
    if idx != -1:
        return job_url[:idx]
    return job_url


DERIVE_FUNCS = {
    "mokahr": derive_url_mokahr,
    "dayi": derive_url_dayi,
    "beisen": derive_url_beisen,
    "feishu": derive_url_feishu,
}


# ── 批次存储辅助函数 ─────────────────────────────────

def generate_batch_id() -> str:
    return f"b_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:4]}"


def load_batch(batch_id: str) -> dict:
    fp = BATCHES_DIR / f"{batch_id}.json"
    if fp.exists():
        try:
            return json.loads(fp.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def save_batch(batch_id: str, state: dict):
    BATCHES_DIR.mkdir(parents=True, exist_ok=True)
    fp = BATCHES_DIR / f"{batch_id}.json"
    fp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def delete_batch_file(batch_id: str) -> bool:
    fp = BATCHES_DIR / f"{batch_id}.json"
    if fp.exists():
        fp.unlink()
        return True
    return False


def compute_batch_status(rows: list) -> str:
    if not rows:
        return "空批次"
    statuses = [r.get("status", "") for r in rows]
    total = len(statuses)
    active = [s for s in statuses if s != "已存在，跳过"]
    if not active:
        return "全部已存在"

    if all(s in ("全部完成", "已存在，跳过") for s in statuses):
        return "全部完成"

    crawling = sum(1 for s in statuses if s == "爬虫执行中")
    if crawling:
        return f"爬虫执行中 ({crawling}/{total})"

    generated = sum(1 for s in statuses if s == "爬虫已生成")
    if generated:
        return "爬虫已生成"

    generating = sum(1 for s in statuses if s == "爬虫生成中")
    if generating:
        return f"爬虫生成中 ({generating}/{total})"

    creating = sum(1 for s in statuses if s == "创建中")
    if creating:
        return f"创建中 ({creating}/{total})"

    created = sum(1 for s in statuses if s in ("创建成功", "等待生成爬虫"))
    if created:
        return "公司已创建"

    wait_create = sum(1 for s in statuses if s == "等待创建公司")
    if wait_create:
        return "天眼查已完成"

    wait_tyc = sum(1 for s in statuses if s == "等待查询天眼查")
    if wait_tyc == len(active):
        return "等待查询天眼查"
    if wait_tyc:
        return f"天眼查查询中 ({len(active) - wait_tyc}/{len(active)})"

    return "进行中"


def list_all_batches() -> list[dict]:
    BATCHES_DIR.mkdir(parents=True, exist_ok=True)
    batches = []
    for fp in BATCHES_DIR.glob("*.json"):
        try:
            data = json.loads(fp.read_text(encoding="utf-8"))
            rows = data.get("rows", [])
            batches.append({
                "batch_id": data.get("batch_id", fp.stem),
                "created_at": data.get("created_at", ""),
                "source": data.get("source", ""),
                "source_detail": data.get("source_detail", ""),
                "note": data.get("note", ""),
                "row_count": len(rows),
                "status": compute_batch_status(rows),
            })
        except Exception:
            continue
    batches.sort(key=lambda x: x.get("created_at", ""), reverse=True)
    return batches


# ── 请求模型 ──────────────────────────────────────────

class NoteBody(BaseModel):
    note: str


class SaveRowsBody(BaseModel):
    rows: list[dict]


# ── 页面 ─────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def index():
    html_path = Path(__file__).parent / "index.html"
    return html_path.read_text(encoding="utf-8")


# ── 渠道统计 ──────────────────────────────────────────

@app.get("/api/platforms")
async def get_platforms():
    """返回各渠道可用公司数量"""
    import pymongo
    counts = {}
    try:
        client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        for p in PLATFORMS:
            coll = client[DB_NAME][p]
            pipeline = [
                {"$group": {"_id": "$company_name"}},
                {"$count": "total"},
            ]
            result = list(coll.aggregate(pipeline))
            counts[p] = result[0]["total"] if result else 0
        client.close()
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
    return {"platforms": counts}


# ── 从 MongoDB 加载渠道数据 ────────────────────────────

@app.post("/api/load/{platform}")
async def load_platform(platform: str):
    if platform not in PLATFORMS:
        return JSONResponse({"error": f"不支持的渠道: {platform}"}, status_code=400)

    import pymongo

    # 1. 从 MongoDB 拉取公司数据
    try:
        client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        coll = client[DB_NAME][platform]
        pipeline = [
            {"$group": {
                "_id": "$company_name",
                "job_url": {"$first": "$job_url"},
                "company_code": {"$first": "$company_code"},
            }},
            {"$sort": {"_id": 1}},
        ]
        results = list(coll.aggregate(pipeline))
        results = [r for r in results if r["_id"] and r.get("job_url")]
        client.close()
    except Exception as e:
        return JSONResponse({"error": f"MongoDB 连接失败: {e}"}, status_code=500)

    if not results:
        return JSONResponse({"error": f"{platform} 没有数据"}, status_code=404)

    # 2. URL 反推
    derive = DERIVE_FUNCS[platform]
    company_list = []
    for doc in results:
        name = doc["_id"]
        job_url = doc["job_url"]
        company_code = doc.get("company_code", "")
        url = derive(job_url, company_code)
        company_list.append({"company_name": name, "url": url, "channel": platform})

    # 3. 检查平台已有公司
    existing_map = {}  # url -> crawler_status
    try:
        async with httpx.AsyncClient() as http_client:
            page = 1
            while True:
                resp = await http_client.get(
                    COMPANIES_API,
                    params={"page": page, "page_size": 10000},
                    timeout=30,
                )
                data = resp.json()
                items = data.get("items", [])
                for item in items:
                    if item.get("url"):
                        existing_map[item["url"]] = item.get("crawler_status", "")
                if len(items) < 10000:
                    break
                page += 1
    except Exception:
        pass

    # 4. 构建行数据
    rows = []
    for c in company_list:
        url = c["url"]
        if url in existing_map and existing_map[url] == "ready":
            status = "已存在，跳过"
        else:
            status = "等待查询天眼查"

        rows.append({
            "company_name": c["company_name"],
            "url": c["url"],
            "channel": c["channel"],
            "alias": None,
            "name": None,
            "credit_code": None,
            "city": None,
            "company_id": None,
            "site_type": None,
            "job_url_example": None,
            "description": None,
            "status": status,
            "error": None,
        })

    # 5. 创建批次
    batch_id = generate_batch_id()
    state = {
        "batch_id": batch_id,
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source": platform,
        "source_detail": f"MongoDB {platform}",
        "note": "",
        "rows": rows,
    }
    save_batch(batch_id, state)

    skipped = sum(1 for r in rows if r["status"] == "已存在，跳过")
    return {
        "batch_id": batch_id,
        "count": len(rows),
        "skipped": skipped,
        "rows": rows,
        "created_at": state["created_at"],
    }


# ── CSV 上传 ──────────────────────────────────────────

@app.post("/api/upload")
async def upload_csv(file: UploadFile = File(...)):
    content = await file.read()
    text = content.decode("utf-8-sig")

    first_line = text.split("\n")[0]
    delimiter = "\t" if "\t" in first_line else ","

    reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
    headers = [h.strip() for h in reader.fieldnames or []]

    if "company_name" not in headers:
        return JSONResponse({"error": "CSV 中缺少 company_name 列"}, status_code=400)
    if "url" not in headers:
        return JSONResponse({"error": "CSV 中缺少 url 列"}, status_code=400)

    rows = []
    seen = set()
    for row in reader:
        name = (row.get("company_name") or "").strip()
        url = (row.get("url") or "").strip()
        if name and url and name not in seen:
            rows.append({
                "company_name": name,
                "url": url,
                "channel": None,
                "alias": None,
                "name": None,
                "credit_code": None,
                "city": None,
                "company_id": None,
                "site_type": None,
                "job_url_example": None,
                "description": None,
                "status": "等待查询天眼查",
                "error": None,
            })
            seen.add(name)

    if not rows:
        return JSONResponse({"error": "CSV 中没有有效数据"}, status_code=400)

    batch_id = generate_batch_id()
    state = {
        "batch_id": batch_id,
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source": "csv",
        "source_detail": file.filename or "",
        "note": "",
        "rows": rows,
    }
    save_batch(batch_id, state)
    return {
        "batch_id": batch_id,
        "count": len(rows),
        "rows": rows,
        "created_at": state["created_at"],
    }


# ── CSV 模板 ─────────────────────────────────────────

@app.get("/api/template")
async def download_template():
    csv_content = "company_name,url\n腾讯科技（深圳）有限公司,https://example.com/jobs\n"
    return StreamingResponse(
        io.BytesIO(("\uFEFF" + csv_content).encode("utf-8")),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=template.csv"},
    )


# ── 批次列表 ──────────────────────────────────────────

@app.get("/api/batches")
async def get_batches():
    return {"batches": list_all_batches()}


@app.get("/api/batches/{batch_id}")
async def get_batch(batch_id: str):
    state = load_batch(batch_id)
    if not state:
        return JSONResponse({"error": "批次不存在"}, status_code=404)
    return state


@app.delete("/api/batches/{batch_id}")
async def remove_batch(batch_id: str):
    deleted = delete_batch_file(batch_id)
    return {"ok": deleted}


@app.post("/api/batches/{batch_id}/note")
async def save_note(batch_id: str, body: NoteBody):
    state = load_batch(batch_id)
    if not state:
        return JSONResponse({"error": "批次不存在"}, status_code=404)
    state["note"] = body.note
    save_batch(batch_id, state)
    return {"ok": True}


@app.post("/api/batches/{batch_id}/save-rows")
async def save_rows(batch_id: str, body: SaveRowsBody):
    state = load_batch(batch_id)
    if not state:
        return JSONResponse({"error": "批次不存在"}, status_code=404)
    state["rows"] = body.rows
    save_batch(batch_id, state)
    return {"ok": True}


# ── 查询天眼查（SSE） ─────────────────────────────────

@app.get("/api/batches/{batch_id}/query-tyc")
async def query_tianyancha(batch_id: str, request: Request):
    indices_param = request.query_params.get("indices", "")
    state = load_batch(batch_id)
    rows = state.get("rows", [])

    if indices_param:
        target_indices = [int(x) for x in indices_param.split(",") if x.strip().isdigit()]
    else:
        target_indices = [i for i, r in enumerate(rows) if r["status"] == "等待查询天眼查"]

    if not target_indices:
        async def empty():
            yield f"data: {json.dumps({'type': 'done', 'total': 0, 'success': 0, 'not_found': 0, 'error': 0}, ensure_ascii=False)}\n\n"
        return StreamingResponse(empty(), media_type="text/event-stream")

    async def event_stream():
        sem = asyncio.Semaphore(CONCURRENCY_TYC)
        queue = asyncio.Queue()
        total = len(target_indices)

        async def query_one(idx: int, company_name: str, client: httpx.AsyncClient):
            async with sem:
                try:
                    resp = await client.get(
                        TIANYANCHA_API,
                        params={"keyword": company_name},
                        headers={"Content-Type": "application/json"},
                        timeout=15,
                    )
                    data = resp.json()
                    rd = data.get("data", {}).get("result")
                    err_code = data.get("data", {}).get("error_code", -1)

                    if rd and err_code == 0:
                        credit_code = rd.get("creditCode") or None
                        alias = (rd.get("alias") or "").strip() or None
                        city = (rd.get("city") or "").strip() or None

                        result = {
                            "idx": idx,
                            "alias": alias,
                            "credit_code": credit_code,
                            "city": city,
                            "status": "天眼查已查到",
                        }
                    else:
                        result = {
                            "idx": idx,
                            "alias": None,
                            "credit_code": None,
                            "city": None,
                            "status": "天眼查未查到",
                        }
                except Exception as exc:
                    result = {
                        "idx": idx,
                        "alias": None,
                        "credit_code": None,
                        "city": None,
                        "status": "天眼查出错",
                        "error": str(exc),
                    }
                await asyncio.sleep(SLEEP_AFTER_TYC)
                await queue.put(result)

        async with httpx.AsyncClient() as client:
            tasks = [
                asyncio.create_task(query_one(idx, rows[idx]["company_name"], client))
                for idx in target_indices
            ]

            success_count = 0
            not_found_count = 0
            error_count = 0

            for i in range(total):
                item = await queue.get()
                idx = item["idx"]
                rows[idx]["alias"] = item.get("alias")
                rows[idx]["credit_code"] = item.get("credit_code")
                rows[idx]["city"] = item.get("city")
                rows[idx]["status"] = item["status"]
                if item.get("error"):
                    rows[idx]["error"] = item["error"]

                if item["status"] == "天眼查已查到":
                    success_count += 1
                elif item["status"] == "天眼查未查到":
                    not_found_count += 1
                else:
                    error_count += 1

                payload = {
                    "type": "progress",
                    "completed": i + 1,
                    "total": total,
                    "idx": idx,
                    "row": rows[idx],
                }
                yield f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"

            await asyncio.gather(*tasks, return_exceptions=True)

        # 更新状态：天眼查完成的转为等待创建公司
        for idx in target_indices:
            if rows[idx]["status"] in ("天眼查已查到", "天眼查未查到"):
                rows[idx]["status"] = "等待创建公司"

        state["rows"] = rows
        save_batch(batch_id, state)

        summary = {
            "type": "done",
            "total": total,
            "success": success_count,
            "not_found": not_found_count,
            "error": error_count,
            "rows": rows,
        }
        yield f"data: {json.dumps(summary, ensure_ascii=False)}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")


# ── 批量创建公司（SSE） ───────────────────────────────

def compute_final_name(row, existing_names: set) -> str:
    """计算最终公司名称，处理冲突降级"""
    alias = row.get("alias")
    company_name = row["company_name"]
    channel = row.get("channel")
    city = row.get("city")

    if channel:
        # MongoDB 加载：名称格式 简称_渠道
        if alias:
            candidate = f"{alias}_{channel}"
            if candidate not in existing_names:
                return candidate
            # 冲突 → 简称（城市）_渠道
            if city:
                candidate = f"{alias}（{city}）_{channel}"
                if candidate not in existing_names:
                    return candidate
            # 冲突 → 全称_渠道
            return f"{company_name}_{channel}"
        else:
            # 无简称：直接全称_渠道
            return f"{company_name}_{channel}"
    else:
        # CSV 上传：直接用简称
        if alias:
            candidate = alias
            if candidate not in existing_names:
                return candidate
            if city:
                candidate = f"{alias}（{city}）"
                if candidate not in existing_names:
                    return candidate
            return company_name
        else:
            return company_name


@app.get("/api/batches/{batch_id}/create")
async def create_companies(batch_id: str, request: Request):
    indices_param = request.query_params.get("indices", "")
    state = load_batch(batch_id)
    rows = state.get("rows", [])

    if indices_param:
        target_indices = [int(x) for x in indices_param.split(",") if x.strip().isdigit()]
    else:
        target_indices = [i for i, r in enumerate(rows) if r["status"] == "等待创建公司"]

    if not target_indices:
        async def empty():
            yield f"data: {json.dumps({'type': 'done', 'total': 0, 'created': 0, 'skipped': 0, 'failed': 0}, ensure_ascii=False)}\n\n"
        return StreamingResponse(empty(), media_type="text/event-stream")

    async def event_stream():
        sem = asyncio.Semaphore(CONCURRENCY_CREATE)
        total = len(target_indices)

        # 先拉取平台已有公司的 name 和 url
        existing_names = set()
        existing_urls = set()
        try:
            async with httpx.AsyncClient() as client:
                page = 1
                while True:
                    resp = await client.get(
                        COMPANIES_API,
                        params={"page": page, "page_size": 10000},
                        timeout=30,
                    )
                    data = resp.json()
                    items = data.get("items", [])
                    for item in items:
                        if item.get("name"):
                            existing_names.add(item["name"])
                        if item.get("url"):
                            existing_urls.add(item["url"])
                    if len(items) < 10000:
                        break
                    page += 1
        except Exception:
            pass

        created_count = 0
        skipped_count = 0
        failed_count = 0

        async def create_one(idx: int, client: httpx.AsyncClient):
            nonlocal created_count, skipped_count, failed_count
            async with sem:
                row = rows[idx]
                url = row["url"]

                # URL 去重
                if url in existing_urls:
                    row["status"] = "已跳过(URL重复)"
                    skipped_count += 1
                    return

                # 计算最终名称
                final_name = compute_final_name(row, existing_names)
                row["name"] = final_name

                # 名称去重
                if final_name in existing_names:
                    row["status"] = "已跳过(名称重复)"
                    skipped_count += 1
                    return

                # 创建公司
                payload = {
                    "name": final_name,
                    "url": url,
                }
                if row.get("company_name"):
                    payload["company_name"] = row["company_name"]
                if row.get("credit_code"):
                    payload["credit_code"] = row["credit_code"]
                if row.get("site_type"):
                    payload["site_type"] = row["site_type"]
                if row.get("job_url_example"):
                    payload["job_url_example"] = row["job_url_example"]
                if row.get("description"):
                    payload["description"] = row["description"]

                try:
                    resp = await client.post(
                        COMPANIES_API,
                        json=payload,
                        timeout=30,
                    )
                    data = resp.json()
                    if resp.status_code in (200, 201) and data.get("id"):
                        row["company_id"] = data["id"]
                        row["status"] = "等待生成爬虫"
                        existing_names.add(final_name)
                        existing_urls.add(url)
                        created_count += 1
                    else:
                        row["status"] = "创建失败"
                        row["error"] = data.get("error") or data.get("message") or str(data)
                        failed_count += 1
                except Exception as exc:
                    row["status"] = "创建失败"
                    row["error"] = str(exc)
                    failed_count += 1
                await asyncio.sleep(SLEEP_AFTER_CREATE)

        async with httpx.AsyncClient() as client:
            tasks_list = []
            for idx in target_indices:
                tasks_list.append(asyncio.create_task(create_one(idx, client)))

            # 逐个等待并报告进度
            for i, task in enumerate(asyncio.as_completed(tasks_list)):
                await task
                # 找到对应完成的行
                payload = {
                    "type": "progress",
                    "completed": i + 1,
                    "total": total,
                }
                yield f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"

        state["rows"] = rows
        save_batch(batch_id, state)

        summary = {
            "type": "done",
            "total": total,
            "created": created_count,
            "skipped": skipped_count,
            "failed": failed_count,
            "rows": rows,
        }
        yield f"data: {json.dumps(summary, ensure_ascii=False)}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")


# ── 生成爬虫（SSE） ──────────────────────────────────

@app.get("/api/batches/{batch_id}/generate")
async def generate_crawlers(batch_id: str, request: Request):
    indices_param = request.query_params.get("indices", "")
    state = load_batch(batch_id)
    rows = state.get("rows", [])

    if indices_param:
        target_indices = [int(x) for x in indices_param.split(",") if x.strip().isdigit()]
    else:
        target_indices = [i for i, r in enumerate(rows) if r["status"] == "等待生成爬虫"]

    if not target_indices:
        async def empty():
            yield f"data: {json.dumps({'type': 'done', 'total': 0, 'success': 0, 'failed': 0}, ensure_ascii=False)}\n\n"
        return StreamingResponse(empty(), media_type="text/event-stream")

    async def event_stream():
        sem_gen = asyncio.Semaphore(CONCURRENCY_GENERATE)
        total = len(target_indices)
        success_count = 0
        failed_count = 0

        async def generate_one(idx: int, client: httpx.AsyncClient):
            nonlocal success_count, failed_count
            async with sem_gen:
                row = rows[idx]
                company_id = row.get("company_id")
                if not company_id:
                    row["status"] = "生成失败"
                    row["error"] = "无 company_id"
                    failed_count += 1
                    return

                try:
                    row["status"] = "爬虫生成中"
                    resp = await client.post(
                        f"{COMPANIES_API}/{company_id}/generate",
                        json={"timeout": 10800, "verbose": True},
                        timeout=60,
                    )
                    data = resp.json()
                    execution_id = data.get("execution_id") or data.get("id")

                    if not execution_id:
                        row["status"] = "生成失败"
                        row["error"] = data.get("error") or str(data)
                        failed_count += 1
                        return

                    await asyncio.sleep(SLEEP_AFTER_GENERATE_START)
                    # 轮询执行状态（间隔10s，最多轮询120次≈20分钟）
                    for _ in range(120):
                        await asyncio.sleep(POLL_INTERVAL_GENERATE)
                        try:
                            poll_resp = await client.get(
                                f"{EXECUTIONS_API}/{execution_id}",
                                timeout=15,
                            )
                            poll_data = poll_resp.json()
                            exec_status = poll_data.get("status", "")
                            if exec_status == "completed":
                                row["status"] = "爬虫已生成"
                                success_count += 1
                                return
                            elif exec_status in ("failed", "error"):
                                row["status"] = "生成失败"
                                row["error"] = poll_data.get("error") or "执行失败"
                                failed_count += 1
                                return
                        except Exception:
                            continue

                    row["status"] = "生成失败"
                    row["error"] = "轮询超时"
                    failed_count += 1

                except Exception as exc:
                    row["status"] = "生成失败"
                    row["error"] = str(exc)
                    failed_count += 1

        async with httpx.AsyncClient() as client:
            tasks_list = []
            for idx in target_indices:
                tasks_list.append(asyncio.create_task(generate_one(idx, client)))

            for i, task in enumerate(asyncio.as_completed(tasks_list)):
                await task
                payload = {
                    "type": "progress",
                    "completed": i + 1,
                    "total": total,
                }
                yield f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"

        # 更新等待执行爬虫
        for idx in target_indices:
            if rows[idx]["status"] == "爬虫已生成":
                rows[idx]["status"] = "等待执行爬虫"

        state["rows"] = rows
        save_batch(batch_id, state)

        summary = {
            "type": "done",
            "total": total,
            "success": success_count,
            "failed": failed_count,
            "rows": rows,
        }
        yield f"data: {json.dumps(summary, ensure_ascii=False)}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")


# ── 执行爬虫（SSE） ──────────────────────────────────

@app.get("/api/batches/{batch_id}/crawl")
async def execute_crawlers(batch_id: str, request: Request):
    indices_param = request.query_params.get("indices", "")
    state = load_batch(batch_id)
    rows = state.get("rows", [])

    if indices_param:
        target_indices = [int(x) for x in indices_param.split(",") if x.strip().isdigit()]
    else:
        target_indices = [i for i, r in enumerate(rows) if r["status"] == "等待执行爬虫"]

    if not target_indices:
        async def empty():
            yield f"data: {json.dumps({'type': 'done', 'total': 0, 'success': 0, 'failed': 0}, ensure_ascii=False)}\n\n"
        return StreamingResponse(empty(), media_type="text/event-stream")

    async def event_stream():
        sem_crawl = asyncio.Semaphore(CONCURRENCY_CRAWL)
        total = len(target_indices)
        success_count = 0
        failed_count = 0

        async def crawl_one(idx: int, client: httpx.AsyncClient):
            nonlocal success_count, failed_count
            async with sem_crawl:
                row = rows[idx]
                company_id = row.get("company_id")
                if not company_id:
                    row["status"] = "执行失败"
                    row["error"] = "无 company_id"
                    failed_count += 1
                    return

                try:
                    row["status"] = "爬虫执行中"
                    resp = await client.post(
                        f"{COMPANIES_API}/{company_id}/crawl",
                        timeout=60,
                    )
                    data = resp.json()
                    execution_id = data.get("execution_id") or data.get("id")

                    if not execution_id:
                        row["status"] = "执行失败"
                        row["error"] = data.get("error") or str(data)
                        failed_count += 1
                        return

                    await asyncio.sleep(SLEEP_AFTER_CRAWL_START)
                    # 轮询执行状态（间隔10s，最多轮询200次≈33分钟）
                    for _ in range(200):
                        await asyncio.sleep(POLL_INTERVAL_CRAWL)
                        try:
                            poll_resp = await client.get(
                                f"{EXECUTIONS_API}/{execution_id}",
                                timeout=15,
                            )
                            poll_data = poll_resp.json()
                            exec_status = poll_data.get("status", "")
                            if exec_status == "completed":
                                row["status"] = "全部完成"
                                success_count += 1
                                return
                            elif exec_status in ("failed", "error"):
                                row["status"] = "执行失败"
                                row["error"] = poll_data.get("error") or "执行失败"
                                failed_count += 1
                                return
                        except Exception:
                            continue

                    row["status"] = "执行失败"
                    row["error"] = "轮询超时"
                    failed_count += 1

                except Exception as exc:
                    row["status"] = "执行失败"
                    row["error"] = str(exc)
                    failed_count += 1

        async with httpx.AsyncClient() as client:
            tasks_list = []
            for idx in target_indices:
                tasks_list.append(asyncio.create_task(crawl_one(idx, client)))

            for i, task in enumerate(asyncio.as_completed(tasks_list)):
                await task
                payload = {
                    "type": "progress",
                    "completed": i + 1,
                    "total": total,
                }
                yield f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"

        state["rows"] = rows
        save_batch(batch_id, state)

        summary = {
            "type": "done",
            "total": total,
            "success": success_count,
            "failed": failed_count,
            "rows": rows,
        }
        yield f"data: {json.dumps(summary, ensure_ascii=False)}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")


# ── 启动 ─────────────────────────────────────────────

@app.on_event("startup")
async def startup():
    BATCHES_DIR.mkdir(parents=True, exist_ok=True)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8877)
