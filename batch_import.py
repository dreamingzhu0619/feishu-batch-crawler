#!/usr/bin/env python3
"""
Feishu 公司批量导入自动化脚本

流程: 本地 JSON 读取 → 数据补全(天眼查) → 平台导入 → 生成爬虫 → 运行爬虫
附带实时 Web 看板: http://localhost:8888/dashboard
"""

import argparse
import asyncio
import enum
import json
import logging
import os
import re
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
from urllib.parse import urlsplit

import httpx
import uvicorn
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  配置常量
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

DATA_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "feishu_companies.json")

PLATFORM_BASE_URL = "http://localhost:6666"
TIANYANCHA_URL = "http://192.168.253.10:8083/tianyancha/open/ic"
DASHBOARD_PORT = 8888

POLL_INTERVAL = 10        # 秒, 轮询间隔
SUBMIT_INTERVAL = 1       # 秒, 提交间隔
MAX_RETRIES = 3           # 网络重试次数
REQUEST_TIMEOUT = 30      # 秒, HTTP 请求超时
MAX_GLOBAL_LOGS = 500     # 全局日志最大条数

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("batch_import")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  内嵌 HTML 看板
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Feishu Batch Import Dashboard</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,monospace;background:#0f1117;color:#e0e0e0;min-height:100vh}
.header{display:flex;justify-content:space-between;align-items:center;padding:12px 24px;background:#161822;border-bottom:1px solid #2a2d3a}
.header h1{font-size:18px;color:#58a6ff}
.header .runtime{color:#8b949e;font-size:13px}
.controls{display:flex;gap:8px}
.controls button{padding:6px 16px;border:none;border-radius:4px;cursor:pointer;font-size:13px;font-weight:600;transition:.2s}
.btn-pause{background:#d29922;color:#000}.btn-pause:hover{background:#e3b341}
.btn-resume{background:#238636;color:#fff}.btn-resume:hover{background:#2ea043}
.btn-stop{background:#da3633;color:#fff}.btn-stop:hover{background:#f85149}
.btn-pause:disabled,.btn-resume:disabled,.btn-stop:disabled{opacity:.4;cursor:not-allowed}
.cards{display:flex;gap:12px;padding:16px 24px;flex-wrap:wrap}
.card{background:#161822;border:1px solid #2a2d3a;border-radius:8px;padding:14px 20px;min-width:120px;text-align:center}
.card .num{font-size:28px;font-weight:700}.card .lbl{font-size:11px;color:#8b949e;margin-top:2px}
.c-total .num{color:#58a6ff}.c-done .num{color:#3fb950}.c-fail .num{color:#f85149}
.c-skip .num{color:#8b949e}.c-pend .num{color:#d29922}
.c-gen .num{color:#bc8cff}.c-genq .num{color:#7c7f8a}.c-gend .num{color:#79c0ff}
.c-crawl .num{color:#f0883e}.c-crawlq .num{color:#7c7f8a}.c-crawld .num{color:#56d364}
.progress-section{padding:0 24px 12px}
.pbar-wrap{margin-bottom:8px}
.pbar-label{font-size:12px;color:#8b949e;margin-bottom:4px}
.pbar{height:20px;background:#21262d;border-radius:10px;overflow:hidden;display:flex}
.pbar span{height:100%;transition:width .5s}
.p-ok{background:#238636}.p-fail{background:#da3633}.p-run{background:#1f6feb}.p-queue{background:#30363d}
.filters{padding:8px 24px;display:flex;gap:6px;flex-wrap:wrap}
.filters button{padding:4px 12px;border:1px solid #30363d;border-radius:16px;background:transparent;color:#8b949e;cursor:pointer;font-size:12px}
.filters button.active{background:#1f6feb;color:#fff;border-color:#1f6feb}
.table-wrap{padding:0 24px;overflow-x:auto}
table{width:100%;border-collapse:collapse;font-size:13px}
th{background:#161822;padding:8px 12px;text-align:left;border-bottom:1px solid #2a2d3a;color:#8b949e;font-weight:600;position:sticky;top:0}
td{padding:8px 12px;border-bottom:1px solid #1c1f2b}
tr:hover{background:#1c2030}
.badge{display:inline-block;padding:2px 8px;border-radius:10px;font-size:11px;font-weight:600}
.b-completed{background:#0d3117;color:#3fb950}.b-failed{background:#3c1418;color:#f85149}
.b-skipped{background:#1c1f2b;color:#8b949e}.b-pending{background:#2d1b00;color:#d29922}
.b-enriching,.b-creating{background:#0c2d6b;color:#58a6ff}
.b-generating,.b-waiting_generate{background:#271052;color:#bc8cff}
.b-running,.b-waiting_run{background:#3d1d00;color:#f0883e}
.log-btn{padding:2px 8px;background:#21262d;border:1px solid #30363d;border-radius:4px;color:#58a6ff;cursor:pointer;font-size:12px}
.log-panel{background:#0d1117;border-top:1px solid #2a2d3a;height:300px;overflow-y:auto;padding:12px 24px;font-family:'Fira Code',Consolas,monospace;font-size:12px;line-height:1.6}
.log-panel .l{white-space:pre-wrap;word-break:break-all}.l-err{color:#f85149}.l-warn{color:#d29922}.l-ok{color:#3fb950}
.modal-overlay{display:none;position:fixed;inset:0;background:rgba(0,0,0,.6);z-index:100;align-items:center;justify-content:center}
.modal-overlay.show{display:flex}
.modal{background:#161822;border:1px solid #2a2d3a;border-radius:12px;padding:24px;max-width:600px;width:90%;max-height:70vh;display:flex;flex-direction:column}
.modal h3{margin-bottom:12px;color:#58a6ff}.modal .modal-logs{flex:1;overflow-y:auto;font-family:monospace;font-size:12px;line-height:1.6;background:#0d1117;padding:12px;border-radius:6px}
.modal .close-btn{margin-top:12px;align-self:flex-end;padding:6px 16px;background:#21262d;border:1px solid #30363d;border-radius:4px;color:#e0e0e0;cursor:pointer}
.confirm-overlay{display:none;position:fixed;inset:0;background:rgba(0,0,0,.6);z-index:200;align-items:center;justify-content:center}
.confirm-overlay.show{display:flex}
.confirm-box{background:#161822;border:1px solid #da3633;border-radius:12px;padding:24px;text-align:center}
.confirm-box p{margin-bottom:16px;font-size:14px}
.confirm-box button{padding:6px 20px;border:none;border-radius:4px;cursor:pointer;font-size:13px;margin:0 6px}
.confirm-yes{background:#da3633;color:#fff}.confirm-no{background:#21262d;color:#e0e0e0;border:1px solid #30363d}
</style>
</head>
<body>
<div class="header">
  <h1>Feishu Batch Import</h1>
  <span class="runtime" id="runtime">--:--:--</span>
  <div class="controls">
    <button class="btn-pause" id="btnPause" title="暂停提交新任务，已提交到平台的任务继续执行">⏸ Pause</button>
    <button class="btn-resume" id="btnResume" style="display:none" title="恢复提交新任务">▶ Resume</button>
    <button class="btn-stop" id="btnStop" title="取消所有排队任务并退出脚本">⏹ Stop</button>
  </div>
</div>
<div class="cards" id="cards"></div>
<div class="progress-section">
  <div class="pbar-wrap"><div class="pbar-label" id="genLabel">Generate: 0/0</div><div class="pbar" id="genBar"></div></div>
  <div class="pbar-wrap"><div class="pbar-label" id="crawlLabel">Crawl: 0/0</div><div class="pbar" id="crawlBar"></div></div>
</div>
<div class="filters" id="filters"></div>
<div class="table-wrap"><table><thead><tr>
  <th>#</th><th>公司名</th><th>简称</th><th>状态</th><th>URL</th><th>平台ID</th><th>最新日志</th><th>操作</th>
</tr></thead><tbody id="tbody"></tbody></table></div>
<div class="log-panel" id="globalLogs"></div>

<div class="modal-overlay" id="modalOverlay">
  <div class="modal"><h3 id="modalTitle">Logs</h3><div class="modal-logs" id="modalLogs"></div>
  <button class="close-btn" onclick="document.getElementById('modalOverlay').classList.remove('show')">Close</button></div>
</div>
<div class="confirm-overlay" id="confirmOverlay">
  <div class="confirm-box"><p>Stop 将取消所有排队任务并退出脚本，确定？</p>
  <button class="confirm-yes" id="confirmYes">确定 Stop</button>
  <button class="confirm-no" onclick="document.getElementById('confirmOverlay').classList.remove('show')">取消</button></div>
</div>

<script>
const API='/api';
let filter='all',startTime=null,lastLogIdx=0,paused=false;

const FILTERS=['all','completed','failed','skipped','in_progress','pending'];
const CARD_CFG=[
  {k:'total',l:'Total',c:'c-total'},{k:'completed',l:'Completed',c:'c-done'},{k:'failed',l:'Failed',c:'c-fail'},
  {k:'skipped',l:'Skipped',c:'c-skip'},{k:'pending',l:'Pending',c:'c-pend'},
  {k:'generate_running',l:'Generating',c:'c-gen'},{k:'generate_queued',l:'Gen Queue',c:'c-genq'},
  {k:'generate_done',l:'Generated',c:'c-gend'},
  {k:'crawl_running',l:'Crawling',c:'c-crawl'},{k:'crawl_queued',l:'Crawl Queue',c:'c-crawlq'},
  {k:'crawl_done',l:'Crawled',c:'c-crawld'}
];
const IN_PROGRESS_STATES=['enriching','creating','generating','waiting_generate','running','waiting_run'];

// Init filters
const fDiv=document.getElementById('filters');
FILTERS.forEach(f=>{const b=document.createElement('button');b.textContent=f.replace('_',' ');b.className=f==='all'?'active':'';
  b.onclick=()=>{filter=f;fDiv.querySelectorAll('button').forEach(x=>x.classList.remove('active'));b.classList.add('active')};fDiv.appendChild(b)});

function esc(s){if(!s)return'';const d=document.createElement('div');d.textContent=s;return d.innerHTML}
function elapsed(ms){const s=Math.floor(ms/1000),m=Math.floor(s/60),h=Math.floor(m/60);return `${String(h).padStart(2,'0')}:${String(m%60).padStart(2,'0')}:${String(s%60).padStart(2,'0')}`}

function renderCards(s){
  const el=document.getElementById('cards');
  el.innerHTML=CARD_CFG.map(c=>`<div class="card ${c.c}"><div class="num">${s[c.k]||0}</div><div class="lbl">${c.l}</div></div>`).join('');
}

function renderBars(s){
  const total=s.total||1;
  const genDone=s.generate_done||0,genRun=s.generate_running||0,genFail=s.generate_failed||0,genQ=s.generate_queued||0;
  const crDone=s.crawl_done||0,crRun=s.crawl_running||0,crFail=s.crawl_failed||0,crQ=s.crawl_queued||0;
  document.getElementById('genLabel').textContent=`Generate: ${genDone}/${total} (${genRun} running, ${genQ} queued, ${genFail} failed)`;
  document.getElementById('crawlLabel').textContent=`Crawl: ${crDone}/${genDone||1} (${crRun} running, ${crQ} queued, ${crFail} failed)`;
  const genTotal=Math.max(total,1);
  document.getElementById('genBar').innerHTML=
    `<span class="p-ok" style="width:${genDone/genTotal*100}%"></span>`+
    `<span class="p-fail" style="width:${genFail/genTotal*100}%"></span>`+
    `<span class="p-run" style="width:${genRun/genTotal*100}%"></span>`+
    `<span class="p-queue" style="width:${genQ/genTotal*100}%"></span>`;
  const crTotal=Math.max(genDone,1);
  document.getElementById('crawlBar').innerHTML=
    `<span class="p-ok" style="width:${crDone/crTotal*100}%"></span>`+
    `<span class="p-fail" style="width:${crFail/crTotal*100}%"></span>`+
    `<span class="p-run" style="width:${crRun/crTotal*100}%"></span>`+
    `<span class="p-queue" style="width:${crQ/crTotal*100}%"></span>`;
}

function renderTable(tasks){
  const tb=document.getElementById('tbody');
  let filtered=tasks;
  if(filter==='in_progress') filtered=tasks.filter(t=>IN_PROGRESS_STATES.includes(t.state));
  else if(filter!=='all') filtered=tasks.filter(t=>t.state===filter);
  tb.innerHTML=filtered.map((t,i)=>{
    const st=t.state.toLowerCase();
    const lastLog=t.logs&&t.logs.length?t.logs[t.logs.length-1]:'';
    return `<tr>
      <td>${i+1}</td><td>${esc(t.company_name)}</td><td>${esc(t.short_name)}</td>
      <td><span class="badge b-${st}">${t.state}</span></td>
      <td style="max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap"><a href="${esc(t.code_url)}" target="_blank" style="color:#58a6ff">${esc(t.code_url)}</a></td>
      <td>${t.platform_company_id||'-'}</td>
      <td style="max-width:300px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:12px;color:#8b949e">${esc(lastLog)}</td>
      <td><button class="log-btn" onclick="showLogs('${esc(t.company_name)}')">Logs</button></td>
    </tr>`}).join('');
}

function renderGlobalLogs(logs){
  const el=document.getElementById('globalLogs');
  const atBottom=el.scrollHeight-el.scrollTop-el.clientHeight<50;
  if(logs.length>lastLogIdx){
    const frag=document.createDocumentFragment();
    for(let i=lastLogIdx;i<logs.length;i++){
      const d=document.createElement('div');d.className='l';
      const log=logs[i];
      if(log.includes('ERROR')||log.includes('FAILED'))d.classList.add('l-err');
      else if(log.includes('SKIP')||log.includes('WARN'))d.classList.add('l-warn');
      else if(log.includes('SUCCESS')||log.includes('COMPLETED'))d.classList.add('l-ok');
      d.textContent=log;frag.appendChild(d);
    }
    el.appendChild(frag);
    lastLogIdx=logs.length;
    if(atBottom)el.scrollTop=el.scrollHeight;
  }
}

async function showLogs(name){
  const r=await fetch(`${API}/logs/${encodeURIComponent(name)}`);
  const d=await r.json();
  document.getElementById('modalTitle').textContent=`Logs: ${name}`;
  document.getElementById('modalLogs').innerHTML=d.logs.map(l=>`<div class="l">${esc(l)}</div>`).join('');
  document.getElementById('modalOverlay').classList.add('show');
}

async function poll(){
  try{
    const r=await fetch(`${API}/status`);
    const d=await r.json();
    if(!startTime)startTime=new Date(d.start_time);
    document.getElementById('runtime').textContent=elapsed(Date.now()-startTime.getTime());
    renderCards(d.summary);renderBars(d.summary);renderTable(d.tasks);renderGlobalLogs(d.global_logs||[]);
    paused=d.paused||false;
    document.getElementById('btnPause').style.display=paused?'none':'';
    document.getElementById('btnResume').style.display=paused?'':'none';
  }catch(e){console.error('Poll error:',e)}
}

document.getElementById('btnPause').onclick=async()=>{await fetch(`${API}/control/pause`,{method:'POST'})};
document.getElementById('btnResume').onclick=async()=>{await fetch(`${API}/control/resume`,{method:'POST'})};
document.getElementById('btnStop').onclick=()=>document.getElementById('confirmOverlay').classList.add('show');
document.getElementById('confirmYes').onclick=async()=>{
  document.getElementById('confirmOverlay').classList.remove('show');
  await fetch(`${API}/control/stop`,{method:'POST'});
};

poll();setInterval(poll,3000);
</script>
</body>
</html>"""

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  状态枚举
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class CompanyState(str, enum.Enum):
    PENDING = "pending"
    ENRICHING = "enriching"
    CREATING = "creating"
    GENERATING = "generating"
    WAITING_GENERATE = "waiting_generate"
    RUNNING = "running"
    WAITING_RUN = "waiting_run"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  CompanyTask 数据类
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


@dataclass
class CompanyTask:
    company_name: str
    job_url: str
    code_url: str = ""
    state: CompanyState = CompanyState.PENDING
    short_name: str = ""
    credit_code: str = ""
    platform_company_id: Optional[int] = None
    generate_execution_id: Optional[int] = None
    crawl_execution_id: Optional[int] = None
    error_message: str = ""
    logs: list = field(default_factory=list)

    def log(self, msg: str):
        ts = datetime.now().strftime("%H:%M:%S")
        entry = f"[{ts}] {msg}"
        self.logs.append(entry)
        logger.info(f"[{self.short_name or self.company_name}] {msg}")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Name 生成工具
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_SUFFIXES = [
    "有限责任公司", "股份有限公司", "有限公司",
    "集团有限公司", "集团股份有限公司", "集团",
    "责任有限公司", "科技发展公司", "技术公司",
]
_PAREN_RE = re.compile(r"[（(][^）)]*[）)]")


def make_short_name(company_name: str) -> str:
    """从 company_name 生成简称, 如 '上海蔚来汽车有限公司' → '蔚来汽车'"""
    name = company_name.strip()
    # 去掉括号内容 (如 "（天津）")
    name = _PAREN_RE.sub("", name)
    # 去掉后缀
    for suf in sorted(_SUFFIXES, key=len, reverse=True):
        if name.endswith(suf):
            name = name[: -len(suf)]
            break
    # 去掉常见省市前缀: 只保留核心名
    # 如 "上海蔚来汽车" → 尝试去 "上海" 等
    _CITY_PREFIXES = [
        "北京", "上海", "广州", "深圳", "天津", "重庆",
        "杭州", "南京", "成都", "武汉", "西安", "苏州",
        "长沙", "郑州", "青岛", "大连", "厦门", "宁波",
        "合肥", "福州", "济南", "昆明", "贵阳", "珠海",
        "无锡", "东莞", "佛山", "中山", "惠州", "嘉兴",
    ]
    for city in _CITY_PREFIXES:
        if name.startswith(city) and len(name) > len(city) + 1:
            name = name[len(city):]
            break
    return name.strip()


def extract_code_url(job_url: str) -> str:
    """从 job_url 提取 code_url: https://{slug}.jobs.feishu.cn/index"""
    parts = urlsplit(job_url)
    return f"{parts.scheme}://{parts.netloc}/index"

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  PlatformClient — 封装平台 API
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class PlatformClient:
    def __init__(self, base_url: str = PLATFORM_BASE_URL):
        self._base = base_url.rstrip("/")
        self._client = httpx.AsyncClient(
            base_url=self._base,
            timeout=REQUEST_TIMEOUT,
        )
        self._tianyancha = httpx.AsyncClient(timeout=REQUEST_TIMEOUT)

    async def close(self):
        await self._client.aclose()
        await self._tianyancha.aclose()

    async def _request(self, method: str, path: str, **kwargs) -> dict:
        """带重试的请求"""
        last_exc = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = await self._client.request(method, path, **kwargs)
                if resp.status_code >= 500:
                    raise httpx.HTTPStatusError(
                        f"Server error {resp.status_code}",
                        request=resp.request,
                        response=resp,
                    )
                return {"_status": resp.status_code, **resp.json()}
            except (httpx.ConnectError, httpx.TimeoutException, httpx.HTTPStatusError) as e:
                last_exc = e
                if attempt < MAX_RETRIES:
                    wait = 2 ** attempt
                    logger.warning(f"Request {method} {path} failed (attempt {attempt}): {e}, retry in {wait}s")
                    await asyncio.sleep(wait)
        raise last_exc  # type: ignore

    async def search_tianyancha(self, keyword: str) -> dict:
        """直接调内网天眼查接口"""
        last_exc = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = await self._tianyancha.get(
                    TIANYANCHA_URL, params={"keyword": keyword},
                )
                data = resp.json()
                result = (data.get("data") or {}).get("result") or {}
                if not result or not isinstance(result, dict) or result.get("error_code"):
                    return {"items": []}
                return {"items": [{
                    "name": result.get("alias") or keyword,
                    "creditCode": result.get("creditCode", ""),
                    "regCapital": result.get("regCapital", ""),
                }]}
            except (httpx.ConnectError, httpx.TimeoutException) as e:
                last_exc = e
                if attempt < MAX_RETRIES:
                    await asyncio.sleep(2 ** attempt)
        raise last_exc  # type: ignore

    async def list_companies(self, search: str, page: int = 1, page_size: int = 20) -> dict:
        return await self._request("GET", "/api/companies", params={
            "search": search, "page": page, "page_size": page_size,
        })

    async def create_company(self, name: str, url: str, company_name: str = "",
                             credit_code: str = "", site_type: str = "auto") -> dict:
        body: dict = {"name": name, "url": url, "site_type": site_type}
        if company_name:
            body["company_name"] = company_name
        if credit_code:
            body["credit_code"] = credit_code
        return await self._request("POST", "/api/companies", json=body)

    async def trigger_generate(self, company_id: int) -> dict:
        return await self._request(
            "POST", f"/api/companies/{company_id}/generate",
            json={"timeout": 10800, "verbose": True},
        )

    async def trigger_crawl(self, company_id: int) -> dict:
        return await self._request("POST", f"/api/companies/{company_id}/crawl")

    async def get_execution(self, execution_id: int) -> dict:
        return await self._request("GET", f"/api/executions/{execution_id}")

    async def cancel_execution(self, execution_id: int) -> dict:
        return await self._request("POST", f"/api/executions/{execution_id}/cancel")

    async def get_company(self, company_id: int) -> dict:
        return await self._request("GET", f"/api/companies/{company_id}")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  BatchOrchestrator — 核心编排引擎
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class BatchOrchestrator:
    def __init__(
        self,
        dry_run: bool = False,
        skip_crawl: bool = False,
        filter_keyword: str = "",
    ):
        self.tasks: list[CompanyTask] = []
        self.client = PlatformClient()
        self.dry_run = dry_run
        self.skip_crawl = skip_crawl
        self.filter_keyword = filter_keyword
        self._paused = False
        self._shutdown = False
        self._start_time = datetime.now()
        self._global_logs: list[str] = []
        self._used_names: set[str] = set()

    def _global_log(self, msg: str):
        ts = datetime.now().strftime("%H:%M:%S")
        entry = f"[{ts}] {msg}"
        self._global_logs.append(entry)
        if len(self._global_logs) > MAX_GLOBAL_LOGS:
            self._global_logs = self._global_logs[-MAX_GLOBAL_LOGS:]

    def _unique_name(self, base: str) -> str:
        """确保 name 唯一, 追加 _2, _3..."""
        candidate = f"{base}_feishu"
        if candidate not in self._used_names:
            self._used_names.add(candidate)
            return candidate
        i = 2
        while f"{base}_feishu_{i}" in self._used_names:
            i += 1
        final = f"{base}_feishu_{i}"
        self._used_names.add(final)
        return final

    # ── 本地 JSON 加载 ──

    def load_from_json(self, filepath: str = DATA_FILE):
        """从本地 JSON 文件加载公司数据"""
        self._global_log(f"Loading data from {filepath}...")
        if not os.path.exists(filepath):
            logger.error(f"Data file not found: {filepath}")
            sys.exit(1)

        with open(filepath, "r", encoding="utf-8") as f:
            results = json.load(f)

        for doc in results:
            cname = doc.get("company_name")
            if not cname:
                continue
            if self.filter_keyword and self.filter_keyword not in cname:
                continue
            job_url = doc.get("job_url", "")
            if not job_url:
                continue

            code_url = extract_code_url(job_url)
            short = make_short_name(cname)
            name = self._unique_name(short)

            task = CompanyTask(
                company_name=cname,
                job_url=job_url,
                code_url=code_url,
                short_name=name,
            )
            self.tasks.append(task)

        self._global_log(f"Loaded {len(self.tasks)} companies from JSON")
        logger.info(f"Loaded {len(self.tasks)} companies")

    # ── 数据补全: 天眼查 ──

    async def _enrich(self, task: CompanyTask):
        task.state = CompanyState.ENRICHING
        task.log("Searching Tianyancha...")
        self._global_log(f"[{task.short_name}] Enriching via Tianyancha")
        try:
            resp = await self.client.search_tianyancha(task.company_name)
            items = resp.get("items", [])
            if items:
                # 优先精确匹配
                match = next((it for it in items if it.get("name") == task.company_name), items[0])
                task.credit_code = match.get("creditCode", "")
                task.log(f"Found credit_code: {task.credit_code}")
            else:
                task.log("Tianyancha: no results, credit_code left empty")
        except Exception as e:
            task.log(f"Tianyancha error: {e}, continuing without credit_code")

    # ── 创建公司 ──

    async def _create(self, task: CompanyTask):
        task.state = CompanyState.CREATING
        task.log("Checking if company already exists on platform...")
        self._global_log(f"[{task.short_name}] Creating company")

        # 检查是否已存在 (用 short_name 搜索)
        try:
            existing = await self.client.list_companies(search=task.short_name)
            if existing.get("total", 0) > 0:
                for item in existing.get("items", []):
                    if item.get("name") == task.short_name:
                        task.state = CompanyState.SKIPPED
                        task.log(f"SKIP: Company '{task.short_name}' already exists (id={item['id']})")
                        self._global_log(f"[{task.short_name}] SKIPPED - already exists")
                        return
        except Exception as e:
            task.log(f"Search error: {e}, proceeding with create")

        # 创建
        try:
            resp = await self.client.create_company(
                name=task.short_name,
                url=task.code_url,
                company_name=task.company_name,
                credit_code=task.credit_code,
            )
            if resp.get("_status", 200) == 400:
                detail = resp.get("detail", "")
                if "already exists" in str(detail).lower():
                    task.state = CompanyState.SKIPPED
                    task.log(f"SKIP: {detail}")
                    self._global_log(f"[{task.short_name}] SKIPPED - {detail}")
                    return
                task.state = CompanyState.FAILED
                task.error_message = str(detail)
                task.log(f"FAILED to create: {detail}")
                self._global_log(f"[{task.short_name}] FAILED - {detail}")
                return

            task.platform_company_id = resp.get("id")
            task.log(f"Created company id={task.platform_company_id}")
        except Exception as e:
            task.state = CompanyState.FAILED
            task.error_message = str(e)
            task.log(f"FAILED to create: {e}")
            self._global_log(f"[{task.short_name}] FAILED - create error: {e}")

    # ── 触发 Generate ──

    async def _generate(self, task: CompanyTask):
        task.state = CompanyState.GENERATING
        task.log("Triggering crawler generation...")
        self._global_log(f"[{task.short_name}] Triggering generate")
        try:
            resp = await self.client.trigger_generate(task.platform_company_id)
            if resp.get("_status", 200) >= 400:
                detail = resp.get("detail", "")
                task.state = CompanyState.FAILED
                task.error_message = str(detail)
                task.log(f"FAILED to generate: {detail}")
                self._global_log(f"[{task.short_name}] FAILED - generate error: {detail}")
                return
            task.generate_execution_id = resp.get("execution_id")
            task.state = CompanyState.WAITING_GENERATE
            task.log(f"Generate queued, execution_id={task.generate_execution_id}")
        except Exception as e:
            task.state = CompanyState.FAILED
            task.error_message = str(e)
            task.log(f"FAILED to generate: {e}")
            self._global_log(f"[{task.short_name}] FAILED - generate error: {e}")

    # ── 触发 Crawl ──

    async def _crawl(self, task: CompanyTask):
        task.state = CompanyState.RUNNING
        task.log("Triggering crawl...")
        self._global_log(f"[{task.short_name}] Triggering crawl")
        try:
            resp = await self.client.trigger_crawl(task.platform_company_id)
            if resp.get("_status", 200) >= 400:
                detail = resp.get("detail", "")
                task.state = CompanyState.FAILED
                task.error_message = str(detail)
                task.log(f"FAILED to crawl: {detail}")
                self._global_log(f"[{task.short_name}] FAILED - crawl error: {detail}")
                return
            task.crawl_execution_id = resp.get("execution_id")
            task.state = CompanyState.WAITING_RUN
            task.log(f"Crawl queued, execution_id={task.crawl_execution_id}")
        except Exception as e:
            task.state = CompanyState.FAILED
            task.error_message = str(e)
            task.log(f"FAILED to crawl: {e}")
            self._global_log(f"[{task.short_name}] FAILED - crawl error: {e}")

    # ── 轮询 Executions ──

    async def _poll_executions(self):
        """轮询所有等待中的任务, Generate 完成后自动触发 Crawl"""
        for task in self.tasks:
            if self._shutdown:
                return

            # Poll generate
            if task.state == CompanyState.WAITING_GENERATE and task.generate_execution_id:
                try:
                    resp = await self.client.get_execution(task.generate_execution_id)
                    status = resp.get("status", "")
                    if status == "success":
                        task.log("Generate SUCCESS")
                        self._global_log(f"[{task.short_name}] Generate SUCCESS")
                        if self.skip_crawl:
                            task.state = CompanyState.COMPLETED
                            task.log("Skip crawl mode - marked COMPLETED")
                        else:
                            await self._crawl(task)
                    elif status == "failed":
                        task.state = CompanyState.FAILED
                        task.error_message = resp.get("error_message", "generation failed")
                        task.log(f"Generate FAILED: {task.error_message}")
                        self._global_log(f"[{task.short_name}] Generate FAILED: {task.error_message}")
                    elif status == "cancelled":
                        task.state = CompanyState.FAILED
                        task.error_message = "generation cancelled"
                        task.log("Generate CANCELLED")
                        self._global_log(f"[{task.short_name}] Generate CANCELLED")
                except Exception as e:
                    task.log(f"Poll generate error: {e}")

            # Poll crawl
            elif task.state == CompanyState.WAITING_RUN and task.crawl_execution_id:
                try:
                    resp = await self.client.get_execution(task.crawl_execution_id)
                    status = resp.get("status", "")
                    if status == "success":
                        task.state = CompanyState.COMPLETED
                        task.log("Crawl SUCCESS — COMPLETED")
                        self._global_log(f"[{task.short_name}] Crawl SUCCESS — COMPLETED")
                    elif status == "failed":
                        task.state = CompanyState.FAILED
                        task.error_message = resp.get("error_message", "crawl failed")
                        task.log(f"Crawl FAILED: {task.error_message}")
                        self._global_log(f"[{task.short_name}] Crawl FAILED: {task.error_message}")
                    elif status == "cancelled":
                        task.state = CompanyState.FAILED
                        task.error_message = "crawl cancelled"
                        task.log("Crawl CANCELLED")
                        self._global_log(f"[{task.short_name}] Crawl CANCELLED")
                except Exception as e:
                    task.log(f"Poll crawl error: {e}")

    # ── Stop: 取消排队中的任务 ──

    async def _cancel_pending_executions(self):
        """取消平台上所有 pending 状态的 execution"""
        self._global_log("Cancelling all pending executions on platform...")
        for task in self.tasks:
            for eid in [task.generate_execution_id, task.crawl_execution_id]:
                if eid is None:
                    continue
                try:
                    resp = await self.client.get_execution(eid)
                    if resp.get("status") == "pending":
                        await self.client.cancel_execution(eid)
                        task.log(f"Cancelled pending execution {eid}")
                        self._global_log(f"[{task.short_name}] Cancelled execution {eid}")
                except Exception as e:
                    logger.warning(f"Failed to cancel execution {eid}: {e}")
        self._global_log("Cancellation complete")

    # ── 主运行循环 ──

    async def run(self):
        if not self.tasks:
            self._global_log("No tasks to process")
            return

        if self.dry_run:
            self._global_log("DRY RUN — listing loaded companies:")
            for i, t in enumerate(self.tasks, 1):
                self._global_log(f"  {i}. {t.company_name} → {t.short_name} | {t.code_url}")
            self._global_log("DRY RUN complete. Exiting.")
            return

        # Phase 1: Submit all tasks (enrich → create → generate)
        self._global_log("=== Phase 1: Submit ===")
        for task in self.tasks:
            if self._shutdown:
                break
            while self._paused and not self._shutdown:
                await asyncio.sleep(1)
            if self._shutdown:
                break

            # Enrich
            await self._enrich(task)
            if self._shutdown:
                break

            # Create
            await self._create(task)
            if task.state in (CompanyState.SKIPPED, CompanyState.FAILED):
                continue
            if self._shutdown:
                break

            # Generate
            await self._generate(task)

            await asyncio.sleep(SUBMIT_INTERVAL)

        # Phase 2: Poll until all done
        self._global_log("=== Phase 2: Poll ===")
        while not self._shutdown:
            waiting = [
                t for t in self.tasks
                if t.state in (CompanyState.WAITING_GENERATE, CompanyState.WAITING_RUN)
            ]
            if not waiting:
                break
            await self._poll_executions()
            await asyncio.sleep(POLL_INTERVAL)

        if self._shutdown:
            await self._cancel_pending_executions()

        await self.client.close()
        self._global_log("=== Done ===")
        summary = self.get_summary()
        self._global_log(
            f"Final: {summary['completed']} completed, {summary['failed']} failed, "
            f"{summary['skipped']} skipped"
        )

    # ── 状态查询 (给看板用) ──

    def get_summary(self) -> dict:
        s = {
            "total": len(self.tasks),
            "completed": 0, "failed": 0, "skipped": 0, "pending": 0,
            "generate_done": 0, "generate_running": 0, "generate_queued": 0, "generate_failed": 0,
            "crawl_done": 0, "crawl_running": 0, "crawl_queued": 0, "crawl_failed": 0,
        }
        for t in self.tasks:
            st = t.state
            if st == CompanyState.COMPLETED:
                s["completed"] += 1
                s["generate_done"] += 1
                s["crawl_done"] += 1
            elif st == CompanyState.FAILED:
                s["failed"] += 1
                # 判断是哪个阶段失败
                if t.crawl_execution_id:
                    s["generate_done"] += 1
                    s["crawl_failed"] += 1
                elif t.generate_execution_id:
                    s["generate_failed"] += 1
            elif st == CompanyState.SKIPPED:
                s["skipped"] += 1
            elif st == CompanyState.PENDING:
                s["pending"] += 1
            elif st in (CompanyState.ENRICHING, CompanyState.CREATING):
                s["pending"] += 1
            elif st == CompanyState.GENERATING:
                s["generate_queued"] += 1
            elif st == CompanyState.WAITING_GENERATE:
                # 可能是 running 或 pending in platform queue
                s["generate_running"] += 1
            elif st == CompanyState.RUNNING:
                s["crawl_queued"] += 1
                s["generate_done"] += 1
            elif st == CompanyState.WAITING_RUN:
                s["crawl_running"] += 1
                s["generate_done"] += 1
        return s

    def get_status(self) -> dict:
        return {
            "start_time": self._start_time.isoformat(),
            "paused": self._paused,
            "shutdown": self._shutdown,
            "summary": self.get_summary(),
            "tasks": [
                {
                    "company_name": t.company_name,
                    "short_name": t.short_name,
                    "state": t.state.value,
                    "code_url": t.code_url,
                    "platform_company_id": t.platform_company_id,
                    "generate_execution_id": t.generate_execution_id,
                    "crawl_execution_id": t.crawl_execution_id,
                    "error_message": t.error_message,
                    "logs": t.logs[-20:],  # 只返回最近 20 条
                }
                for t in self.tasks
            ],
            "global_logs": self._global_logs,
        }

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  FastAPI Dashboard
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

app = FastAPI(title="Batch Import Dashboard")
orchestrator: Optional[BatchOrchestrator] = None


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    return DASHBOARD_HTML


@app.get("/api/status")
async def api_status():
    if orchestrator:
        return JSONResponse(orchestrator.get_status())
    return JSONResponse({"error": "not started"}, status_code=503)


@app.get("/api/summary")
async def api_summary():
    if orchestrator:
        return JSONResponse(orchestrator.get_summary())
    return JSONResponse({"error": "not started"}, status_code=503)


@app.get("/api/logs/{company_name}")
async def api_logs(company_name: str):
    if not orchestrator:
        return JSONResponse({"logs": []})
    for t in orchestrator.tasks:
        if t.company_name == company_name:
            return JSONResponse({"logs": t.logs})
    return JSONResponse({"logs": []})


@app.post("/api/control/pause")
async def control_pause():
    if orchestrator:
        orchestrator._paused = True
        orchestrator._global_log("⏸ PAUSED by user")
        return {"status": "paused"}
    return JSONResponse({"error": "not started"}, status_code=503)


@app.post("/api/control/resume")
async def control_resume():
    if orchestrator:
        orchestrator._paused = False
        orchestrator._global_log("▶ RESUMED by user")
        return {"status": "resumed"}
    return JSONResponse({"error": "not started"}, status_code=503)


@app.post("/api/control/stop")
async def control_stop():
    if orchestrator:
        orchestrator._global_log("⏹ STOP requested by user")
        orchestrator._shutdown = True
        return {"status": "stopping"}
    return JSONResponse({"error": "not started"}, status_code=503)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  入口
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


async def _run_server(port: int):
    config = uvicorn.Config(app, host="0.0.0.0", port=port, log_level="warning")
    server = uvicorn.Server(config)
    await server.serve()


async def _main(args):
    global orchestrator

    orch = BatchOrchestrator(
        dry_run=args.dry_run,
        skip_crawl=args.skip_crawl,
        filter_keyword=args.filter or "",
    )
    orchestrator = orch

    # 加载数据
    orch.load_from_json()

    if not orch.tasks:
        logger.info("No companies matched. Exiting.")
        return

    # Ctrl+C 优雅退出
    loop = asyncio.get_event_loop()

    def _signal_handler():
        logger.info("Ctrl+C received, shutting down gracefully...")
        orch._shutdown = True

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    port = args.port
    logger.info(f"Dashboard: http://localhost:{port}/dashboard")

    # 并行运行: uvicorn server + orchestrator
    server_task = asyncio.create_task(_run_server(port))
    orch_task = asyncio.create_task(orch.run())

    # 等待 orchestrator 完成
    await orch_task
    logger.info("Orchestrator finished. Dashboard remains available. Press Ctrl+C to exit.")

    # 保持 server 运行直到 Ctrl+C
    try:
        await server_task
    except asyncio.CancelledError:
        pass


def main():
    parser = argparse.ArgumentParser(description="Feishu Batch Import Tool")
    parser.add_argument("--dry-run", action="store_true", help="只加载数据不执行")
    parser.add_argument("--skip-crawl", action="store_true", help="只 Generate 不 Crawl")
    parser.add_argument("--filter", type=str, default="", help="只处理匹配的公司名")
    parser.add_argument("--port", type=int, default=DASHBOARD_PORT, help="看板端口 (default: 8888)")
    args = parser.parse_args()

    asyncio.run(_main(args))


if __name__ == "__main__":
    main()
