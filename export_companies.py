#!/usr/bin/env python3
"""
从 MongoDB 导出 mokahr/dayi/beisen/feishu 公司数据，反推招聘列表页 URL，输出 CSV。

用法:
    python export_companies.py                          # 导出全部平台
    python export_companies.py --platform feishu        # 仅导出飞书
    python export_companies.py --platform mokahr dayi   # 导出多个平台
    python export_companies.py --output result.csv      # 指定输出文件
"""
import argparse
import csv
import sys
from urllib.parse import urlparse

import pymongo

MONGO_URI = "mongodb://admin:ZG8uY0zzDuAqtGin@192.168.253.10:27017/"
DB_NAME = "company_one"
PLATFORMS = ["mokahr", "dayi", "beisen", "feishu"]


def connect_mongo():
    client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    return client


def fetch_companies(client, platform):
    """按 company_name 分组，取第一条 job_url 和 company_code。"""
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
    return [r for r in results if r["_id"] and r.get("job_url")]


# ── URL 反推规则 ──────────────────────────────────────────────

def derive_url_mokahr(job_url, _code):
    """
    job_url: https://app.mokahr.com/social-recruitment/deeproute/143885#/job/363189b3-...
    目标:    https://app.mokahr.com/social-recruitment/deeproute/143885#/
    """
    marker = "#/job/"
    idx = job_url.find(marker)
    if idx != -1:
        return job_url[:idx] + "#/"
    # 兜底：去掉最后一段 path
    marker2 = "#/"
    idx2 = job_url.find(marker2)
    if idx2 != -1:
        return job_url[:idx2] + "#/"
    return job_url


def derive_url_dayi(job_url, _code):
    """
    job_url: https://wecruit.hotjob.cn/wecruit/SU{code}/pb/posDetail.html?postId=...
    目标:    https://wecruit.hotjob.cn/SU{code}/pb/index.html#/social
    """
    parsed = urlparse(job_url)
    path = parsed.path  # e.g. /wecruit/SU62a1d6c5bef46f30ac309fa4/pb/posDetail.html
    # 去掉开头的 /wecruit
    if path.startswith("/wecruit/"):
        path = path[len("/wecruit"):]  # -> /SU62a.../pb/posDetail.html
    # 截到 /pb 前
    pb_idx = path.find("/pb")
    if pb_idx != -1:
        base = path[:pb_idx]
    else:
        base = path
    return f"{parsed.scheme}://{parsed.netloc}{base}/pb/index.html#/social"


def derive_url_beisen(job_url, company_code):
    """
    正常: https://hankun.zhiye.com/social/detail?jobAdId=... → https://hankun.zhiye.com/social/jobs
    异常（URL含重复域名）: https://roborock.zhiye.com/roborock.zhiye.com/detail?...
       → 用 company_code 构建 → https://roborock.zhiye.com/social/jobs
    """
    parsed = urlparse(job_url)
    host = parsed.netloc  # e.g. hankun.zhiye.com

    # 异常 URL（path 中包含重复域名）或正常 URL，
    # 都直接用 netloc 构建即可，netloc 已经是正确的域名
    return f"{parsed.scheme}://{host}/social/jobs"


def derive_url_feishu(job_url, _code):
    """
    正常: https://ponyai.jobs.feishu.cn/ponyai/position/7563.../detail
       → https://ponyai.jobs.feishu.cn/ponyai
    特殊: https://jobs.wework.cn/application/position/...
       → https://jobs.wework.cn/application
    """
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


def process_platform(client, platform):
    """返回 [(company_name, url, channel), ...]"""
    companies = fetch_companies(client, platform)
    derive = DERIVE_FUNCS[platform]
    rows = []
    for doc in companies:
        name = doc["_id"]
        job_url = doc["job_url"]
        company_code = doc.get("company_code", "")
        url = derive(job_url, company_code)
        rows.append((name, url, platform))
    return rows


def main():
    parser = argparse.ArgumentParser(description="从 MongoDB 导出公司招聘列表页 URL")
    parser.add_argument(
        "--platform", nargs="+", default=["all"],
        choices=PLATFORMS + ["all"],
        help="要导出的平台（默认 all）",
    )
    parser.add_argument(
        "--output", default="companies_export.csv",
        help="输出 CSV 文件路径（默认 companies_export.csv）",
    )
    args = parser.parse_args()

    platforms = PLATFORMS if "all" in args.platform else args.platform

    print(f"连接 MongoDB...")
    try:
        client = connect_mongo()
    except Exception as e:
        print(f"连接失败: {e}", file=sys.stderr)
        sys.exit(1)

    all_rows = []
    for p in platforms:
        rows = process_platform(client, p)
        print(f"  {p}: {len(rows)} 家公司")
        all_rows.extend(rows)

    client.close()

    with open(args.output, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(["company_name", "url", "channel"])
        writer.writerows(all_rows)

    print(f"\n共导出 {len(all_rows)} 家公司 → {args.output}")


if __name__ == "__main__":
    main()
