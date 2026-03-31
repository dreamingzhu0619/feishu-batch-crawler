#!/usr/bin/env python3
"""在 MongoDB 服务器上运行，导出 feishu 公司去重数据为 JSON"""
import json
import pymongo

client = pymongo.MongoClient("mongodb://admin:ZG8uY0zzDuAqtGin@127.0.0.1:27017/")
coll = client["company_one"]["feishu"]

results = list(coll.aggregate([
    {"$group": {"_id": "$company_name", "job_url": {"$first": "$job_url"}}},
    {"$sort": {"_id": 1}},
]))
client.close()

data = [{"company_name": r["_id"], "job_url": r["job_url"]} for r in results if r["_id"]]
print(json.dumps(data, ensure_ascii=False, indent=2))
