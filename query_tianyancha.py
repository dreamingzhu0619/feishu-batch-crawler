# -*- coding: utf-8 -*-
"""从 url输出结果.md 提取公司名，批量查天眼查"""
import re
import json
import time
import urllib.request
import urllib.parse

MD_FILE = "url输出结果.md"
OUTPUT_FILE = "tianyancha_results.json"
API_URL = "http://192.168.253.10:8083/tianyancha/open/ic"

# 1. 提取公司名
companies = []
with open(MD_FILE, "r", encoding="utf-8") as f:
    for line in f:
        m = re.match(r"^公司:\s*(.+?)\s+\(共\s*\d+\s*个职位\)", line)
        if m:
            name = m.group(1).strip()
            if name and name != "null":
                companies.append(name)

print(f"共 {len(companies)} 家公司待查询\n")

# 2. 批量查天眼查
results = []
for i, name in enumerate(companies, 1):
    params = urllib.parse.urlencode({"keyword": name})
    url = f"{API_URL}?{params}"
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))

        if data.get("code") == 0 and data.get("data", {}).get("result"):
            r = data["data"]["result"]
            info = {
                "company_name": name,
                "legal_person": r.get("legalPersonName", ""),
                "reg_status": r.get("regStatus", ""),
                "reg_capital": r.get("regCapital", ""),
                "city": r.get("city", ""),
                "industry": r.get("industry", ""),
                "staff_num": r.get("staffNumRange", ""),
                "credit_code": r.get("creditCode", ""),
                "business_scope": r.get("businessScope", ""),
            }
            results.append(info)
            print(f"[{i}/{len(companies)}] {name} -> {r.get('regStatus', '')} | {r.get('staffNumRange', '')}")
        else:
            reason = data.get("data", {}).get("reason", "未知")
            results.append({"company_name": name, "error": reason})
            print(f"[{i}/{len(companies)}] {name} -> 无结果: {reason}")
    except Exception as e:
        results.append({"company_name": name, "error": str(e)})
        print(f"[{i}/{len(companies)}] {name} -> 请求失败: {e}")

    time.sleep(0.3)

# 3. 保存结果
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(results, f, ensure_ascii=False, indent=2)

print(f"\n完成！结果已保存到 {OUTPUT_FILE}")
print(f"成功: {sum(1 for r in results if 'error' not in r)} / {len(results)}")
