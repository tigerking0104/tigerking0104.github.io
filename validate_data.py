#!/usr/bin/env python3
"""
校验语录数据文件（xsaid.json）的完整性，作为发布前的质量门禁。

校验规则:
- 顶层必须是非空 JSON 数组
- 每个人物必须有 id / name / stance / domain，quotes 必须是数组
- 每条语录必须有 id 和非空 content，且 id 全局唯一

校验失败时退出码为 1，CI 流程会中止，防止坏数据发布到线上。
（App 端远程更新失败会回退本地缓存，但发布前拦截是第一道防线）

用法:
    python3 validate_data.py [xsaid.json]
"""

import json
import sys


def fail(msg):
    print(f"[FAIL] {msg}")
    sys.exit(1)


def main():
    path = sys.argv[1] if len(sys.argv) > 1 else "xsaid.json"

    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        fail(f"文件不存在: {path}")
    except json.JSONDecodeError as e:
        fail(f"JSON 解析错误: {e}")

    if not isinstance(data, list) or not data:
        fail("顶层必须是非空数组")

    quote_ids = set()
    total = 0
    for p in data:
        pid = p.get("id")
        name = p.get("name")
        if not pid or not name:
            fail(f"人物缺少 id/name: {str(p)[:100]}")
        if not p.get("stance") or not p.get("domain"):
            fail(f"人物 {pid}({name}) 缺少 stance/domain")
        quotes = p.get("quotes")
        if not isinstance(quotes, list):
            fail(f"人物 {pid}({name}) 的 quotes 不是数组")

        for q in quotes:
            qid = q.get("id")
            content = q.get("content")
            if not qid:
                fail(f"{pid}({name}) 有语录缺少 id: {str(content)[:50]}")
            if not isinstance(content, str) or not content.strip():
                fail(f"语录 {qid} 的 content 为空")
            if qid in quote_ids:
                fail(f"语录 id 全局重复: {qid}")
            quote_ids.add(qid)
            total += 1

    print(f"[OK] 校验通过: {len(data)} 个人物, {total} 条语录 ({path})")


if __name__ == "__main__":
    main()
