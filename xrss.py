"""
从 Folo 订阅源读取 X (Twitter) 数据，合并到 data.json

Folo 会订阅 RSSHub 的 Twitter List 路由，并通过其公开 API 提供条目数据。

数据获取方式（两种）:
  1. 公开 API（默认）: 每次只返回最新 10 条，无分页支持
     → 适合通过 cron 高频轮询（每2-4小时一次），确保不漏数据

  2. Folo CLI（需认证）: 支持 cursor 分页，可一次拉取全部历史条目
     → 使用: python3 xrss.py --cli
     → 首次需登录: npx folocli login
     → 适合初次全量同步或补数据

增量更新: 通过 xrss_cursor.json 记录已获取的 quote ID 集合，
每次运行只处理新条目并合并到 data.json。

Cron 示例（每3小时运行一次）:
  0 */3 * * * cd /path/to/said/data && python3 xrss.py >> xrss.log 2>&1
"""

import argparse
import json
import os
import re
import subprocess
import sys
from collections import defaultdict
from datetime import datetime, timezone

import requests

from people import people

# Folo 订阅源 ID（来自 https://app.folo.is/share/feeds/1231125423383773184）
FOLO_FEED_ID = "1231125423383773184"
FOLO_API_URL = f"https://api.folo.is/feeds?id={FOLO_FEED_ID}&entriesLimit=10"

# Folo CLI 分页参数
CLI_BATCH_SIZE = 50  # 每次 CLI 请求的条目数
CLI_MAX_PAGES = 100  # 最多翻页数（安全上限，防止无限循环；100页=5000条，用于回补历史缺口）

# 输出文件路径（合并到主数据文件，由 GitHub Pages 对外提供）
DATA_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_PATH = os.path.join(DATA_DIR, "xsaid.json")
CURSOR_PATH = os.path.join(DATA_DIR, "xrss_cursor.json")


def build_handle_map():
    """从 people.py 构建 X handle → person 的映射"""
    handle_map = {}
    for person in people:
        x_handle = person.get("sources", {}).get("x", {}).get("handle")
        if x_handle:
            key = x_handle.lstrip("@").lower()
            handle_map[key] = person
    return handle_map


def extract_handle_from_url(author_url):
    """从 authorUrl 提取 X handle，如 https://x.com/seanhannity → seanhannity"""
    if not author_url:
        return None
    m = re.search(r"x\.com/(\w+)", author_url)
    return m.group(1).lower() if m else None


def strip_html(html):
    """简单去除 HTML 标签，保留纯文本"""
    if not html:
        return ""
    # 去掉 <hr> 分隔线及之后的内容（通常是引用推文）
    html = re.split(r"<hr\s*/?\s*>", html, maxsplit=1)[0]
    # 去掉所有 HTML 标签
    text = re.sub(r"<[^>]+>", "", html)
    # 合并空白
    text = re.sub(r"\s+", " ", text).strip()
    return text


def extract_tweet_id(url):
    """从推文 URL 提取 status ID"""
    if not url:
        return None
    m = re.search(r"/status/(\d+)", url)
    return m.group(1) if m else None


def parse_date(iso_str):
    """解析 ISO 格式日期字符串，返回 UTC 格式"""
    if not iso_str:
        return None
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        return None


def entry_to_quote(entry, person_id):
    """将 Folo 条目转换为 app Quote 格式"""
    url = entry.get("url", "")
    tweet_id = extract_tweet_id(url)
    quote_id = f"{person_id}_x_{tweet_id}" if tweet_id else f"{person_id}_x_{entry.get('id', 'unknown')}"

    content = strip_html(entry.get("content", "")) or entry.get("title", "")
    date_str = parse_date(entry.get("publishedAt"))
    summary = entry.get("summary")

    return {
        "id": quote_id,
        "content": content,
        "date": date_str,
        "source": "x",
        "source_url": url,
        "context": summary,
        "is_user_added": False,
    }


def load_cursor():
    """加载游标：已获取的 quote ID 集合 + 最后一条的 publishedAt"""
    if os.path.exists(CURSOR_PATH):
        with open(CURSOR_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
            return set(data.get("seen_quote_ids", [])), data.get("last_published_at")
    return set(), None


def save_cursor(seen_ids, last_published_at=None):
    """保存游标"""
    with open(CURSOR_PATH, "w", encoding="utf-8") as f:
        json.dump({
            "seen_quote_ids": sorted(seen_ids),
            "last_published_at": last_published_at,
        }, f, ensure_ascii=False, indent=2)


def load_existing_data():
    """加载已有的 data.json"""
    if os.path.exists(OUTPUT_PATH):
        with open(OUTPUT_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return []


def merge_data(existing, new_persons_quotes):
    """将新 X 语录合并到已有 data.json 中（按 person_id 匹配，quote 按 content 去重）

    existing: data.json 中的完整人物列表
    new_persons_quotes: 本次新获取的 X 语录（按 person_id 分组）

    合并规则:
    - 已有人物: 只追加新语录（按 content 去重），保留原有语录不变
    - 新人物: 整个添加进去
    """
    existing_by_id = {p["id"]: p for p in existing}

    # 构建已有内容的去重集合
    existing_contents = {}
    for p in existing:
        for q in p.get("quotes", []):
            key = f"{p['id']}:{q['content'].strip().lower()}"
            existing_contents[key] = True

    for person_id, person_info in new_persons_quotes.items():
        new_quotes = []
        for q in person_info["quotes"]:
            key = f"{person_id}:{q['content'].strip().lower()}"
            if key not in existing_contents:
                new_quotes.append(q)
                existing_contents[key] = True

        if person_id in existing_by_id:
            # 已有人物：追加新语录
            existing_by_id[person_id].setdefault("quotes", []).extend(new_quotes)
        else:
            # 新人物：添加完整条目
            existing_by_id[person_id] = {
                "id": person_info["id"],
                "name": person_info["name"],
                "avatar_url": None,
                "title": person_info["title"],
                "stance": person_info["stance"],
                "domain": person_info["domain"],
                "quotes": new_quotes,
            }

    return list(existing_by_id.values())


# ==================== 公开 API 方式 ====================

def fetch_entries_public_api():
    """从 Folo 公开 API 获取最新条目（最多10条，无分页）"""
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/131.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Referer": "https://app.folo.is/",
        "Origin": "https://app.folo.is",
    }
    resp = requests.get(FOLO_API_URL, headers=headers, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    if data.get("code") != 0:
        raise RuntimeError(f"Folo API error: {data}")

    return data["data"]["feed"], data["data"]["entries"]


# ==================== Folo CLI 方式（支持分页） ====================

def check_cli_auth():
    """检查 folocli 是否已认证，未认证则提示登录"""
    cmd = ["npx", "--yes", "folocli@latest", "whoami"]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            print("Folo CLI 未认证，请先登录:")
            print("  npx --yes folocli@latest login")
            print("\n登录后会打开浏览器完成设备码认证。")
            print("也可通过环境变量认证: export FOLO_TOKEN=<your_token>")
            sys.exit(1)
        return True
    except FileNotFoundError:
        print("错误: 未找到 npx，请先安装 Node.js:")
        print("  brew install node")
        sys.exit(1)
    except subprocess.TimeoutExpired:
        print("错误: folocli 超时，请检查网络连接")
        sys.exit(1)


def fetch_entries_cli():
    """
    使用 Folo CLI 分页获取全部条目

    CLI 返回格式（嵌套结构）:
      {
        "ok": true,
        "data": {
          "entries": [
            {
              "read": false,
              "entries": { "id": "...", "title": "...", "url": "...", "author": "..." },
              "feeds": { "id": "...", "title": "...", ... },
              "subscriptions": { ... }
            },
            ...
          ],
          "nextCursor": "...",
          "hasNext": true/false
        }
      }

    注意:
    - --feed 参数会导致 "fetch failed"，改用全局 timeline 后按 feed ID 过滤
    - 实际条目数据嵌套在 item["entries"] 中
    - feed 信息在 item["feeds"] 中

    Returns:
        (feed_title, all_entries) 元组
    """
    check_cli_auth()

    all_entries = []
    feed_title = None
    cursor = None
    page = 0
    matched_count = 0

    while page < CLI_MAX_PAGES:
        page += 1
        # 不使用 --feed 参数（会报错），改为获取全局 timeline 后过滤
        cmd = ["npx", "--yes", "folocli@latest", "timeline",
               "--limit", str(CLI_BATCH_SIZE)]
        if cursor:
            cmd.extend(["--cursor", cursor])

        print(f"  [CLI] 第 {page} 页 (cursor={cursor or '初始'})...")
        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=60, check=True)
            raw = json.loads(result.stdout)
        except FileNotFoundError:
            print("错误: 未找到 npx，请先安装 Node.js")
            sys.exit(1)
        except subprocess.CalledProcessError as e:
            print(f"错误: folocli 执行失败: {e.stderr[:200]}")
            sys.exit(1)
        except json.JSONDecodeError:
            print(f"错误: folocli 返回非 JSON 数据: {result.stdout[:200]}")
            sys.exit(1)

        # 解析 CLI 输出信封
        if not raw.get("ok"):
            err = raw.get("error", {})
            print(f"错误: folocli 返回错误: {err.get('code')} - {err.get('message')}")
            sys.exit(1)

        data = raw.get("data", {})
        timeline_items = data.get("entries", [])
        next_cursor = data.get("nextCursor")
        has_next = data.get("hasNext", False)

        if not timeline_items:
            print(f"  [CLI] 第 {page} 页无数据，停止翻页")
            break

        # 解嵌套：提取 item["entries"] 为实际条目，按 feed ID 过滤
        page_matched = 0
        for item in timeline_items:
            feed_info = item.get("feeds", {})
            feed_id = feed_info.get("id", "")

            # 只保留目标 feed 的条目
            if feed_id != FOLO_FEED_ID:
                continue

            # 记录 feed 标题
            if not feed_title:
                feed_title = feed_info.get("title", "Unknown")

            # 提取嵌套的实际条目数据
            entry_data = item.get("entries", {})
            if entry_data:
                all_entries.append(entry_data)
                page_matched += 1

        matched_count += page_matched
        print(f"  [CLI] 第 {page} 页: {len(timeline_items)} 条总计, {page_matched} 条匹配目标feed")

        # 通过 hasNext 判断是否继续
        if not has_next or not next_cursor:
            print(f"  [CLI] 已到达最后一页")
            break

        cursor = next_cursor

    print(f"  [CLI] 总计匹配目标feed: {matched_count} 条")
    if not feed_title:
        feed_title = "Unknown"
    return feed_title, all_entries


# ==================== 主流程 ====================

def extract_handle(entry):
    """从条目中提取 X handle，兼容公开 API 和 CLI 两种格式"""
    # 方式1: 从 authorUrl 提取（公开 API 格式）
    handle = extract_handle_from_url(entry.get("authorUrl", ""))
    if handle:
        return handle

    # 方式2: 从 url 字段提取（CLI 格式，如 https://x.com/seanhannity/status/123）
    handle = extract_handle_from_url(entry.get("url", ""))
    if handle:
        return handle

    # 方式3: 从 author 字段提取（如 "@seanhannity" 或 "seanhannity"）
    author = entry.get("author", "")
    if author:
        m = re.match(r"@?(\w+)", author)
        if m:
            return m.group(1).lower()

    return None


def is_video_entry(entry):
    """判断条目是否包含视频内容，应过滤掉不收录

    检测规则:
    1. content 中包含 <video> 标签
    2. content 中包含视频平台链接（YouTube、Vimeo、Rumble 等）
    3. 条目的 attachments/media 中包含视频类型
    4. URL 本身指向视频平台
    """
    content = entry.get("content", "") or ""
    url = entry.get("url", "") or ""
    title = entry.get("title", "") or ""

    combined = f"{content} {url} {title}".lower()

    # 1. HTML <video> 标签
    if re.search(r"<video[\s>]", content, re.IGNORECASE):
        return True

    # 2. 视频平台链接
    video_patterns = [
        r"youtube\.com/watch",
        r"youtu\.be/",
        r"vimeo\.com/",
        r"rumble\.com/",
        r"twitch\.tv/",
        r"dailymotion\.com/",
        r"bitchute\.com/",
        r"streamable\.com/",
        r"tiktok\.com/",
    ]
    for pattern in video_patterns:
        if re.search(pattern, combined):
            return True

    # 3. attachments/media 中包含视频类型
    attachments = entry.get("attachments") or entry.get("media") or []
    if isinstance(attachments, list):
        for att in attachments:
            if isinstance(att, dict):
                mime = (att.get("mime_type") or att.get("mimeType") or "").lower()
                url_in_att = (att.get("url") or "").lower()
                if mime.startswith("video/") or any(
                    re.search(p, url_in_att) for p in video_patterns
                ):
                    return True
            elif isinstance(att, str) and att.lower():
                if any(re.search(p, att.lower()) for p in video_patterns):
                    return True

    # 4. Folo 特有: entry.media 字段
    media = entry.get("media") or []
    if isinstance(media, list):
        for m in media:
            if isinstance(m, dict):
                media_type = (m.get("type") or "").lower()
                mime = (m.get("mime_type") or m.get("mimeType") or "").lower()
                url_in_media = (m.get("url") or "").lower()
                if media_type == "video" or mime.startswith("video/"):
                    return True
                if any(re.search(p, url_in_media) for p in video_patterns):
                    return True

    return False


def process_entries(entries, handle_map, seen_ids, debug=False):
    """
    处理条目列表：匹配人物、去重、分组

    Returns:
        (new_persons_quotes, updated_seen_ids, new_quote_ids, unmatched_info, video_filtered)
    """
    new_entries = []
    unmatched = []  # 记录未匹配的条目信息
    video_filtered = 0  # 视频过滤计数

    for entry in entries:
        # 过滤含视频的帖子
        if is_video_entry(entry):
            video_filtered += 1
            continue

        handle = extract_handle(entry)
        person = handle_map.get(handle) if handle else None
        if not person:
            unmatched.append({
                "handle": handle,
                "author": entry.get("author", ""),
                "url": entry.get("url", ""),
                "title": (entry.get("title") or "")[:60],
            })
            continue

        url = entry.get("url", "")
        tweet_id = extract_tweet_id(url)
        quote_id = f"{person['id']}_x_{tweet_id}" if tweet_id else f"{person['id']}_x_{entry.get('id', 'unknown')}"

        if quote_id not in seen_ids:
            new_entries.append((entry, person, quote_id))

    new_persons_quotes = {}
    new_quote_ids = []
    for entry, person, quote_id in new_entries:
        pid = person["id"]
        if pid not in new_persons_quotes:
            new_persons_quotes[pid] = {
                "id": person["id"],
                "name": person["name"],
                "title": person.get("title"),
                "stance": person["stance"],
                "domain": person["domain"],
                "quotes": [],
            }
        quote = entry_to_quote(entry, pid)
        new_persons_quotes[pid]["quotes"].append(quote)
        seen_ids.add(quote_id)
        new_quote_ids.append(quote_id)

    return new_persons_quotes, seen_ids, new_quote_ids, unmatched, video_filtered


def main():
    parser = argparse.ArgumentParser(description="从 Folo 订阅源获取 X 数据")
    parser.add_argument("--cli", action="store_true",
                        help="使用 Folo CLI 模式（需 npx folocli login 认证），支持分页获取全部数据")
    parser.add_argument("--debug", action="store_true",
                        help="调试模式：输出前3条原始条目的字段结构")
    args = parser.parse_args()

    handle_map = build_handle_map()
    print(f"已加载 {len(handle_map)} 个 X handle 映射")

    # 加载游标
    seen_ids, last_published_at = load_cursor()
    print(f"游标: 已记录 {len(seen_ids)} 条, last_published_at={last_published_at or '无'}")

    # 获取数据
    if args.cli:
        print("\n使用 Folo CLI 模式（支持分页）...")
        feed_title, entries = fetch_entries_cli()
    else:
        print("\n使用公开 API 模式（最多10条，建议 cron 高频运行）...")
        feed_info, entries = fetch_entries_public_api()
        feed_title = feed_info["title"]

    print(f"\n订阅源: {feed_title}")
    print(f"本次获取条目数: {len(entries)}")
    print("=" * 60)

    # 调试模式：输出前3条原始条目结构
    if args.debug and entries:
        print("\n🔍 调试: 前3条原始条目字段结构:")
        print("-" * 60)
        for i, entry in enumerate(entries[:3]):
            print(f"\n--- 条目 {i+1} ---")
            for key, value in entry.items():
                val_str = str(value)
                if len(val_str) > 100:
                    val_str = val_str[:100] + "..."
                print(f"  {key}: {val_str}")
        print("-" * 60)

    # 处理条目
    new_persons_quotes, seen_ids, new_quote_ids, unmatched, video_filtered = process_entries(
        entries, handle_map, seen_ids)

    # 打印未匹配条目（用于调试）
    if unmatched:
        print(f"\n⚠️  未匹配条目: {len(unmatched)} 条")
        # 统计未匹配的 handle 分布
        handle_dist = {}
        for u in unmatched:
            h = u.get("handle") or "(无handle)"
            handle_dist[h] = handle_dist.get(h, 0) + 1
        print("  Handle 分布:")
        for h, count in sorted(handle_dist.items(), key=lambda x: -x[1])[:20]:
            print(f"    {h}: {count} 条")
        # 打印前3条未匹配样本
        print("  未匹配样本:")
        for u in unmatched[:3]:
            print(f"    author={u['author']}, url={u['url'][:60]}, title={u['title'][:40]}")
        print()

    if not new_persons_quotes:
        print("\n没有匹配到 people.py 中的新条目")
        # 即使没有新条目也保存游标
        latest_published = last_published_at
        for entry in entries:
            pub = entry.get("publishedAt")
            if pub and (latest_published is None or pub > latest_published):
                latest_published = pub
        save_cursor(seen_ids, latest_published)
        return

    new_count = sum(len(p["quotes"]) for p in new_persons_quotes.values())
    matched_ids = set()
    for p in new_persons_quotes.values():
        for q in p["quotes"]:
            matched_ids.add(q["id"])
    already_count = len(entries) - new_count - len(unmatched) - video_filtered
    print(f"新条目: {new_count} 条（跳过 {already_count} 条已获取, {len(unmatched)} 条未匹配, {video_filtered} 条含视频过滤）\n")

    # 合并到已有数据
    existing_data = load_existing_data()
    merged = merge_data(existing_data, new_persons_quotes)

    # 更新 last_published_at（取所有新条目中最晚的 publishedAt）
    latest_published = last_published_at
    for entry in entries:
        pub = entry.get("publishedAt")
        if pub and (latest_published is None or pub > latest_published):
            latest_published = pub

    # 保存
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)
    save_cursor(seen_ids, latest_published)

    # 打印摘要
    total_count = sum(len(p["quotes"]) for p in merged)
    print(f"✅ 新增 {new_count} 条语录，涉及 {len(new_persons_quotes)} 个人物")
    print("-" * 60)
    for pid, pinfo in new_persons_quotes.items():
        print(f"  [{pinfo['stance'].upper()}] {pinfo['name']} - {len(pinfo['quotes'])} 条新增")
        for q in pinfo["quotes"]:
            preview = q["content"][:80] + ("..." if len(q["content"]) > 80 else "")
            print(f"    - {preview}")

    print(f"\n累计: {total_count} 条语录，游标已更新 ({len(seen_ids)} 个 ID)")
    print(f"结果已合并到 {OUTPUT_PATH}")

    if not args.cli:
        print(f"\n💡 提示: 公开 API 每次最多返回10条，建议设置 cron 每2-4小时运行一次:")
        print(f"   0 */3 * * * cd {os.getcwd()} && python3 xrss.py >> xrss.log 2>&1")
        print(f"   如需一次性获取全部历史数据，请使用: python3 xrss.py --cli")


if __name__ == "__main__":
    main()
