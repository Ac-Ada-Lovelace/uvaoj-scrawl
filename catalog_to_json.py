import argparse
import json
import time
from collections import deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from pathlib import Path
from typing import List
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

try:
    import requests
except ImportError as exc:  # pragma: no cover
    raise SystemExit(
        "缺少 requests 库，请先执行 `pip install requests beautifulsoup4`."
    ) from exc

try:
    from bs4 import BeautifulSoup
except ImportError as exc:  # pragma: no cover
    raise SystemExit(
        "缺少 beautifulsoup4 库，请先执行 `pip install beautifulsoup4`."
    ) from exc

BASE_URL = "https://onlinejudge.org/index.php?option=com_onlinejudge&Itemid=8"
ICON_KINDS = {"FOLDER", "FILE"}
HEADERS = {
    "User-Agent": "Mozilla/5.0 (UVA Catalog Full JSON Generator)",
    "Accept-Language": "en-US,en;q=0.9",
}


@dataclass
class CatalogNode:
    name: str
    url: str
    kind: str  # FOLDER / FILE
    children: List["CatalogNode"] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "url": self.url,
            "kind": self.kind,
            "children": [child.to_dict() for child in self.children],
        }


def normalize_url(url: str) -> str:
    parsed = urlparse(url)
    query_pairs = parse_qsl(parsed.query, keep_blank_values=True)
    filtered = [(k, v) for k, v in query_pairs if k not in {"limit", "limitstart"}]
    normalized = parsed._replace(query=urlencode(filtered, doseq=True))
    return urlunparse(normalized)


def fetch_html(url: str, *, delay: float = 0.0) -> str:
    if delay > 0:
        time.sleep(delay)
    resp = requests.get(url, headers=HEADERS, timeout=20)
    resp.raise_for_status()
    resp.encoding = resp.encoding or "utf-8"
    return resp.text


def _find_entry_table(soup: BeautifulSoup):
    for table in soup.find_all("table"):
        if table.find(
            "img",
            alt=lambda value: isinstance(value, str) and value.upper() in ICON_KINDS,
        ):
            return table
    return None


def parse_entries(html: str):
    soup = BeautifulSoup(html, "html.parser")
    table = _find_entry_table(soup)
    if table is None:
        return []

    entries = []
    for row in table.find_all("tr"):
        icon = row.find("img")
        link = row.find("a")
        if not icon or not link:
            continue
        kind = str(icon.get("alt", "")).strip().upper()
        if kind not in ICON_KINDS:
            continue
        href = link.get("href")
        if not href:
            continue
        name = link.get_text(" ", strip=True).replace("\xa0", " ")
        url = urljoin(BASE_URL, str(href))
        entries.append((name, url, kind))
    return entries


class FullCatalogCrawler:
    def __init__(self, *, max_workers: int = 4, delay: float = 0.0):
        self.max_workers = max_workers
        self.delay = delay

    def _fetch_entries(self, url: str):
        html = fetch_html(url, delay=self.delay)
        return parse_entries(html)

    def crawl(self, root_name: str, root_url: str) -> CatalogNode:
        root = CatalogNode(name=root_name, url=root_url, kind="FOLDER")
        queue: deque[CatalogNode] = deque([root])
        seen = {normalize_url(root.url)}

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {}
            while queue or futures:
                while queue and len(futures) < self.max_workers:
                    node = queue.popleft()
                    future = executor.submit(self._fetch_entries, node.url)
                    futures[future] = node

                if not futures:
                    continue

                done, _ = wait(futures.keys(), return_when=FIRST_COMPLETED)
                for future in done:
                    node = futures.pop(future)
                    try:
                        entries = future.result()
                    except Exception as exc:
                        print(f"[WARN] 抓取 {node.url} 失败: {exc}")
                        continue

                    for name, url, kind in entries:
                        child = CatalogNode(name=name, url=url, kind=kind)
                        node.children.append(child)
                        if kind == "FOLDER":
                            normalized = normalize_url(url)
                            if normalized in seen:
                                continue
                            seen.add(normalized)
                            queue.append(child)

        return root


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="抓取 UVA 目录并生成完整 JSON 结构（包含 FILE 节点）。"
    )
    parser.add_argument(
        "--root-url",
        default=BASE_URL,
        help="目录入口 URL，默认 UVA problemset 入口。",
    )
    parser.add_argument(
        "--root-name",
        default="Root",
        help="根节点名称，默认 Root。",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=4,
        help="并发线程数。",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.0,
        help="每个请求前的延迟（秒）。",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("uva_catalog.json"),
        help="输出 JSON 文件路径，默认 ./uva_catalog.json。",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    crawler = FullCatalogCrawler(max_workers=args.max_workers, delay=args.delay)
    root = crawler.crawl(args.root_name, args.root_url)

    data = root.to_dict()
    args.output.write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"目录结构已写入 {args.output}")


if __name__ == "__main__":
    main()
