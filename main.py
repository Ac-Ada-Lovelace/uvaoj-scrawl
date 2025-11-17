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
except ImportError as exc:  # pragma: no cover - 环境缺依赖时直接提示
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
    "User-Agent": "Mozilla/5.0 (UVA Catalog BFS)",
    "Accept-Language": "en-US,en;q=0.9",
}


@dataclass
class Entry:
    name: str
    url: str
    kind: str  # FOLDER 或 FILE


@dataclass
class CatalogNode:
    name: str
    url: str
    depth: int = 0
    children: List["CatalogNode"] = field(default_factory=list)
    has_file_children: bool = False

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "url": self.url,
            "has_file_children": self.has_file_children,
            "children": [child.to_dict() for child in self.children],
        }


def normalize_url(url: str) -> str:
    """
    统一 URL 表示，避免 limit 或 limitstart 之类的参数导致重复访问。
    """
    parsed = urlparse(url)
    query_pairs = parse_qsl(parsed.query, keep_blank_values=True)
    filtered = [(k, v) for k, v in query_pairs if k not in {"limit", "limitstart"}]
    normalized = parsed._replace(query=urlencode(filtered, doseq=True))
    return urlunparse(normalized)


def fetch_html(url: str, *, delay: float = 0.0) -> str:
    if delay > 0:
        time.sleep(delay)
    response = requests.get(url, headers=HEADERS, timeout=20)
    response.raise_for_status()
    response.encoding = response.encoding or "utf-8"
    return response.text


def _find_entry_table(soup: BeautifulSoup):
    """
    找到包含 FOLDER/FILE 图标的 table。页面上可能有多个 table，这里挑第一个满足条件的。
    """
    for table in soup.find_all("table"):
        if table.find(
            "img",
            alt=lambda value: isinstance(value, str) and value.upper() in ICON_KINDS,
        ):
            return table
    return None


def parse_entries(html: str) -> List[Entry]:
    soup = BeautifulSoup(html, "html.parser")
    table = _find_entry_table(soup)
    if table is None:
        return []

    entries: List[Entry] = []
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
        entries.append(Entry(name=name, url=url, kind=kind))
    return entries


class CatalogCrawler:
    """
    负责对 UVA 目录做 BFS，每个节点只展开 FOLDER 类型，遇到 FILE 只记录不深入。
    """

    def __init__(self, *, max_workers: int = 4, delay: float = 0.0):
        self.max_workers = max_workers
        self.delay = delay

    def _fetch_entries(self, url: str) -> List[Entry]:
        html = fetch_html(url, delay=self.delay)
        return parse_entries(html)

    def crawl(self, root_name: str, root_url: str) -> CatalogNode:
        root = CatalogNode(name=root_name, url=root_url, depth=0)
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
                        print(f"[WARN] 无法抓取 {node.url}: {exc}")
                        continue

                    folder_entries = [
                        entry for entry in entries if entry.kind == "FOLDER"
                    ]
                    node.has_file_children = any(
                        entry.kind == "FILE" for entry in entries
                    )

                    for entry in folder_entries:
                        normalized = normalize_url(entry.url)
                        if normalized in seen:
                            continue
                        child = CatalogNode(
                            name=entry.name,
                            url=entry.url,
                            depth=node.depth + 1,
                        )
                        node.children.append(child)
                        seen.add(normalized)
                        queue.append(child)

        return root


def render_markdown(root: CatalogNode) -> str:
    lines: List[str] = []

    def walk(node: CatalogNode, indent: int = 0) -> None:
        prefix = "  " * indent
        marker = f"{prefix}- [{node.name}]({node.url})"
        if node.has_file_children:
            marker += " *(contains problems)*"
        lines.append(marker)
        for child in node.children:
            walk(child, indent + 1)

    walk(root)
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="BFS 抓取 UVA Online Judge 目录结构。")
    parser.add_argument(
        "--root-url",
        default=BASE_URL,
        help="目录入口 URL，默认为 UVA problemset 入口。",
    )
    parser.add_argument(
        "--root-name",
        default="Root",
        help="目录根节点的名称，默认 Root。",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=4,
        help="并发线程数，默认 4。",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.0,
        help="每个请求前的延迟（秒），用于控制访问频率。",
    )
    parser.add_argument(
        "--format",
        choices=("markdown", "json"),
        default="markdown",
        help="输出格式，默认 markdown。",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="输出文件路径，不指定则打印到 stdout。",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    crawler = CatalogCrawler(max_workers=args.max_workers, delay=args.delay)
    root = crawler.crawl(args.root_name, args.root_url)

    if args.format == "json":
        content = json.dumps(root.to_dict(), ensure_ascii=False, indent=2)
    else:
        content = render_markdown(root)

    if args.output:
        args.output.write_text(content, encoding="utf-8")
        print(f"已写入 {args.output}")
    else:
        print(content)


if __name__ == "__main__":
    main()
