import argparse
import json
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, ttk
from typing import Any, Dict, Optional


class CatalogViewerApp:
    """
    Tkinter 工具，用于浏览 UVA 目录 JSON，并支持从任意节点导出子树为 JSON 文件。
    """

    def __init__(self, master: tk.Tk, initial_file: Optional[Path] = None) -> None:
        self.master = master
        self.master.title("UVA Catalog Viewer")
        self._build_widgets()
        self._node_map: Dict[str, Dict[str, Any]] = {}

        if initial_file:
            self.load_json(initial_file)

    def _build_widgets(self) -> None:
        toolbar = ttk.Frame(self.master)
        toolbar.pack(fill=tk.X, padx=5, pady=5)

        open_btn = ttk.Button(toolbar, text="打开 JSON", command=self.open_file_dialog)
        open_btn.pack(side=tk.LEFT, padx=(0, 5))

        export_btn = ttk.Button(toolbar, text="导出所选节点", command=self.export_selected_node)
        export_btn.pack(side=tk.LEFT)

        tree_frame = ttk.Frame(self.master)
        tree_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=(0, 5))

        columns = ("details",)
        self.tree = ttk.Treeview(tree_frame, columns=columns, show="tree headings")
        self.tree.heading("#0", text="名称")
        self.tree.column("#0", stretch=True)
        self.tree.heading("details", text="说明")
        self.tree.column("details", width=200, anchor=tk.W, stretch=False)
        self.tree.pack(fill=tk.BOTH, expand=True, side=tk.LEFT)

        scrollbar = ttk.Scrollbar(tree_frame, orient="vertical", command=self.tree.yview)
        scrollbar.pack(fill=tk.Y, side=tk.RIGHT)
        self.tree.configure(yscrollcommand=scrollbar.set)

    def open_file_dialog(self) -> None:
        file_path = filedialog.askopenfilename(
            title="选择目录 JSON 文件",
            filetypes=(("JSON Files", "*.json"), ("All Files", "*.*")),
        )
        if not file_path:
            return
        self.load_json(Path(file_path))

    def load_json(self, path: Path) -> None:
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:  # pragma: no cover - Tk 应用主要依赖交互测试
            messagebox.showerror("加载失败", f"无法读取 {path}:\n{exc}")
            return

        if not isinstance(data, dict):
            messagebox.showerror("格式错误", "JSON 根节点必须是对象（包含 name/url/children 等信息）。")
            return

        self.tree.delete(*self.tree.get_children())
        self._node_map.clear()
        self._populate_tree("", data)
        self.master.title(f"UVA Catalog Viewer - {path}")

    def _populate_tree(self, parent_id: str, node: Dict[str, Any]) -> None:
        name = node.get("name", "(未命名)")
        kind = node.get("kind")
        has_files = node.get("has_file_children")

        details_parts = []
        if kind:
            details_parts.append(kind)
        if has_files:
            details_parts.append("contains problems")
        details = ", ".join(details_parts)

        tree_id = self.tree.insert(parent_id, tk.END, text=name, values=(details,))
        self._node_map[tree_id] = node

        for child in node.get("children", []):
            if isinstance(child, dict):
                self._populate_tree(tree_id, child)

    def export_selected_node(self) -> None:
        selection = self.tree.selection()
        if not selection:
            messagebox.showinfo("提示", "请先在左侧树中选择一个节点。")
            return

        node_data = self._node_map.get(selection[0])
        if not node_data:
            messagebox.showerror("错误", "未找到所选节点的数据。")
            return

        initial_name = node_data.get("name", "catalog").replace(" ", "_")
        file_path = filedialog.asksaveasfilename(
            title="导出为 JSON",
            initialfile=f"{initial_name}.json",
            defaultextension=".json",
            filetypes=(("JSON Files", "*.json"), ("All Files", "*.*")),
        )
        if not file_path:
            return

        try:
            Path(file_path).write_text(json.dumps(node_data, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception as exc:  # pragma: no cover
            messagebox.showerror("导出失败", f"无法写入 {file_path}:\n{exc}")
            return

        messagebox.showinfo("成功", f"已导出到 {file_path}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="UVA 目录 JSON 的 Tkinter 浏览/导出工具。")
    parser.add_argument(
        "--file",
        type=Path,
        help="启动时自动加载的 JSON 文件。",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    root = tk.Tk()
    CatalogViewerApp(root, initial_file=args.file)
    root.mainloop()


if __name__ == "__main__":
    main()
