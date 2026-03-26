"""
终端纯文本表格与宽度计算辅助函数。
"""

import unicodedata
from typing import List, Optional, Sequence


def char_display_width(ch: str) -> int:
    """计算单个字符在终端中的显示宽度。"""
    if not ch:
        return 0
    if unicodedata.combining(ch):
        return 0
    if unicodedata.east_asian_width(ch) in ("F", "W"):
        return 2
    return 1


def display_width(text: object) -> int:
    """计算字符串在终端中的显示宽度。"""
    return sum(char_display_width(ch) for ch in str(text))


def truncate_display_text(text: object, max_width: int) -> str:
    """按显示宽度截断文本。"""
    value = str(text)
    if max_width <= 0:
        return ""
    if display_width(value) <= max_width:
        return value
    if max_width <= 3:
        return "." * max_width

    keep_width = max_width - 3
    chars = []
    used = 0
    for ch in value:
        ch_width = char_display_width(ch)
        if used + ch_width > keep_width:
            break
        chars.append(ch)
        used += ch_width
    return "".join(chars) + "..."


def format_cell(text: object, width: int, align: str = "left") -> str:
    """按显示宽度对齐单元格内容。"""
    value = truncate_display_text(text, width)
    padding = max(0, width - display_width(value))
    if align == "right":
        return " " * padding + value
    return value + " " * padding


def render_plain_table(
    headers: Sequence[str],
    rows: Sequence[Sequence[object]],
    aligns: Sequence[str],
    *,
    min_widths: Optional[Sequence[int]] = None,
    max_widths: Optional[Sequence[int]] = None,
    section_break_after_rows: Optional[Sequence[int]] = None,
    indent: str = "  ",
    col_gap: int = 2,
) -> List[str]:
    """渲染纯文本表格（按显示宽度对齐，兼容中文）。"""
    col_count = len(headers)
    if col_count == 0:
        return []

    min_widths = min_widths or [0] * col_count
    max_widths = max_widths or [0] * col_count
    align_list = list(aligns) if aligns else ["left"] * col_count
    if len(align_list) < col_count:
        align_list.extend(["left"] * (col_count - len(align_list)))

    col_widths: List[int] = []
    for i in range(col_count):
        width = display_width(headers[i])
        for row in rows:
            if i < len(row):
                width = max(width, display_width(row[i]))
        if i < len(min_widths):
            width = max(width, min_widths[i])
        if i < len(max_widths) and max_widths[i] > 0:
            width = min(width, max_widths[i])
        col_widths.append(width)

    def build_line(cells: Sequence[object]) -> str:
        rendered = []
        for i in range(col_count):
            value = cells[i] if i < len(cells) else ""
            rendered.append(format_cell(value, col_widths[i], align_list[i]))
        return indent + (" " * col_gap).join(rendered)

    lines = [build_line(headers)]
    separator = indent + "-" * (sum(col_widths) + col_gap * (col_count - 1))
    lines.append(separator)
    section_breaks = set(section_break_after_rows or [])
    for row_idx, row in enumerate(rows):
        lines.append(build_line(row))
        if row_idx in section_breaks and row_idx < len(rows) - 1:
            lines.append(separator)
    return lines


def format_percent(numerator: int, denominator: int) -> str:
    """格式化百分比。"""
    if denominator <= 0:
        return "-"
    return f"{(numerator / denominator) * 100:.1f}%"
