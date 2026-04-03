"""
基于 /api/v1/cluster_metric/list_task_dimension 的任务维度查询与本地看板。
"""

from __future__ import annotations

import json
import webbrowser
from collections import defaultdict
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse
from typing import Any, Dict, List, Tuple

from .api import QzAPIError, get_api
from .config import get_workspace_resources, load_all_resources
from .display import format_duration, get_display, truncate_string
from .resource_resolution import (
    ResourceResolutionError,
    resolve_cached_resource_ref,
    resolve_workspace_ref,
)

try:
    from rich import box
    from rich.table import Table

    _RICH_AVAILABLE = True
except ImportError:
    _RICH_AVAILABLE = False


TASK_COLUMNS: List[Dict[str, str]] = [
    {
        "key": "name",
        "label": "任务名",
        "kind": "string",
        "max_len": "40",
        "max_width": "320",
    },
    {
        "key": "status",
        "label": "状态",
        "kind": "string",
        "max_len": "16",
        "max_width": "120",
    },
    {
        "key": "type",
        "label": "类型",
        "kind": "string",
        "max_len": "24",
        "max_width": "180",
    },
    {
        "key": "priority",
        "label": "优先级",
        "kind": "number",
        "max_len": "8",
        "max_width": "80",
    },
    {
        "key": "user_name",
        "label": "用户",
        "kind": "string",
        "max_len": "14",
        "max_width": "120",
    },
    {
        "key": "project_name",
        "label": "项目",
        "kind": "string",
        "max_len": "24",
        "max_width": "200",
    },
    {
        "key": "workspace_name",
        "label": "工作空间",
        "kind": "string",
        "max_len": "18",
        "max_width": "160",
    },
    {
        "key": "node_types",
        "label": "节点类型",
        "kind": "string",
        "max_len": "18",
        "max_width": "120",
    },
    {
        "key": "node_count",
        "label": "节点数",
        "kind": "number",
        "max_len": "8",
        "max_width": "80",
    },
    {
        "key": "node_names",
        "label": "节点列表",
        "kind": "string",
        "max_len": "42",
        "max_width": "320",
    },
    {
        "key": "gpu_total",
        "label": "GPU 总数",
        "kind": "number",
        "max_len": "10",
        "max_width": "90",
    },
    {
        "key": "gpu_usage_rate_pct",
        "label": "GPU 利用率%",
        "kind": "number",
        "max_len": "10",
        "max_width": "110",
    },
    {
        "key": "cpu_total",
        "label": "CPU 总量",
        "kind": "number",
        "max_len": "12",
        "max_width": "110",
    },
    {
        "key": "cpu_usage_rate_pct",
        "label": "CPU 利用率%",
        "kind": "number",
        "max_len": "10",
        "max_width": "110",
    },
    {
        "key": "memory_total",
        "label": "内存总量",
        "kind": "number",
        "max_len": "12",
        "max_width": "110",
    },
    {
        "key": "memory_usage_rate_pct",
        "label": "内存利用率%",
        "kind": "number",
        "max_len": "10",
        "max_width": "120",
    },
    {
        "key": "created_at",
        "label": "创建时间",
        "kind": "string",
        "max_len": "24",
        "max_width": "190",
    },
    {
        "key": "running_time_ms",
        "label": "运行时长ms",
        "kind": "number",
        "max_len": "14",
        "max_width": "120",
    },
    {
        "key": "running_duration",
        "label": "运行时长",
        "kind": "string",
        "max_len": "14",
        "max_width": "120",
    },
    {
        "key": "id",
        "label": "任务ID",
        "kind": "string",
        "max_len": "36",
        "max_width": "280",
    },
]


HTML_PAGE = """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>QZCLI Tasks</title>
  <style>
    :root {
      --bg-0: #f4efe3;
      --bg-1: #fffaf0;
      --card: rgba(255, 250, 240, 0.82);
      --line: rgba(93, 78, 55, 0.14);
      --text: #221b16;
      --muted: #6d6255;
      --accent: #135d66;
      --accent-2: #9a3412;
      --good: #166534;
      --warn: #b45309;
      --bad: #b91c1c;
      --shadow: 0 18px 50px rgba(69, 49, 24, 0.12);
      --radius: 20px;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      color: var(--text);
      font-family: "PingFang SC", "Hiragino Sans GB", "Noto Sans CJK SC", "Microsoft YaHei", "Source Han Sans SC", sans-serif;
      background:
        radial-gradient(circle at 0% 0%, rgba(19, 93, 102, 0.16), transparent 28%),
        radial-gradient(circle at 100% 10%, rgba(154, 52, 18, 0.14), transparent 24%),
        linear-gradient(135deg, var(--bg-0), var(--bg-1));
      min-height: 100vh;
    }
    .wrap {
      width: min(1600px, calc(100vw - 28px));
      margin: 20px auto 40px;
    }
    .hero {
      position: relative;
      overflow: hidden;
      background: linear-gradient(145deg, rgba(255,255,255,0.78), rgba(255,248,235,0.9));
      border: 1px solid var(--line);
      border-radius: 28px;
      box-shadow: var(--shadow);
      padding: 22px 24px 18px;
      backdrop-filter: blur(10px);
    }
    .hero h1 {
      margin: 0;
      font-family: "Songti SC", "STSong", "Noto Serif CJK SC", "Source Han Serif SC", serif;
      font-size: clamp(28px, 4vw, 42px);
      letter-spacing: -0.03em;
    }
    .meta {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin-top: 12px;
    }
    .pill {
      border: 1px solid var(--line);
      padding: 7px 11px;
      border-radius: 999px;
      background: rgba(255,255,255,0.66);
      color: var(--muted);
      font-size: 13px;
    }
    .grid {
      display: grid;
      grid-template-columns: repeat(12, minmax(0, 1fr));
      gap: 14px;
      margin-top: 14px;
    }
    .card {
      grid-column: span 12;
      border: 1px solid var(--line);
      background: var(--card);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
      backdrop-filter: blur(12px);
      padding: 16px 16px 18px;
    }
    .card h2 {
      margin: 0 0 12px;
      font-size: 16px;
      letter-spacing: 0.01em;
    }
    .stats {
      display: grid;
      grid-template-columns: repeat(6, minmax(0, 1fr));
      gap: 10px;
    }
    .stat {
      background: rgba(255,255,255,0.65);
      border: 1px solid rgba(93, 78, 55, 0.1);
      border-radius: 16px;
      padding: 12px;
    }
    .stat .k {
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }
    .stat .v {
      margin-top: 6px;
      font-family: "Songti SC", "STSong", "Noto Serif CJK SC", "Source Han Serif SC", serif;
      font-size: 28px;
      line-height: 1;
    }
    .controls {
      display: grid;
      grid-template-columns: repeat(6, minmax(0, 1fr));
      gap: 10px;
    }
    label {
      display: block;
      font-size: 12px;
      color: var(--muted);
      margin-bottom: 6px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }
    input, select, button {
      width: 100%;
      border: 1px solid rgba(93, 78, 55, 0.16);
      border-radius: 12px;
      background: rgba(255,255,255,0.92);
      padding: 10px 12px;
      color: var(--text);
      font: inherit;
    }
    button {
      cursor: pointer;
      background: linear-gradient(135deg, #135d66, #0f766e);
      border-color: transparent;
      color: #fff8f1;
      font-weight: 600;
    }
    button.secondary {
      background: linear-gradient(135deg, #7c2d12, #c2410c);
    }
    button.danger {
      background: linear-gradient(135deg, #991b1b, #dc2626);
    }
    button:disabled {
      opacity: 0.55;
      cursor: not-allowed;
      transform: none;
    }
    .two-col {
      display: grid;
      grid-template-columns: 1.3fr 1fr;
      gap: 14px;
    }
    .counts {
      display: grid;
      gap: 8px;
      max-height: 420px;
      overflow: auto;
      padding-right: 4px;
    }
    .chip-box {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      min-height: 44px;
      padding: 8px;
      border: 1px solid rgba(93, 78, 55, 0.16);
      border-radius: 12px;
      background: rgba(255,255,255,0.92);
      align-content: flex-start;
    }
    .chip-box-compact {
      margin-top: 8px;
      min-height: 52px;
    }
    .chip {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      width: auto;
      border: 1px solid rgba(93, 78, 55, 0.16);
      border-radius: 999px;
      background: rgba(248, 243, 233, 0.92);
      color: #746757;
      padding: 7px 11px;
      font-size: 12px;
      line-height: 1;
      cursor: pointer;
      box-shadow: inset 0 0 0 1px rgba(255,255,255,0.32);
      transition: background 120ms ease, color 120ms ease, border-color 120ms ease, transform 120ms ease, box-shadow 120ms ease;
      user-select: none;
    }
    .chip:hover {
      transform: translateY(-1px);
      border-color: rgba(19, 93, 102, 0.28);
      color: var(--text);
    }
    .chip.active {
      background: linear-gradient(135deg, rgba(19, 93, 102, 0.22), rgba(154, 52, 18, 0.22));
      border-color: rgba(19, 93, 102, 0.55);
      color: #112f35;
      font-weight: 600;
      box-shadow: 0 0 0 2px rgba(19, 93, 102, 0.14);
    }
    .chip kbd {
      border: 1px solid rgba(93, 78, 55, 0.14);
      border-radius: 999px;
      padding: 2px 6px;
      background: rgba(255,255,255,0.74);
      font: inherit;
      font-size: 11px;
      color: var(--muted);
    }
    .chip .chip-mark {
      display: inline-flex;
      width: 16px;
      justify-content: center;
      color: transparent;
      font-weight: 700;
    }
    .chip.active .chip-mark {
      color: #135d66;
    }
    .chip.active kbd {
      border-color: rgba(19, 93, 102, 0.28);
      background: rgba(255,255,255,0.9);
      color: #135d66;
    }
    .chip-remove {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      width: 18px;
      height: 18px;
      border-radius: 999px;
      background: rgba(255,255,255,0.68);
      color: #7c2d12;
      font-weight: 700;
    }
    .stop-card {
      display: grid;
      gap: 14px;
    }
    .stop-summary {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 10px;
    }
    .selection-list {
      display: grid;
      gap: 8px;
      max-height: 220px;
      overflow: auto;
      padding-right: 4px;
    }
    .selection-item {
      display: grid;
      grid-template-columns: 1fr auto;
      gap: 10px;
      align-items: center;
      background: rgba(255,255,255,0.72);
      border: 1px solid rgba(93, 78, 55, 0.1);
      border-radius: 14px;
      padding: 10px 12px;
    }
    .selection-item strong {
      font-size: 13px;
    }
    .count-row {
      display: grid;
      grid-template-columns: 1fr auto;
      gap: 10px;
      align-items: center;
      background: rgba(255,255,255,0.72);
      border: 1px solid rgba(93, 78, 55, 0.1);
      border-radius: 14px;
      padding: 10px 12px;
    }
    .bar {
      height: 7px;
      border-radius: 999px;
      background: rgba(19, 93, 102, 0.12);
      overflow: hidden;
      margin-top: 8px;
    }
    .bar span {
      display: block;
      height: 100%;
      background: linear-gradient(90deg, var(--accent), var(--accent-2));
      border-radius: inherit;
    }
    .table-wrap {
      overflow: auto;
      border-radius: 16px;
      border: 1px solid rgba(93, 78, 55, 0.12);
      background: rgba(255,255,255,0.68);
    }
    table {
      width: 100%;
      border-collapse: collapse;
      min-width: 1400px;
      font-size: 13px;
    }
    .resizable-table {
      width: max-content;
      min-width: 100%;
      table-layout: fixed;
    }
    th, td {
      border-bottom: 1px solid rgba(93, 78, 55, 0.08);
      padding: 10px 12px;
      text-align: left;
      vertical-align: top;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    th {
      position: sticky;
      top: 0;
      z-index: 1;
      background: rgba(250, 245, 235, 0.96);
      cursor: pointer;
      user-select: none;
    }
    th.resizable-th {
      position: sticky;
      padding-right: 20px;
    }
    .th-label {
      display: block;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .col-resizer {
      position: absolute;
      top: 0;
      right: -4px;
      width: 10px;
      height: 100%;
      cursor: col-resize;
      touch-action: none;
      z-index: 3;
    }
    .col-resizer::after {
      content: "";
      position: absolute;
      top: 20%;
      bottom: 20%;
      left: 50%;
      width: 2px;
      transform: translateX(-50%);
      border-radius: 999px;
      background: rgba(19, 93, 102, 0.32);
      opacity: 0;
      transition: opacity 120ms ease, background 120ms ease;
    }
    th:hover .col-resizer::after,
    .col-resizer.active::after {
      opacity: 1;
    }
    body.is-resizing-columns,
    body.is-resizing-columns * {
      cursor: col-resize !important;
      user-select: none !important;
    }
    .muted { color: var(--muted); }
    .badge {
      display: inline-flex;
      align-items: center;
      padding: 4px 8px;
      border-radius: 999px;
      border: 1px solid rgba(93, 78, 55, 0.12);
      background: rgba(255,255,255,0.8);
    }
    .status-running { color: var(--good); }
    .status-queuing, .status-pending { color: var(--warn); }
    .status-failed, .status-stopped { color: var(--bad); }
    @media (max-width: 1200px) {
      .stats, .controls { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .two-col { grid-template-columns: 1fr; }
    }
    @media (max-width: 720px) {
      .wrap { width: min(100vw - 16px, 1600px); margin-top: 8px; }
      .hero { padding: 18px; border-radius: 22px; }
      .stats, .controls { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <h1>Task Dimension Explorer</h1>
      <div id="meta" class="meta"></div>
    </section>

    <section class="grid">
      <div class="card">
        <h2>Overview</h2>
        <div id="stats" class="stats"></div>
      </div>

      <div class="card">
        <h2>Controls</h2>
        <div class="controls">
          <div>
            <label for="workspaceSelect">Workspace</label>
            <select id="workspaceSelect">
              <option value="">Loading workspaces...</option>
            </select>
          </div>
          <div>
            <label for="projectSelect">Project</label>
            <select id="projectSelect">
              <option value="">Loading projects...</option>
            </select>
          </div>
          <div>
            <label for="groupSelect">Partition</label>
            <select id="groupSelect">
              <option value="">Loading partitions...</option>
            </select>
          </div>
          <div>
            <label for="search">Global Search</label>
            <input id="search" placeholder="任务名 / 用户 / 项目 / 节点" />
          </div>
          <div>
            <label for="filterColumn">Filter Column</label>
            <select id="filterColumn">
              <option value="">Loading columns...</option>
            </select>
          </div>
          <div>
            <label for="filterValue">Filter Value</label>
            <input id="filterValue" placeholder="子串匹配" />
          </div>
          <div>
            <label for="groupBySelect">Group By</label>
            <select id="groupBySelect">
              <option value="">Loading group fields...</option>
            </select>
            <div id="groupByChips" class="chip-box chip-box-compact"></div>
          </div>
          <div>
            <label for="statsColumn">Value Counts</label>
            <select id="statsColumn">
              <option value="">Loading columns...</option>
            </select>
          </div>
          <div>
            <label for="metricColumn">Numeric Metric</label>
            <select id="metricColumn">
              <option value="">Loading metrics...</option>
            </select>
          </div>
          <div>
            <label for="onlyMineToggle">Task Scope</label>
            <button id="onlyMineToggle" type="button" class="secondary">All Tasks</button>
          </div>
          <div>
            <label>&nbsp;</label>
            <button id="refreshButton">Refresh Data</button>
          </div>
          <div>
            <label>&nbsp;</label>
            <button id="resetButton" class="secondary">Reset Filters</button>
          </div>
        </div>
      </div>

      <div class="card">
        <h2>停止任务</h2>
        <div class="stop-card">
          <div class="stop-summary">
            <div class="stat">
              <div class="k">Selected</div>
              <div id="stopSelectedCount" class="v">0</div>
            </div>
            <div class="stat">
              <div class="k">Scope</div>
              <div id="stopScopeLabel" class="v" style="font-size:22px;">仅本人可停</div>
            </div>
          </div>
          <div>
            <div class="muted" style="margin-bottom:8px;">已选任务列表</div>
            <div id="stopSelectionList" class="selection-list"></div>
          </div>
          <div>
            <button id="stopSelectedButton" type="button" class="danger">停止任务</button>
          </div>
        </div>
      </div>

      <div class="card">
        <div class="two-col">
          <div>
            <h2 id="groupTitle">Grouped Summary</h2>
            <div class="table-wrap">
              <table id="groupTable"></table>
            </div>
          </div>
          <div>
            <h2 id="countsTitle">Value Counts</h2>
            <div id="counts" class="counts"></div>
          </div>
        </div>
      </div>

      <div class="card">
        <h2 id="tableTitle">Rows</h2>
        <div class="table-wrap">
          <table id="taskTable" class="resizable-table"></table>
        </div>
      </div>
    </section>
  </div>

  <script>
    const state = {
      snapshot: null,
      rows: [],
      columns: [],
      sortKey: "created_at_epoch",
      sortDir: "desc",
      groupSortKey: "count",
      groupSortDir: "desc",
      groupByKeys: [],
      statsColumn: "status",
      metricColumn: "gpu_total",
      filterColumn: "",
      filterValue: "",
      search: "",
      onlyMine: false,
      columnWidths: {},
      selectedRowIds: [],
      selectedWorkspaceId: "",
      selectedProject: "",
      selectedGroup: "",
    };

    const byId = (id) => document.getElementById(id);
    const COLUMN_WIDTH_STORAGE_KEY = "qzcli.task_dimensions.column_widths.v1";

    function fmtNumber(value) {
      const num = Number(value || 0);
      if (!Number.isFinite(num)) return "0";
      return num.toLocaleString("zh-CN", { maximumFractionDigits: 2 });
    }

    function badgeClass(status) {
      const key = String(status || "").toLowerCase();
      if (key.includes("running")) return "status-running";
      if (key.includes("queue") || key.includes("pending")) return "status-queuing";
      if (key.includes("fail") || key.includes("stop")) return "status-failed";
      return "";
    }

    function normalizeCell(value) {
      if (Array.isArray(value)) return value.join(", ");
      if (value === null || value === undefined) return "";
      if (typeof value === "object") return JSON.stringify(value);
      return String(value);
    }

    function shorten(value, maxLen) {
      const text = normalizeCell(value);
      if (!maxLen || text.length <= maxLen) return text;
      return `${text.slice(0, Math.max(1, maxLen - 1))}…`;
    }

    function compare(a, b, key, dir) {
      const av = a[key];
      const bv = b[key];
      const factor = dir === "asc" ? 1 : -1;
      const aMissing = av === null || av === undefined || av === "";
      const bMissing = bv === null || bv === undefined || bv === "";
      if (aMissing && bMissing) return 0;
      if (aMissing) return 1;
      if (bMissing) return -1;

      const aNum = Number(av);
      const bNum = Number(bv);
      const bothNumeric = Number.isFinite(aNum) && Number.isFinite(bNum);
      if (bothNumeric) {
        if (aNum === bNum) return 0;
        return (aNum - bNum) * factor;
      }

      const aText = normalizeCell(av);
      const bText = normalizeCell(bv);
      return aText.localeCompare(bText, "zh-CN", { numeric: true, sensitivity: "base" }) * factor;
    }

    function clamp(value, min, max) {
      return Math.min(Math.max(value, min), max);
    }

    function parseWidth(value, fallback = 160) {
      const num = Number(value);
      return Number.isFinite(num) && num > 0 ? num : fallback;
    }

    function getDefaultColumnWidth(col) {
      return clamp(parseWidth(col.max_width, 160), 80, 720);
    }

    function loadSavedColumnWidths() {
      try {
        const raw = window.localStorage.getItem(COLUMN_WIDTH_STORAGE_KEY);
        if (!raw) {
          return {};
        }
        const parsed = JSON.parse(raw);
        return parsed && typeof parsed === "object" ? parsed : {};
      } catch (error) {
        return {};
      }
    }

    function saveColumnWidths() {
      try {
        window.localStorage.setItem(COLUMN_WIDTH_STORAGE_KEY, JSON.stringify(state.columnWidths));
      } catch (error) {
      }
    }

    function hydrateColumnWidths(columns) {
      const saved = loadSavedColumnWidths();
      const next = {};
      for (const col of columns) {
        next[col.key] = clamp(parseWidth(saved[col.key], getDefaultColumnWidth(col)), 80, 720);
      }
      state.columnWidths = next;
    }

    function getColumnWidth(col) {
      return clamp(parseWidth(state.columnWidths[col.key], getDefaultColumnWidth(col)), 80, 720);
    }

    function renderHeaderCell(col) {
      const arrow = state.sortKey === col.key ? (state.sortDir === "asc" ? " ↑" : " ↓") : "";
      return `
        <th class="resizable-th" data-key="${col.key}" title="点击按 ${col.label} 排序">
          <span class="th-label">${col.label}${arrow}</span>
          <span class="col-resizer" data-key="${col.key}" title="拖动调整列宽"></span>
        </th>
      `;
    }

    function applyColumnWidth(table, key, width) {
      const col = table.querySelector(`col[data-key="${key}"]`);
      if (col) {
        col.style.width = `${width}px`;
      }
    }

    function wireColumnResize(table) {
      table.querySelectorAll(".col-resizer[data-key]").forEach((handle) => {
        handle.addEventListener("pointerdown", (event) => {
          if (event.pointerType !== "touch" && event.button !== 0) {
            return;
          }
          const key = handle.dataset.key;
          const column = state.columns.find((item) => item.key === key);
          if (!column) {
            return;
          }
          event.preventDefault();
          event.stopPropagation();

          const startX = event.clientX;
          const startWidth = getColumnWidth(column);
          handle.classList.add("active");
          document.body.classList.add("is-resizing-columns");

          const onMove = (moveEvent) => {
            const nextWidth = clamp(startWidth + (moveEvent.clientX - startX), 80, 720);
            state.columnWidths[key] = nextWidth;
            applyColumnWidth(table, key, nextWidth);
          };

          const stopResize = () => {
            handle.classList.remove("active");
            document.body.classList.remove("is-resizing-columns");
            document.removeEventListener("pointermove", onMove);
            document.removeEventListener("pointerup", stopResize);
            document.removeEventListener("pointercancel", stopResize);
            saveColumnWidths();
          };

          document.addEventListener("pointermove", onMove);
          document.addEventListener("pointerup", stopResize);
          document.addEventListener("pointercancel", stopResize);
        });
      });
    }

    function currentRows() {
      const search = state.search.trim().toLowerCase();
      const filterValue = state.filterValue.trim().toLowerCase();
      let rows = state.rows.filter((row) => {
        if (state.onlyMine && !row.is_mine) {
          return false;
        }
        if (search) {
          const hay = state.columns.map((col) => normalizeCell(row[col.key]).toLowerCase()).join(" | ");
          if (!hay.includes(search)) return false;
        }
        if (state.filterColumn && filterValue) {
          const value = normalizeCell(row[state.filterColumn]).toLowerCase();
          if (!value.includes(filterValue)) return false;
        }
        return true;
      });
      rows.sort((a, b) => compare(a, b, state.sortKey, state.sortDir));
      return rows;
    }

    function setOptions(select, items, includeEmpty = false, emptyLabel = "All") {
      const prevValues = select.multiple
        ? [...select.selectedOptions].map((opt) => opt.value)
        : [select.value];
      select.innerHTML = "";
      if (includeEmpty) {
        const option = document.createElement("option");
        option.value = "";
        option.textContent = emptyLabel;
        select.appendChild(option);
      }
      for (const item of items) {
        const option = document.createElement("option");
        option.value = item.value;
        option.textContent = item.label;
        select.appendChild(option);
      }
      if (select.multiple) {
        [...select.options].forEach((opt) => {
          opt.selected = prevValues.includes(opt.value);
        });
      } else if ([...select.options].some((opt) => opt.value === prevValues[0])) {
        select.value = prevValues[0];
      }
    }

    function setGroupBySelect(columns) {
      const select = byId("groupBySelect");
      const available = columns
        .filter((col) => !state.groupByKeys.includes(col.key))
        .map((col) => ({ value: col.key, label: `${col.label} (${col.key})` }));
      setOptions(select, available, true, "Add group field");
      select.value = "";
    }

    function renderGroupByChips(columns) {
      const root = byId("groupByChips");
      root.innerHTML = "";
      const selected = columns.filter((col) => state.groupByKeys.includes(col.key));
      if (!selected.length) {
        root.innerHTML = `<span class="muted">未选择分组字段</span>`;
        return;
      }
      for (const col of selected) {
        const chip = document.createElement("button");
        chip.type = "button";
        chip.className = "chip active";
        chip.dataset.key = col.key;
        chip.innerHTML = `<span class="chip-mark">✓</span><span>${col.label}</span><kbd>${col.key}</kbd><span class="chip-remove">×</span>`;
        chip.addEventListener("click", () => {
          state.groupByKeys = state.groupByKeys.filter((key) => key !== col.key);
          state.groupSortKey = "count";
          state.groupSortDir = "desc";
          syncInputs();
          render();
        });
        root.appendChild(chip);
      }
    }

    function updateMeta(snapshot) {
      const meta = byId("meta");
      meta.innerHTML = "";
      const pills = [
        `Workspace: ${snapshot.workspace_name || snapshot.workspace_id}`,
        `Workspace ID: ${snapshot.workspace_id}`,
        `Project Filter: ${snapshot.project_display || "全部项目"}`,
        `Partition Filter: ${snapshot.group_display || "全部分区"}`,
        `My Tasks In Workspace: ${fmtNumber(snapshot.my_task_count || 0)}`,
        `Endpoint: ${snapshot.endpoint}`,
        `Generated: ${snapshot.generated_at}`,
      ];
      for (const text of pills) {
        const el = document.createElement("div");
        el.className = "pill";
        el.textContent = text;
        meta.appendChild(el);
      }
    }

    function updateStats(rows) {
      const users = new Set(rows.map((row) => row.user_name).filter(Boolean));
      const projects = new Set(rows.map((row) => row.project_name).filter(Boolean));
      const running = rows.filter((row) => String(row.status).toUpperCase() === "RUNNING").length;
      const gpu = rows.reduce((sum, row) => sum + Number(row.gpu_total || 0), 0);
      const cpu = rows.reduce((sum, row) => sum + Number(row.cpu_total || 0), 0);
      const mem = rows.reduce((sum, row) => sum + Number(row.memory_total || 0), 0);
      const cards = [
        ["Rows", fmtNumber(rows.length)],
        ["Running", fmtNumber(running)],
        ["Users", fmtNumber(users.size)],
        ["Projects", fmtNumber(projects.size)],
        ["GPU Total", fmtNumber(gpu)],
        ["CPU Total", fmtNumber(cpu)],
      ];
      byId("stats").innerHTML = cards.map(([k, v]) => `<div class="stat"><div class="k">${k}</div><div class="v">${v}</div></div>`).join("");
      byId("stats").insertAdjacentHTML("beforeend", `<div class="stat"><div class="k">Memory Total</div><div class="v">${fmtNumber(mem)}</div></div>`);
    }

    function updateCounts(rows) {
      const column = state.statsColumn || "status";
      const counter = new Map();
      for (const row of rows) {
        const key = normalizeCell(row[column]) || "(empty)";
        counter.set(key, (counter.get(key) || 0) + 1);
      }
      const items = [...counter.entries()].sort((a, b) => b[1] - a[1]).slice(0, 30);
      const max = items.length ? items[0][1] : 1;
      byId("countsTitle").textContent = `Value Counts · ${column || "status"}`;
      byId("counts").innerHTML = items.map(([name, count]) => `
        <div class="count-row">
          <div>
            <div>${name}</div>
            <div class="bar"><span style="width:${(count / max) * 100}%"></span></div>
          </div>
          <strong>${fmtNumber(count)}</strong>
        </div>
      `).join("");
    }

    function updateGroups(rows) {
      const groupKeys = state.groupByKeys;
      const metricKey = state.metricColumn;
      const table = byId("groupTable");
      if (!groupKeys.length) {
        byId("groupTitle").textContent = "Grouped Summary";
        table.innerHTML = "<thead><tr><th>提示</th></tr></thead><tbody><tr><td class='muted'>选择 Group By 后显示分组统计。</td></tr></tbody>";
        return;
      }
      const groups = new Map();
      for (const row of rows) {
        const groupValues = {};
        for (const key of groupKeys) {
          groupValues[key] = row[key];
        }
        const mapKey = JSON.stringify(groupKeys.map((key) => normalizeCell(groupValues[key]) || "(empty)"));
        if (!groups.has(mapKey)) {
          const seed = {
            count: 0,
            gpu: 0,
            cpu: 0,
            mem: 0,
            metric: 0,
            groupValues,
          };
          for (const key of groupKeys) {
            seed[key] = groupValues[key];
          }
          groups.set(mapKey, seed);
        }
        const item = groups.get(mapKey);
        item.count += 1;
        item.gpu += Number(row.gpu_total || 0);
        item.cpu += Number(row.cpu_total || 0);
        item.mem += Number(row.memory_total || 0);
        item.metric += Number(row[metricKey] || 0);
      }
      const items = [...groups.values()].sort((a, b) => compare(a, b, state.groupSortKey, state.groupSortDir));
      const totals = items.reduce((acc, item) => {
        acc.count += Number(item.count || 0);
        acc.gpu += Number(item.gpu || 0);
        acc.cpu += Number(item.cpu || 0);
        acc.mem += Number(item.mem || 0);
        acc.metric += Number(item.metric || 0);
        return acc;
      }, { count: 0, gpu: 0, cpu: 0, mem: 0, metric: 0 });
      byId("groupTitle").textContent = `Grouped Summary · ${groupKeys.join(" + ")}`;
      table.innerHTML = `
        <thead>
          <tr>
            ${groupKeys.map((key) => `<th data-group-key="${key}" title="点击按 ${key} 排序">${key}${state.groupSortKey === key ? (state.groupSortDir === "asc" ? " ↑" : " ↓") : ""}</th>`).join("")}
            <th data-group-key="count" title="点击按 Count 排序">Count${state.groupSortKey === "count" ? (state.groupSortDir === "asc" ? " ↑" : " ↓") : ""}</th>
            <th data-group-key="gpu" title="点击按 GPU Sum 排序">GPU Sum${state.groupSortKey === "gpu" ? (state.groupSortDir === "asc" ? " ↑" : " ↓") : ""}</th>
            <th data-group-key="cpu" title="点击按 CPU Sum 排序">CPU Sum${state.groupSortKey === "cpu" ? (state.groupSortDir === "asc" ? " ↑" : " ↓") : ""}</th>
            <th data-group-key="mem" title="点击按 Memory Sum 排序">Memory Sum${state.groupSortKey === "mem" ? (state.groupSortDir === "asc" ? " ↑" : " ↓") : ""}</th>
            <th data-group-key="metric" title="点击按 ${metricKey} Sum 排序">${metricKey} Sum${state.groupSortKey === "metric" ? (state.groupSortDir === "asc" ? " ↑" : " ↓") : ""}</th>
          </tr>
        </thead>
        <tbody>
          ${items.map((item) => `
            <tr>
              ${groupKeys.map((key) => `<td title="${normalizeCell(item.groupValues[key]).replace(/"/g, "&quot;")}">${shorten(item.groupValues[key], 24) || "(empty)"}</td>`).join("")}
              <td>${fmtNumber(item.count)}</td>
              <td>${fmtNumber(item.gpu)}</td>
              <td>${fmtNumber(item.cpu)}</td>
              <td>${fmtNumber(item.mem)}</td>
              <td>${fmtNumber(item.metric)}</td>
            </tr>
          `).join("")}
          <tr>
            ${groupKeys.map((key, index) => `<td><strong>${index === 0 ? "Sum" : "-"}</strong></td>`).join("")}
            <td><strong>${fmtNumber(totals.count)}</strong></td>
            <td><strong>${fmtNumber(totals.gpu)}</strong></td>
            <td><strong>${fmtNumber(totals.cpu)}</strong></td>
            <td><strong>${fmtNumber(totals.mem)}</strong></td>
            <td><strong>${fmtNumber(totals.metric)}</strong></td>
          </tr>
        </tbody>
      `;
      table.querySelectorAll("th[data-group-key]").forEach((th) => {
        th.addEventListener("click", () => {
          const key = th.dataset.groupKey;
          const defaultDir = groupKeys.includes(key) ? "asc" : "desc";
          if (state.groupSortKey === key) {
            state.groupSortDir = state.groupSortDir === "asc" ? "desc" : "asc";
          } else {
            state.groupSortKey = key;
            state.groupSortDir = defaultDir;
          }
          render();
        });
      });
    }

    function updateTable(rows) {
      const table = byId("taskTable");
      const columns = state.columns;
      byId("tableTitle").textContent = `Rows · ${fmtNumber(rows.length)}`;
      table.innerHTML = `
        <colgroup>
          <col style="width:72px;" />
          ${columns.map((col) => `<col data-key="${col.key}" style="width:${getColumnWidth(col)}px;" />`).join("")}
        </colgroup>
        <thead>
          <tr>
            <th style="width:72px;max-width:72px;">Select</th>
            ${columns.map((col) => renderHeaderCell(col)).join("")}
          </tr>
        </thead>
        <tbody>
          ${rows.map((row) => `
            <tr>
              <td style="max-width:72px;">
                ${row.is_mine ? `<input type="checkbox" data-row-id="${row.id}" ${state.selectedRowIds.includes(row.id) ? "checked" : ""} />` : `<span class="muted">-</span>`}
              </td>
              ${columns.map((col) => {
                const value = normalizeCell(row[col.key]);
                const shortValue = shorten(value, Number(col.max_len || 0));
                if (col.key === "status") {
                  return `<td title="${value.replace(/"/g, "&quot;")}"><span class="badge ${badgeClass(value)}">${shortValue || "-"}</span></td>`;
                }
                return `<td title="${value.replace(/"/g, "&quot;")}">${shortValue || "-"}</td>`;
              }).join("")}
            </tr>
          `).join("")}
        </tbody>
      `;
      table.querySelectorAll("input[data-row-id]").forEach((input) => {
        input.addEventListener("change", (event) => {
          const rowId = event.target.dataset.rowId;
          if (event.target.checked) {
            if (!state.selectedRowIds.includes(rowId)) {
              state.selectedRowIds = [...state.selectedRowIds, rowId];
            }
          } else {
            state.selectedRowIds = state.selectedRowIds.filter((id) => id !== rowId);
          }
          updateSelectionControls();
        });
      });
      table.querySelectorAll("th[data-key]").forEach((th) => {
        th.addEventListener("click", () => {
          const key = th.dataset.key;
          if (state.sortKey === key) {
            state.sortDir = state.sortDir === "asc" ? "desc" : "asc";
          } else {
            state.sortKey = key;
            state.sortDir = "desc";
          }
          render();
        });
      });
      wireColumnResize(table);
    }

    function syncInputs() {
      byId("workspaceSelect").value = state.selectedWorkspaceId;
      byId("projectSelect").value = state.selectedProject;
      byId("groupSelect").value = state.selectedGroup;
      byId("search").value = state.search;
      byId("filterColumn").value = state.filterColumn;
      byId("filterValue").value = state.filterValue;
      byId("statsColumn").value = state.statsColumn;
      byId("metricColumn").value = state.metricColumn;
      byId("onlyMineToggle").textContent = state.onlyMine ? "Only Mine" : "All Tasks";
      setGroupBySelect(state.columns);
      renderGroupByChips(state.columns);
      updateSelectionControls();
    }

    function getRowsById() {
      return new Map(state.rows.map((row) => [row.id, row]));
    }

    function buildStopPreview(rows, limit = 12, withError = false) {
      const preview = rows.slice(0, limit).map((row) => {
        if (withError && row.error) {
          return `- ${row.name || row.id} [${row.error}]`;
        }
        return `- ${row.name || row.id}`;
      }).join("\\n");
      const suffix = rows.length > limit ? `\\n... 还有 ${rows.length - limit} 个任务` : "";
      return `${preview}${suffix}`;
    }

    async function requestStop(jobIds) {
      const response = await fetch("/api/stop", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          workspace_id: state.selectedWorkspaceId,
          project: state.selectedProject,
          group: state.selectedGroup,
          job_ids: jobIds,
        }),
      });
      const payload = await response.json();
      if (!response.ok) {
        throw new Error(payload.error || "停止任务失败");
      }
      return payload;
    }

    async function stopSelectedJobs() {
      if (!state.selectedRowIds.length) {
        return;
      }
      const rowsById = getRowsById();
      const selectedRows = state.selectedRowIds.map((id) => rowsById.get(id)).filter(Boolean);
      const confirmText = `将要停止 ${selectedRows.length} 个任务：\\n\\n${buildStopPreview(selectedRows)}\\n\\n确认继续？`;
      if (!window.confirm(confirmText)) {
        return;
      }

      let pendingIds = [...state.selectedRowIds];
      let stoppedCount = 0;

      while (pendingIds.length) {
        let payload;
        try {
          payload = await requestStop(pendingIds);
        } catch (error) {
          const retry = window.confirm(`本轮停止请求失败：${error.message || error}\\n\\n点击“确定”重试这 ${pendingIds.length} 个任务，点击“取消”结束。`);
          if (!retry) {
            state.selectedRowIds = pendingIds;
            syncInputs();
            render();
            return;
          }
          continue;
        }

        const results = payload.results || [];
        const failed = results.filter((item) => !item.stopped);
        stoppedCount += results.length - failed.length;

        await loadData(true);

        if (!failed.length) {
          state.selectedRowIds = [];
          syncInputs();
          render();
          window.alert(`停止完成，共停止 ${stoppedCount} 个任务。`);
          return;
        }

        const refreshedRowsById = getRowsById();
        const failedRows = failed.map((item) => ({
          id: item.job_id,
          name: (refreshedRowsById.get(item.job_id) || rowsById.get(item.job_id) || {}).name || item.job_id,
          error: item.error || "stop_failed",
        }));
        const retryText = `已停止 ${stoppedCount} 个任务，仍有 ${failedRows.length} 个失败：\\n\\n${buildStopPreview(failedRows, 12, true)}\\n\\n点击“确定”重试失败的一批，点击“取消”结束。`;
        const retry = window.confirm(retryText);
        if (!retry) {
          state.selectedRowIds = failedRows
            .map((row) => row.id)
            .filter((id) => refreshedRowsById.has(id) && refreshedRowsById.get(id).is_mine);
          syncInputs();
          render();
          return;
        }
        pendingIds = failedRows.map((row) => row.id);
      }
    }

    function updateSelectionControls() {
      const stoppableIds = new Set(state.rows.filter((row) => row.is_mine).map((row) => row.id));
      state.selectedRowIds = state.selectedRowIds.filter((id) => stoppableIds.has(id));
      const button = byId("stopSelectedButton");
      const count = state.selectedRowIds.length;
      button.disabled = count === 0;
      button.textContent = count ? `停止任务 (${count})` : "停止任务";
      byId("stopSelectedCount").textContent = String(count);
      const rowsById = new Map(state.rows.map((row) => [row.id, row]));
      const list = byId("stopSelectionList");
      const selectedRows = state.selectedRowIds.map((id) => rowsById.get(id)).filter(Boolean);
      if (!selectedRows.length) {
        list.innerHTML = `<div class="muted">未选择任何可停止的任务</div>`;
        return;
      }
      list.innerHTML = selectedRows.map((row) => `
        <div class="selection-item">
          <div>
            <strong>${shorten(row.name, 48) || row.id}</strong>
            <div class="muted">${row.user_name || "-"} | P${row.priority} | ${row.project_name || "-"} </div>
          </div>
          <div class="muted">${shorten(row.id, 18)}</div>
        </div>
      `).join("");
    }

    function render() {
      const rows = currentRows();
      updateStats(rows);
      updateCounts(rows);
      updateGroups(rows);
      updateTable(rows);
      updateSelectionControls();
    }

    function wireInputs() {
      byId("workspaceSelect").addEventListener("change", (event) => {
        state.selectedWorkspaceId = event.target.value;
        state.selectedProject = "";
        state.selectedGroup = "";
        loadData(false);
      });
      byId("projectSelect").addEventListener("change", (event) => {
        state.selectedProject = event.target.value;
        loadData(false);
      });
      byId("groupSelect").addEventListener("change", (event) => {
        state.selectedGroup = event.target.value;
        loadData(false);
      });
      byId("search").addEventListener("input", (event) => { state.search = event.target.value; render(); });
      byId("filterColumn").addEventListener("change", (event) => { state.filterColumn = event.target.value; render(); });
      byId("filterValue").addEventListener("input", (event) => { state.filterValue = event.target.value; render(); });
      byId("groupBySelect").addEventListener("change", (event) => {
        const value = event.target.value;
        if (!value) {
          return;
        }
        if (!state.groupByKeys.includes(value)) {
          state.groupByKeys = [...state.groupByKeys, value];
        }
        state.groupSortKey = "count";
        state.groupSortDir = "desc";
        syncInputs();
        render();
      });
      byId("statsColumn").addEventListener("change", (event) => { state.statsColumn = event.target.value; render(); });
      byId("metricColumn").addEventListener("change", (event) => { state.metricColumn = event.target.value; render(); });
      byId("onlyMineToggle").addEventListener("click", () => {
        state.onlyMine = !state.onlyMine;
        state.selectedRowIds = [];
        syncInputs();
        render();
      });
      byId("refreshButton").addEventListener("click", () => loadData(true));
      byId("resetButton").addEventListener("click", () => {
        state.search = "";
        state.filterColumn = "";
        state.filterValue = "";
        state.groupByKeys = state.snapshot.default_group_by || [];
        state.groupSortKey = "count";
        state.groupSortDir = "desc";
        state.sortKey = "created_at_epoch";
        state.sortDir = "desc";
        state.statsColumn = (state.snapshot.default_group_by || [])[0] || "status";
        state.metricColumn = "gpu_total";
        state.onlyMine = false;
        state.selectedRowIds = [];
        syncInputs();
        render();
      });
      byId("stopSelectedButton").addEventListener("click", stopSelectedJobs);
    }

    async function loadData(refresh = false) {
      const params = new URLSearchParams();
      if (state.selectedWorkspaceId) params.set("workspace_id", state.selectedWorkspaceId);
      if (state.selectedProject) params.set("project", state.selectedProject);
      if (state.selectedGroup) params.set("group", state.selectedGroup);
      const prefix = refresh ? "/api/refresh" : "/api/tasks";
      const response = await fetch(params.size ? `${prefix}?${params.toString()}` : prefix, { cache: "no-store" });
      const snapshot = await response.json();
      state.snapshot = snapshot;
      state.rows = snapshot.rows || [];
      state.selectedRowIds = [];
      state.columns = snapshot.columns || [];
      hydrateColumnWidths(state.columns);
      state.selectedWorkspaceId = snapshot.selected_workspace_id || "";
      state.selectedProject = snapshot.selected_project || "";
      state.selectedGroup = snapshot.selected_group || "";
      const options = state.columns.map((col) => ({ value: col.key, label: `${col.label} (${col.key})` }));
      const numericOptions = state.columns.filter((col) => col.kind === "number").map((col) => ({ value: col.key, label: `${col.label} (${col.key})` }));
      setOptions(
        byId("workspaceSelect"),
        (snapshot.workspace_options || []).map((item) => ({ value: item.id, label: item.name || item.id }))
      );
      setOptions(
        byId("projectSelect"),
        (snapshot.project_options || []).map((item) => ({ value: item.id, label: item.name || item.id })),
        true,
        "All projects"
      );
      setOptions(
        byId("groupSelect"),
        (snapshot.group_options || []).map((item) => ({ value: item.id, label: item.name || item.id })),
        true,
        "All partitions"
      );
      setOptions(byId("filterColumn"), options, true, "All columns");
      setOptions(byId("statsColumn"), options);
      setOptions(byId("metricColumn"), numericOptions);
      updateMeta(snapshot);
      const defaultGroupBy = Array.isArray(snapshot.default_group_by)
        ? snapshot.default_group_by
        : (snapshot.default_group_by ? [snapshot.default_group_by] : []);
      state.groupByKeys = state.groupByKeys.length ? state.groupByKeys : defaultGroupBy;
      state.statsColumn = state.statsColumn || defaultGroupBy[0] || "status";
      syncInputs();
      render();
    }

    wireInputs();
    loadData(false).catch((error) => {
      byId("stats").innerHTML = `<div class="stat"><div class="k">Load Error</div><div class="v">${error}</div></div>`;
    });
  </script>
</body>
</html>
"""


def _as_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _as_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _parse_created_at(value: str) -> int:
    if not value:
        return 0

    candidate = value.strip()
    if candidate.count(" ") >= 4:
        candidate = candidate.rsplit(" ", 1)[0]

    for fmt in ("%Y-%m-%d %H:%M:%S %z", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            return int(datetime.strptime(candidate, fmt).timestamp() * 1000)
        except ValueError:
            continue
    return 0


def _list_workspace_options() -> List[Dict[str, Any]]:
    all_resources = load_all_resources()
    items = []
    for workspace_id, ws_data in all_resources.items():
        projects = sorted(
            (
                {"id": project_id, "name": project.get("name", project_id)}
                for project_id, project in (ws_data.get("projects") or {}).items()
            ),
            key=lambda item: (item["name"], item["id"]),
        )
        groups = sorted(
            (
                {"id": group_id, "name": group.get("name", group_id)}
                for group_id, group in (ws_data.get("compute_groups") or {}).items()
            ),
            key=lambda item: (item["name"], item["id"]),
        )
        items.append(
            {
                "id": workspace_id,
                "name": ws_data.get("name", workspace_id),
                "projects": projects,
                "compute_groups": groups,
                "updated_at": ws_data.get("updated_at", 0),
            }
        )
    return sorted(items, key=lambda item: (item["name"], item["id"]))


def _pick_workspace(
    requested_workspace: str,
    cookie_workspace_id: str,
    workspace_options: List[Dict[str, Any]],
) -> Tuple[str, str]:
    if requested_workspace:
        return resolve_workspace_ref(requested_workspace)

    if cookie_workspace_id:
        ws_resources = get_workspace_resources(cookie_workspace_id) or {}
        return cookie_workspace_id, ws_resources.get("name", cookie_workspace_id)

    if workspace_options:
        preferred_names = ("分布式训练空间",)
        for preferred_name in preferred_names:
            for item in workspace_options:
                if str(item.get("name", "") or "") == preferred_name:
                    return item["id"], item["name"]
        for preferred_name in preferred_names:
            for item in workspace_options:
                if preferred_name in str(item.get("name", "") or ""):
                    return item["id"], item["name"]
        first = workspace_options[0]
        return first["id"], first["name"]

    raise ResourceResolutionError(
        "未找到可用工作空间；请先运行 qzcli catalog -u 预热缓存，或显式传 -w"
    )


def _resolve_project_in_workspace(
    workspace_id: str, project_input: str
) -> Tuple[str, str, str]:
    project_id = ""
    project_display = project_input
    project_name_filter = ""

    if not project_input:
        return project_id, project_display, project_name_filter

    if project_input.startswith("project-"):
        return project_input, project_display, project_name_filter

    try:
        project_id, project_display = resolve_cached_resource_ref(
            workspace_id, "projects", project_input
        )
    except ResourceResolutionError:
        project_name_filter = project_input.lower()

    return project_id, project_display, project_name_filter


def _resolve_group_in_workspace(workspace_id: str, group_input: str) -> Tuple[str, str]:
    if not group_input:
        return "", ""

    if group_input.startswith("lcg-"):
        ws_resources = get_workspace_resources(workspace_id) or {}
        group = (ws_resources.get("compute_groups") or {}).get(group_input, {})
        return group_input, group.get("name", group_input)

    group_id, group_display = resolve_cached_resource_ref(
        workspace_id, "compute_groups", group_input
    )
    return group_id, group_display


def _default_group_in_workspace(workspace_id: str) -> Tuple[str, str]:
    ws_resources = get_workspace_resources(workspace_id) or {}
    groups = ws_resources.get("compute_groups") or {}
    preferred_names = ("分布式训练",)

    for preferred_name in preferred_names:
        for group_id, group in groups.items():
            group_name = str(group.get("name", "") or "")
            if group_name == preferred_name:
                return group_id, group_name

    for preferred_name in preferred_names:
        for group_id, group in groups.items():
            group_name = str(group.get("name", "") or "")
            if preferred_name in group_name:
                return group_id, group_name

    return "", ""


def _collect_group_node_names(
    api, workspace_id: str, cookie: str, group_id: str
) -> set[str]:
    page_num = 1
    node_names: set[str] = set()
    while True:
        data = api.list_node_dimension(
            workspace_id,
            cookie,
            logic_compute_group_id=group_id,
            page_num=page_num,
            page_size=200,
        )
        page_nodes = data.get("node_dimensions", [])
        if not page_nodes:
            break
        for node in page_nodes:
            name = str(node.get("name", "") or "")
            if name:
                node_names.add(name)
        total = data.get("total")
        if total is not None and len(node_names) >= int(total):
            break
        if len(page_nodes) < 200:
            break
        page_num += 1
    return node_names


def _resolve_workspace_and_project(
    args,
    *,
    allow_auto_workspace: bool = False,
) -> Tuple[str, str, str, str, str, List[Dict[str, Any]]]:
    api = get_api()
    cookie_data = api.ensure_cookie()
    workspace_options = _list_workspace_options()
    workspace_input = (args.workspace or "").strip()

    if not workspace_input and not allow_auto_workspace:
        workspace_id = cookie_data.get("workspace_id", "")
        if not workspace_id:
            raise ResourceResolutionError("请指定工作空间: qzcli tasks -w <名称或ID>")
        ws_resources = get_workspace_resources(workspace_id) or {}
        workspace_name = ws_resources.get("name", workspace_id)
    else:
        workspace_id, workspace_name = _pick_workspace(
            workspace_input,
            cookie_data.get("workspace_id", "") if allow_auto_workspace else "",
            workspace_options,
        )

    project_input = (args.project or "").strip()
    project_id, project_display, project_name_filter = _resolve_project_in_workspace(
        workspace_id,
        project_input,
    )

    return (
        workspace_id,
        workspace_name,
        project_id,
        project_display,
        project_name_filter,
        workspace_options,
    )


def _fetch_task_dimensions(
    workspace_id: str,
    workspace_name: str,
    *,
    project_id: str = "",
    project_display: str = "",
    project_name_filter: str = "",
    group_id: str = "",
    group_display: str = "",
    page_size: int = 100,
) -> Dict[str, Any]:
    api = get_api()
    cookie = api.ensure_cookie()["cookie"]
    my_job_ids = _fetch_my_job_ids(api, workspace_id, cookie)

    tasks: List[Dict[str, Any]] = []
    page_num = 1
    while True:
        data = api.list_task_dimension(
            workspace_id,
            cookie,
            project_id=project_id or None,
            page_num=page_num,
            page_size=page_size,
        )
        page_tasks = data.get("task_dimensions", [])
        tasks.extend(page_tasks)
        total = data.get("total", 0)
        if len(tasks) >= total or not page_tasks:
            break
        page_num += 1

    if project_name_filter:
        tasks = [
            task
            for task in tasks
            if project_name_filter
            in (task.get("project", {}) or {}).get("name", "").lower()
        ]

    if group_id:
        group_nodes = _collect_group_node_names(api, workspace_id, cookie, group_id)
        tasks = [
            task
            for task in tasks
            if group_nodes.intersection(
                (task.get("nodes_occupied") or {}).get("nodes") or []
            )
        ]

    rows = [
        _flatten_task_dimension(
            task,
            workspace_id,
            workspace_name,
            is_mine=str(task.get("id", "") or "") in my_job_ids,
        )
        for task in tasks
    ]
    return {
        "workspace_id": workspace_id,
        "workspace_name": workspace_name,
        "project_display": project_display or (project_name_filter or ""),
        "group_display": group_display,
        "my_task_count": len(my_job_ids),
        "rows": rows,
    }


def _flatten_task_dimension(
    task: Dict[str, Any],
    workspace_id: str,
    workspace_name: str,
    *,
    is_mine: bool = False,
) -> Dict[str, Any]:
    cpu = task.get("cpu") or {}
    memory = task.get("memory") or {}
    gpu = task.get("gpu") or {}
    user = task.get("user") or {}
    project = task.get("project") or {}
    nodes = task.get("nodes_occupied") or {}
    node_names = nodes.get("nodes") or []
    created_at = str(task.get("created_at", "") or "")
    running_time_ms = _as_int(task.get("running_time_ms"))

    return {
        "id": str(task.get("id", "") or ""),
        "name": str(task.get("name", "") or ""),
        "status": str(task.get("status", "") or ""),
        "type": str(task.get("type", "") or ""),
        "priority": _as_int(task.get("priority")),
        "created_at": created_at,
        "created_at_epoch": _parse_created_at(created_at),
        "running_time_ms": running_time_ms,
        "running_duration": format_duration(str(running_time_ms)),
        "is_mine": is_mine,
        "workspace_id": workspace_id,
        "workspace_name": workspace_name or workspace_id,
        "project_id": str(project.get("id", "") or ""),
        "project_name": str(project.get("name", "") or ""),
        "user_id": str(user.get("id", "") or ""),
        "user_name": str(user.get("name", "") or ""),
        "node_types": ", ".join(task.get("node_types") or []),
        "node_count": _as_int(nodes.get("count")),
        "node_names": ", ".join(node_names),
        "cpu_total": _as_float(cpu.get("total")),
        "cpu_used": _as_float(cpu.get("used")),
        "cpu_usage_rate_pct": round(_as_float(cpu.get("usage_rate")) * 100, 2),
        "memory_total": _as_float(memory.get("total")),
        "memory_used": _as_float(memory.get("used")),
        "memory_usage_rate_pct": round(_as_float(memory.get("usage_rate")) * 100, 2),
        "gpu_total": _as_float(gpu.get("total")),
        "gpu_used": _as_float(gpu.get("used")),
        "gpu_usage_rate_pct": round(_as_float(gpu.get("usage_rate")) * 100, 2),
    }


def _workspace_snapshot_options(
    workspace_id: str,
    rows: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    ws_resources = get_workspace_resources(workspace_id) or {}
    projects = sorted(
        (
            {"id": project_id, "name": project.get("name", project_id)}
            for project_id, project in (ws_resources.get("projects") or {}).items()
        ),
        key=lambda item: (item["name"], item["id"]),
    )
    if not projects:
        project_pairs = {
            (row.get("project_id", ""), row.get("project_name", ""))
            for row in rows
            if row.get("project_id") or row.get("project_name")
        }
        projects = sorted(
            (
                {"id": project_id, "name": project_name or project_id}
                for project_id, project_name in project_pairs
                if project_id or project_name
            ),
            key=lambda item: (item["name"], item["id"]),
        )
    groups = sorted(
        (
            {"id": group_id, "name": group.get("name", group_id)}
            for group_id, group in (ws_resources.get("compute_groups") or {}).items()
        ),
        key=lambda item: (item["name"], item["id"]),
    )
    return projects, groups


def _fetch_my_job_ids(api, workspace_id: str, cookie: str) -> set[str]:
    job_ids: set[str] = set()
    page_num = 1
    while True:
        data = api.list_jobs_with_cookie(
            workspace_id,
            cookie,
            page_num=page_num,
            page_size=200,
        )
        jobs = data.get("jobs", [])
        if not jobs:
            break
        for job in jobs:
            job_id = str(job.get("job_id", "") or "")
            if job_id:
                job_ids.add(job_id)
        total = data.get("total")
        if total is not None and len(job_ids) >= int(total):
            break
        if len(jobs) < 200:
            break
        page_num += 1
    return job_ids


def _build_snapshot(
    data: Dict[str, Any],
    *,
    default_group_by: str = "",
    workspace_options: List[Dict[str, Any]],
    selected_workspace_id: str,
    selected_project: str,
    selected_group: str,
) -> Dict[str, Any]:
    rows = data["rows"]
    project_options, group_options = _workspace_snapshot_options(
        selected_workspace_id, rows
    )
    return {
        "endpoint": "/api/v1/cluster_metric/list_task_dimension",
        "workspace_id": data["workspace_id"],
        "workspace_name": data["workspace_name"],
        "project_display": data["project_display"],
        "group_display": data.get("group_display", ""),
        "my_task_count": data.get("my_task_count", 0),
        "generated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
        "row_count": len(rows),
        "default_group_by": default_group_by,
        "workspace_options": [
            {"id": item["id"], "name": item["name"]} for item in workspace_options
        ],
        "project_options": project_options,
        "group_options": group_options,
        "selected_workspace_id": selected_workspace_id,
        "selected_project": selected_project,
        "selected_group": selected_group,
        "columns": TASK_COLUMNS,
        "rows": rows,
    }


def _print_task_rows(
    rows: List[Dict[str, Any]], title: str, *, blame_mode: bool = False
) -> None:
    display = get_display()
    if not rows:
        display.print("[dim]暂无任务[/dim]")
        return

    total_gpu = sum(row["gpu_total"] for row in rows)
    total_cpu = sum(row["cpu_total"] for row in rows)
    total_mem = sum(row["memory_total"] for row in rows)
    users = {row["user_name"] for row in rows if row["user_name"]}
    projects = {row["project_name"] for row in rows if row["project_name"]}
    display.print(
        f"[bold]{title}[/bold] [dim](任务 {len(rows)} | 用户 {len(users)} | 项目 {len(projects)} | "
        f"GPU {total_gpu:.0f} | CPU {total_cpu:.1f} | MEM {total_mem:.1f})[/dim]"
    )
    display.print("")

    if _RICH_AVAILABLE and getattr(display, "console", None):
        table = Table(
            box=box.SIMPLE, show_header=True, header_style="bold", expand=False
        )
        table.add_column("状态")
        table.add_column("任务名", style="white")
        table.add_column("用户", style="cyan")
        table.add_column("项目", style="magenta")
        table.add_column("类型")
        table.add_column("优先级", justify="right")
        table.add_column("节点", justify="right")
        table.add_column("GPU", justify="right")
        table.add_column("CPU%", justify="right")
        table.add_column("MEM%", justify="right")
        table.add_column("运行时长", justify="right")
        table.add_column("创建时间")
        for row in rows:
            table.add_row(
                row["status"],
                truncate_string(row["name"], 42),
                row["user_name"],
                truncate_string(row["project_name"], 24),
                row["type"],
                str(row["priority"]),
                str(row["node_count"]),
                f"{row['gpu_total']:.0f}",
                f"{row['cpu_usage_rate_pct']:.1f}",
                f"{row['memory_usage_rate_pct']:.1f}",
                row["running_duration"],
                row["created_at"][:19],
            )
        display.console.print(table)
    else:
        print(title)
        print("-" * 140)
        print(
            f"{'状态':<12} {'任务名':<42} {'用户':<16} {'项目':<24} {'类型':<22} "
            f"{'P':>2} {'节点':>4} {'GPU':>4} {'CPU%':>6} {'MEM%':>6} {'运行时长':>10}"
        )
        print("-" * 140)
        for row in rows:
            print(
                f"{truncate_string(row['status'], 12):<12} "
                f"{truncate_string(row['name'], 42):<42} "
                f"{truncate_string(row['user_name'], 16):<16} "
                f"{truncate_string(row['project_name'], 24):<24} "
                f"{truncate_string(row['type'], 22):<22} "
                f"{row['priority']:>2} {row['node_count']:>4} {row['gpu_total']:>4.0f} "
                f"{row['cpu_usage_rate_pct']:>6.1f} {row['memory_usage_rate_pct']:>6.1f} "
                f"{truncate_string(row['running_duration'], 10):>10}"
            )

    if blame_mode:
        _print_blame_summary(rows)


def _print_blame_summary(rows: List[Dict[str, Any]]) -> None:
    display = get_display()
    buckets: Dict[str, Dict[str, float]] = defaultdict(
        lambda: {"tasks": 0, "gpu": 0.0, "cpu": 0.0, "mem": 0.0}
    )
    for row in rows:
        key = row["user_name"] or "(unknown)"
        item = buckets[key]
        item["tasks"] += 1
        item["gpu"] += row["gpu_total"]
        item["cpu"] += row["cpu_total"]
        item["mem"] += row["memory_total"]

    display.print("")
    display.print("[bold]Blame Summary[/bold]")
    ranked = sorted(
        buckets.items(),
        key=lambda item: (-item[1]["tasks"], -item[1]["gpu"], item[0]),
    )[:20]
    for user_name, info in ranked:
        display.print(
            f"  {user_name}: {int(info['tasks'])} 任务 | GPU {info['gpu']:.0f} | "
            f"CPU {info['cpu']:.1f} | MEM {info['mem']:.1f}"
        )


def _make_handler(fetch_snapshot, initial_snapshot: Dict[str, Any]):
    state = {
        "snapshot": initial_snapshot,
        "cache": {
            ("", "", ""): initial_snapshot,
            (
                initial_snapshot.get("selected_workspace_id", ""),
                initial_snapshot.get("selected_project", ""),
                initial_snapshot.get("selected_group", ""),
            ): initial_snapshot,
        },
    }

    class _Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            query = parse_qs(parsed.query)
            workspace_id = query.get("workspace_id", [""])[0]
            project = query.get("project", [""])[0]
            group = query.get("group", [""])[0]
            cache_key = (workspace_id, project, group)

            if parsed.path == "/":
                body = HTML_PAGE.encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            if parsed.path == "/api/tasks":
                try:
                    snapshot = state["cache"].get(cache_key)
                    if snapshot is None:
                        snapshot = fetch_snapshot(
                            workspace_id=workspace_id, project=project, group=group
                        )
                        state["cache"][cache_key] = snapshot
                    payload = json.dumps(snapshot, ensure_ascii=False).encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                except Exception as exc:  # pragma: no cover - served path
                    payload = json.dumps(
                        {"error": str(exc)}, ensure_ascii=False
                    ).encode("utf-8")
                    self.send_response(500)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return

            if parsed.path == "/api/refresh":
                try:
                    snapshot = fetch_snapshot(
                        workspace_id=workspace_id, project=project, group=group
                    )
                    state["cache"][cache_key] = snapshot
                    state["snapshot"] = snapshot
                    payload = json.dumps(snapshot, ensure_ascii=False).encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                except Exception as exc:  # pragma: no cover - served path
                    payload = json.dumps(
                        {"error": str(exc)}, ensure_ascii=False
                    ).encode("utf-8")
                    self.send_response(500)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return

            self.send_response(404)
            self.end_headers()

        def do_POST(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path != "/api/stop":
                self.send_response(404)
                self.end_headers()
                return

            try:
                content_length = int(self.headers.get("Content-Length", "0"))
                payload = json.loads(self.rfile.read(content_length) or b"{}")
                workspace_id = str(payload.get("workspace_id", "") or "")
                project = str(payload.get("project", "") or "")
                group = str(payload.get("group", "") or "")
                requested_job_ids = [
                    str(job_id) for job_id in payload.get("job_ids", []) if str(job_id)
                ]
                snapshot = fetch_snapshot(
                    workspace_id=workspace_id, project=project, group=group
                )
                stoppable_ids = {
                    row["id"] for row in snapshot.get("rows", []) if row.get("is_mine")
                }
                api = get_api()
                results = []
                for job_id in requested_job_ids:
                    if job_id not in stoppable_ids:
                        results.append(
                            {"job_id": job_id, "stopped": False, "error": "not_owned"}
                        )
                        continue
                    result = api.stop_job_result(job_id)
                    results.append(result)
                body = json.dumps({"results": results}, ensure_ascii=False).encode(
                    "utf-8"
                )
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except Exception as exc:  # pragma: no cover - served path
                body = json.dumps({"error": str(exc)}, ensure_ascii=False).encode(
                    "utf-8"
                )
                self.send_response(500)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
            return

    return _Handler


def _serve_dashboard(
    fetch_snapshot, initial_snapshot: Dict[str, Any], host: str, port: int
) -> int:
    display = get_display()
    server = ThreadingHTTPServer(
        (host, port), _make_handler(fetch_snapshot, initial_snapshot)
    )
    actual_port = server.server_address[1]
    url = f"http://{host}:{actual_port}/"

    display.print_success(f"前端已启动: {url}")
    display.print("[dim]页面刷新时会重新拉取最新任务数据；按 Ctrl+C 停止服务。[/dim]")

    opened = webbrowser.open(url)
    if not opened:
        display.print(f"[dim]未能自动打开浏览器，请手动访问: {url}[/dim]")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        display.print("\n[dim]前端服务已停止[/dim]")
    finally:
        server.server_close()
    return 0


def cmd_task_dimensions(args) -> int:
    display = get_display()
    default_group_by = "user_name" if getattr(args, "command", "") == "blame" else ""

    try:
        (
            workspace_id,
            workspace_name,
            project_id,
            project_display,
            project_name_filter,
            workspace_options,
        ) = _resolve_workspace_and_project(
            args,
            allow_auto_workspace=args.serve,
        )
        default_group_id, default_group_display = _default_group_in_workspace(
            workspace_id
        )
        data = _fetch_task_dimensions(
            workspace_id,
            workspace_name,
            project_id=project_id,
            project_display=project_display,
            project_name_filter=project_name_filter,
            group_id=default_group_id,
            group_display=default_group_display,
            page_size=args.page_size,
        )
    except (QzAPIError, ResourceResolutionError) as exc:
        display.print_error(str(exc))
        return 1

    title = f"Task Dimensions · {workspace_name or workspace_id}"
    if project_display or project_name_filter:
        title += f" · {project_display or project_name_filter}"

    if not args.serve:
        rows = data["rows"]
        rows.sort(key=lambda item: (-item["created_at_epoch"], item["name"]))
        _print_task_rows(
            rows, title, blame_mode=getattr(args, "command", "") == "blame"
        )
        return 0

    def fetch_snapshot(
        *, workspace_id: str = "", project: str = "", group: str = ""
    ) -> Dict[str, Any]:
        selected_workspace_id, selected_workspace_name = _pick_workspace(
            workspace_id,
            "",
            workspace_options,
        )
        selected_project_id, selected_project_display, selected_project_name_filter = (
            _resolve_project_in_workspace(
                selected_workspace_id,
                project,
            )
        )
        if group:
            selected_group_id, selected_group_display = _resolve_group_in_workspace(
                selected_workspace_id,
                group,
            )
        else:
            selected_group_id, selected_group_display = _default_group_in_workspace(
                selected_workspace_id
            )
        fresh = _fetch_task_dimensions(
            selected_workspace_id,
            selected_workspace_name,
            project_id=selected_project_id,
            project_display=selected_project_display,
            project_name_filter=selected_project_name_filter,
            group_id=selected_group_id,
            group_display=selected_group_display,
            page_size=args.page_size,
        )
        return _build_snapshot(
            fresh,
            default_group_by=default_group_by,
            workspace_options=workspace_options,
            selected_workspace_id=selected_workspace_id,
            selected_project=project,
            selected_group=selected_group_id,
        )

    snapshot = _build_snapshot(
        data,
        default_group_by=default_group_by,
        workspace_options=workspace_options,
        selected_workspace_id=workspace_id,
        selected_project=project_id,
        selected_group=default_group_id,
    )
    display.print(
        f"[dim]{title}: 已加载 {snapshot['row_count']} 条记录，准备启动前端...[/dim]"
    )
    return _serve_dashboard(fetch_snapshot, snapshot, args.host, args.port)
