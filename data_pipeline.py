# -*- coding: utf-8 -*-
"""Unified data cleaning pipeline for VoiceInput historical data.

Outputs privacy-safe aggregated CSV files for open-source analysis.
"""
from __future__ import annotations

import csv
import json
import os
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from statistics import median
from typing import Dict, Iterable, List, Optional, Set, Tuple

DATA_SOURCES = [
    {
        "label": "backup",
        "jsonl_dir": r"D:\project\homehelper\voiceinput\records_20260324-22\records\jsonl",
        "audio_dir": r"D:\project\homehelper\voiceinput\records_20260324-22\records\audio",
    },
    {
        "label": "main",
        "jsonl_dir": r"D:\project\homehelper\voiceinput\records\jsonl",
        "audio_dir": r"D:\project\homehelper\voiceinput\records\audio",
    },
    {
        "label": "workspace",
        "jsonl_dir": r"D:\project\voiceinput-0407\records\jsonl",
        "audio_dir": r"D:\project\voiceinput-0407\records\audio",
    },
]

OUTPUT_DIR = r"D:\project\voiceinput-0407\analysis\data"
MAX_VOICE_TEXT_LEN = 500
TEST_PATTERNS = [
    re.compile(r"^hello[\s,]*hello", re.IGNORECASE),
    re.compile(r"^[Hh]ello[\.,!]*$"),
    re.compile(r"^喂+"),
    re.compile(r"你在吗"),
    re.compile(r"听[到得]我说话"),
    re.compile(r"测试"),
    re.compile(r"^嗯+[\.。]*$"),
    re.compile(r"^啊+[\.。]*$"),
    re.compile(r"^哦+[\.。]*$"),
    re.compile(r"^hey\s*jarvis", re.IGNORECASE),
    re.compile(r"^helello", re.IGNORECASE),
]

APP_CATEGORY_MAP = {
    "Code.exe": "IDE/Development",
    "python.exe": "IDE/Development",
    "chrome.exe": "Browser/Research",
    "msedge.exe": "Browser/Research",
    "Obsidian.exe": "Note-taking",
    "Weixin.exe": "Communication",
    "WXWork.exe": "Communication",
    "Feishu.exe": "Communication",
    "notepad.exe": "Text Editor",
    "explorer.exe": "System",
    "unknown": "Unknown/Early Version",
}


@dataclass
class VoiceLifecycle:
    filename: str
    date: str
    month: str
    source_label: str
    has_asr_event: bool = False
    asr_text_len: int = 0
    duration: float = 0.0
    asr_latency_ms: float = 0.0
    rtf: float = 0.0
    app_name: str = "unknown"
    window_title: str = ""
    language: str = "unknown"
    is_test_pattern: bool = False
    has_jsonl_record: bool = False
    fate: str = "unknown"
    fate_reason: str = ""
    committed_app_name: str = ""
    committed_category: str = ""
    is_cross_app: bool = False
    draft_session_id: str = ""


def ensure_output_dir() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def normalize_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def safe_float(value: object) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def detect_language(text: str) -> str:
    if not text:
        return "empty"
    chinese = len(re.findall(r"[\u4e00-\u9fff]", text))
    english = len(re.findall(r"[a-zA-Z]", text))
    if chinese == 0 and english == 0:
        return "punct_only"
    if chinese > english:
        return "chinese"
    if english > chinese:
        return "english"
    return "mixed"


def matches_test_pattern(text: str) -> bool:
    return any(pattern.search(text) for pattern in TEST_PATTERNS)


def app_category(app_name: str) -> str:
    return APP_CATEGORY_MAP.get(app_name or "unknown", "Other")


def list_jsonl_files() -> List[Tuple[str, str, str]]:
    files: List[Tuple[str, str, str]] = []
    for source in DATA_SOURCES:
        jsonl_dir = source["jsonl_dir"]
        for name in sorted(os.listdir(jsonl_dir)):
            if not name.endswith(".jsonl") or name.endswith("_debug.jsonl"):
                continue
            files.append((source["label"], jsonl_dir, name))
    return files


def extract_audio_date(filename: str, root: str) -> str:
    if len(filename) >= 8 and filename[:8].isdigit():
        return filename[:8]
    if filename.startswith("rec_"):
        normalized_root = root.replace("\\", "/")
        parts = normalized_root.split("/")
        if len(parts) >= 3 and all(part.isdigit() for part in parts[-3:]):
            year, month, day = parts[-3], parts[-2], parts[-1]
            if len(year) == 4 and len(month) == 2 and len(day) == 2:
                return f"{year}{month}{day}"
    return ""


def collect_audio_files() -> Set[str]:
    audio_files: Set[str] = set()
    for source in DATA_SOURCES:
        for root, _, files in os.walk(source["audio_dir"]):
            for name in files:
                if name.endswith(".opus"):
                    audio_files.add(name)
    return audio_files


def collect_audio_dates() -> Dict[str, str]:
    audio_dates: Dict[str, str] = {}
    for source in DATA_SOURCES:
        for root, _, files in os.walk(source["audio_dir"]):
            for name in files:
                if not name.endswith(".opus"):
                    continue
                date_str = extract_audio_date(name, root)
                if date_str:
                    audio_dates[name] = date_str
    return audio_dates


def iter_jsonl_records() -> Iterable[Tuple[str, str, Dict[str, object]]]:
    for source_label, jsonl_dir, name in list_jsonl_files():
        date_str = name.replace(".jsonl", "")
        file_path = os.path.join(jsonl_dir, name)
        with open(file_path, "r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                yield source_label, date_str, record


def get_or_create_lifecycle(
    lifecycles: Dict[str, VoiceLifecycle],
    filename: str,
    date_str: str,
    source_label: str,
    record: Dict[str, object],
) -> Optional[VoiceLifecycle]:
    filename = normalize_text(filename)
    if not filename:
        return None
    lifecycle = lifecycles.get(filename)
    if lifecycle is None:
        lifecycle = VoiceLifecycle(
            filename=filename,
            date=date_str,
            month=date_str[:6],
            source_label=source_label,
        )
        lifecycles[filename] = lifecycle
    if not lifecycle.app_name:
        lifecycle.app_name = normalize_text(record.get("app_name")) or "unknown"
    if not lifecycle.window_title:
        lifecycle.window_title = normalize_text(record.get("window_title"))
    if not lifecycle.draft_session_id:
        lifecycle.draft_session_id = normalize_text(record.get("draft_session_id"))
    if lifecycle.asr_text_len == 0:
        lifecycle.asr_text_len = len(normalize_text(record.get("text") or record.get("clean_text") or record.get("raw_text")))
    if lifecycle.language == "unknown":
        lifecycle.language = detect_language(normalize_text(record.get("text") or record.get("clean_text") or record.get("raw_text")))
    return lifecycle


def build_lifecycles() -> Tuple[Dict[str, VoiceLifecycle], Set[str]]:
    lifecycles: Dict[str, VoiceLifecycle] = {}
    clipboard_sessions: Set[str] = set()

    for source_label, date_str, record in iter_jsonl_records():
        event_type = normalize_text(record.get("event_type"))
        month = date_str[:6]
        draft_session_id = normalize_text(record.get("draft_session_id"))

        if event_type == "clipboard_received" and draft_session_id:
            clipboard_sessions.add(draft_session_id)
            continue

        if event_type != "asr_received":
            continue

        filename = normalize_text(record.get("filename"))
        if not filename:
            continue
        text = normalize_text(record.get("text") or record.get("clean_text") or record.get("raw_text"))
        lifecycle = lifecycles.get(filename)
        if lifecycle is None:
            lifecycle = VoiceLifecycle(
                filename=filename,
                date=date_str,
                month=month,
                source_label=source_label,
            )
            lifecycles[filename] = lifecycle
        lifecycle.has_jsonl_record = True
        lifecycle.has_asr_event = True
        lifecycle.asr_text_len = len(text)
        lifecycle.duration = safe_float(record.get("duration"))
        lifecycle.asr_latency_ms = safe_float(record.get("asr_time_cost")) * 1000
        lifecycle.rtf = safe_float(record.get("rtf") or record.get("total_rtf") or record.get("decode_rtf"))
        lifecycle.app_name = normalize_text(record.get("app_name")) or "unknown"
        lifecycle.window_title = normalize_text(record.get("window_title"))
        lifecycle.language = detect_language(text)
        lifecycle.is_test_pattern = matches_test_pattern(text)
        lifecycle.draft_session_id = draft_session_id

    for source_label, date_str, record in iter_jsonl_records():
        event_type = normalize_text(record.get("event_type"))
        draft_session_id = normalize_text(record.get("draft_session_id"))

        if event_type == "clipboard_received" and draft_session_id:
            clipboard_sessions.add(draft_session_id)
            continue

        if event_type == "asr_received":
            continue

        if event_type == "sys_filtered":
            filename = normalize_text(record.get("filename"))
            lifecycle = get_or_create_lifecycle(lifecycles, filename, date_str, source_label, record)
            if lifecycle:
                lifecycle.fate = "filtered"
                lifecycle.fate_reason = normalize_text(record.get("filter_reason")) or "unknown"
            continue

        if event_type == "ui_overridden":
            filename = normalize_text(record.get("filename"))
            lifecycle = get_or_create_lifecycle(lifecycles, filename, date_str, source_label, record)
            if lifecycle:
                lifecycle.fate = "speech_overwrite"
                lifecycle.fate_reason = "ui_overridden"
            continue

        if event_type in ("ui_mark_error", "ui_marked_error"):
            filenames = record.get("filenames") or []
            if not isinstance(filenames, list) or not filenames:
                filenames = [normalize_text(record.get("filename"))]
            for filename in filenames:
                lifecycle = get_or_create_lifecycle(lifecycles, filename, date_str, source_label, record)
                if lifecycle:
                    lifecycle.fate = "mark_error"
                    lifecycle.fate_reason = normalize_text(record.get("reason")) or "unknown"
            continue

        if event_type == "ui_discarded":
            filenames = record.get("filenames") or []
            if not isinstance(filenames, list) or not filenames:
                filenames = [normalize_text(record.get("filename"))]
            reason = normalize_text(record.get("reason")) or "unknown"
            for filename in filenames:
                lifecycle = get_or_create_lifecycle(lifecycles, filename, date_str, source_label, record)
                if not lifecycle:
                    continue
                if reason == "speech_overwrite":
                    lifecycle.fate = "speech_overwrite"
                elif "markerror" in reason.lower() or reason == "right_edge_mark_error":
                    lifecycle.fate = "mark_error"
                else:
                    lifecycle.fate = "discarded"
                lifecycle.fate_reason = reason
            continue

        if event_type == "ui_committed":
            filenames = record.get("filenames") or []
            if not isinstance(filenames, list) or not filenames:
                filenames = [normalize_text(record.get("filename"))]
            committed_app = normalize_text(record.get("app_name")) or "unknown"
            for filename in filenames:
                lifecycle = get_or_create_lifecycle(lifecycles, filename, date_str, source_label, record)
                if not lifecycle:
                    continue
                lifecycle.fate = "committed"
                lifecycle.fate_reason = normalize_text(record.get("reason")) or "unknown"
                lifecycle.committed_app_name = committed_app
                lifecycle.committed_category = app_category(committed_app)
                lifecycle.is_cross_app = lifecycle.app_name not in ("", "unknown") and committed_app not in ("", "unknown") and lifecycle.app_name != committed_app
            continue

    return lifecycles, clipboard_sessions


def write_csv(path: str, rows: List[Dict[str, object]], fieldnames: List[str]) -> None:
    with open(path, "w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def aggregate_daily_funnel(lifecycles: Dict[str, VoiceLifecycle]) -> List[Dict[str, object]]:
    by_day: Dict[str, Counter] = defaultdict(Counter)
    for lifecycle in lifecycles.values():
        by_day[lifecycle.date]["tracked_voices"] += 1
        if lifecycle.has_asr_event:
            by_day[lifecycle.date]["asr_received"] += 1
        else:
            by_day[lifecycle.date]["action_only"] += 1
        by_day[lifecycle.date][lifecycle.fate or "unknown"] += 1
        if lifecycle.is_test_pattern:
            by_day[lifecycle.date]["test_pattern"] += 1
        if lifecycle.is_cross_app:
            by_day[lifecycle.date]["cross_app"] += 1
        if lifecycle.duration:
            by_day[lifecycle.date]["audio_seconds"] += lifecycle.duration
        by_day[lifecycle.date]["text_chars"] += lifecycle.asr_text_len

    rows: List[Dict[str, object]] = []
    for date_str in sorted(by_day):
        counter = by_day[date_str]
        asr_total = counter["asr_received"]
        rows.append(
            {
                "date": date_str,
                "month": date_str[:6],
                "tracked_voices": counter["tracked_voices"],
                "asr_received": asr_total,
                "action_only": counter["action_only"],
                "committed": counter["committed"],
                "speech_overwrite": counter["speech_overwrite"],
                "discarded": counter["discarded"],
                "filtered": counter["filtered"],
                "mark_error": counter["mark_error"],
                "unknown": counter["unknown"],
                "test_pattern": counter["test_pattern"],
                "cross_app": counter["cross_app"],
                "audio_hours": round(counter["audio_seconds"] / 3600, 3),
                "mean_text_len": round(counter["text_chars"] / asr_total, 2) if asr_total else 0,
            }
        )
    return rows


def aggregate_monthly_summary(lifecycles: Dict[str, VoiceLifecycle], audio_dates: Dict[str, str]) -> List[Dict[str, object]]:
    months = sorted({lifecycle.month for lifecycle in lifecycles.values()})
    rows: List[Dict[str, object]] = []
    audio_by_month: Counter = Counter()
    for date_str in audio_dates.values():
        if len(date_str) == 8 and date_str.isdigit():
            audio_by_month[date_str[:6]] += 1

    for month in months:
        items = [item for item in lifecycles.values() if item.month == month]
        tracked_count = len(items)
        asr_count = sum(1 for item in items if item.has_asr_event)
        action_only_count = tracked_count - asr_count
        fate_counter = Counter(item.fate or "unknown" for item in items)
        test_count = sum(1 for item in items if item.is_test_pattern)
        cross_app_count = sum(1 for item in items if item.is_cross_app)
        durations = [item.duration for item in items if item.duration > 0]
        text_lens = [item.asr_text_len for item in items]
        latency = [item.asr_latency_ms for item in items if item.asr_latency_ms > 0]
        rtfs = [item.rtf for item in items if item.rtf > 0]
        rows.append(
            {
                "month": month,
                "audio_files": audio_by_month[month],
                "tracked_voices": tracked_count,
                "asr_received": asr_count,
                "action_only": action_only_count,
                "committed": fate_counter["committed"],
                "speech_overwrite": fate_counter["speech_overwrite"],
                "discarded": fate_counter["discarded"],
                "filtered": fate_counter["filtered"],
                "mark_error": fate_counter["mark_error"],
                "unknown": fate_counter["unknown"],
                "test_pattern": test_count,
                "cross_app": cross_app_count,
                "audio_hours": round(sum(durations) / 3600, 2),
                "mean_duration_sec": round(sum(durations) / len(durations), 2) if durations else 0,
                "median_duration_sec": round(median(durations), 2) if durations else 0,
                "mean_text_len": round(sum(text_lens) / len(text_lens), 2) if text_lens else 0,
                "median_text_len": round(median(text_lens), 2) if text_lens else 0,
                "mean_asr_latency_ms": round(sum(latency) / len(latency), 1) if latency else 0,
                "mean_rtf": round(sum(rtfs) / len(rtfs), 3) if rtfs else 0,
            }
        )
    return rows


def aggregate_app_summary(lifecycles: Dict[str, VoiceLifecycle]) -> List[Dict[str, object]]:
    committed_items = [item for item in lifecycles.values() if item.fate == "committed"]
    app_counter: Counter = Counter(item.committed_app_name or "unknown" for item in committed_items)
    rows: List[Dict[str, object]] = []
    total = len(committed_items)
    for app_name, count in app_counter.most_common():
        items = [item for item in committed_items if (item.committed_app_name or "unknown") == app_name]
        rows.append(
            {
                "app_name": app_name,
                "app_category": app_category(app_name),
                "committed_count": count,
                "share_pct": round(count / total * 100, 2) if total else 0,
                "mean_text_len": round(sum(item.asr_text_len for item in items) / len(items), 2) if items else 0,
                "cross_app_count": sum(1 for item in items if item.is_cross_app),
                "test_pattern_count": sum(1 for item in items if item.is_test_pattern),
            }
        )
    return rows


def aggregate_hourly_distribution(lifecycles: Dict[str, VoiceLifecycle]) -> List[Dict[str, object]]:
    hourly = Counter()
    for source_label, date_str, record in iter_jsonl_records():
        if normalize_text(record.get("event_type")) != "asr_received":
            continue
        timestamp = normalize_text(record.get("event_timestamp"))
        if "T" not in timestamp:
            continue
        try:
            hour = int(timestamp.split("T", 1)[1].split(":", 1)[0])
        except ValueError:
            continue
        hourly[hour] += 1
    return [{"hour": hour, "asr_received": hourly[hour]} for hour in range(24)]


def aggregate_language_fate(lifecycles: Dict[str, VoiceLifecycle]) -> List[Dict[str, object]]:
    counter: Counter = Counter((item.language, item.fate or "unknown") for item in lifecycles.values())
    rows: List[Dict[str, object]] = []
    for (language, fate), count in sorted(counter.items()):
        rows.append({"language": language, "fate": fate, "count": count})
    return rows


def write_summary_json(lifecycles: Dict[str, VoiceLifecycle], clipboard_sessions: Set[str], audio_files: Set[str]) -> None:
    matched_audio = sum(1 for item in lifecycles.values() if item.filename in audio_files)
    asr_event_count = sum(1 for item in lifecycles.values() if item.has_asr_event)
    summary = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "total_lifecycles": len(lifecycles),
        "asr_event_lifecycles": asr_event_count,
        "action_only_lifecycles": len(lifecycles) - asr_event_count,
        "audio_files": len(audio_files),
        "matched_audio": matched_audio,
        "audio_only": len(audio_files) - matched_audio,
        "jsonl_only": sum(1 for item in lifecycles.values() if item.filename not in audio_files),
        "clipboard_sessions": len(clipboard_sessions),
        "voice_text_len_threshold": MAX_VOICE_TEXT_LEN,
        "test_pattern_regex_count": len(TEST_PATTERNS),
    }
    with open(os.path.join(OUTPUT_DIR, "pipeline_summary.json"), "w", encoding="utf-8") as handle:
        json.dump(summary, handle, ensure_ascii=False, indent=2)


def main() -> None:
    ensure_output_dir()
    lifecycles, clipboard_sessions = build_lifecycles()
    audio_files = collect_audio_files()
    audio_dates = collect_audio_dates()

    # Exclude obvious clipboard-paste commits from app-level statistics by fate reason only when no ASR lineage exists.
    filtered_lifecycles = {
        filename: lifecycle
        for filename, lifecycle in lifecycles.items()
        if lifecycle.asr_text_len <= MAX_VOICE_TEXT_LEN or lifecycle.fate != "committed"
    }

    write_csv(
        os.path.join(OUTPUT_DIR, "daily_funnel.csv"),
        aggregate_daily_funnel(filtered_lifecycles),
        [
            "date",
            "month",
            "tracked_voices",
            "asr_received",
            "action_only",
            "committed",
            "speech_overwrite",
            "discarded",
            "filtered",
            "mark_error",
            "unknown",
            "test_pattern",
            "cross_app",
            "audio_hours",
            "mean_text_len",
        ],
    )

    write_csv(
        os.path.join(OUTPUT_DIR, "monthly_summary.csv"),
        aggregate_monthly_summary(filtered_lifecycles, audio_dates),
        [
            "month",
            "audio_files",
            "tracked_voices",
            "asr_received",
            "action_only",
            "committed",
            "speech_overwrite",
            "discarded",
            "filtered",
            "mark_error",
            "unknown",
            "test_pattern",
            "cross_app",
            "audio_hours",
            "mean_duration_sec",
            "median_duration_sec",
            "mean_text_len",
            "median_text_len",
            "mean_asr_latency_ms",
            "mean_rtf",
        ],
    )

    write_csv(
        os.path.join(OUTPUT_DIR, "committed_apps.csv"),
        aggregate_app_summary(filtered_lifecycles),
        [
            "app_name",
            "app_category",
            "committed_count",
            "share_pct",
            "mean_text_len",
            "cross_app_count",
            "test_pattern_count",
        ],
    )

    write_csv(
        os.path.join(OUTPUT_DIR, "hourly_distribution.csv"),
        aggregate_hourly_distribution(filtered_lifecycles),
        ["hour", "asr_received"],
    )

    write_csv(
        os.path.join(OUTPUT_DIR, "language_fate.csv"),
        aggregate_language_fate(filtered_lifecycles),
        ["language", "fate", "count"],
    )

    write_summary_json(filtered_lifecycles, clipboard_sessions, audio_files)
    print(f"Wrote cleaned outputs to {OUTPUT_DIR}")
    print(f"Total tracked lifecycles: {len(filtered_lifecycles)}")


if __name__ == "__main__":
    main()
