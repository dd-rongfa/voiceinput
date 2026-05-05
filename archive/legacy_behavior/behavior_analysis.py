# -*- coding: utf-8 -*-
"""Behavior-focused analysis for March 2026 VoiceInput usage.

Generates local-only diagnostic outputs for retry chains, activity sessions,
and latency percentiles on top of the existing lifecycle pipeline.
"""
from __future__ import annotations

import csv
import json
import os
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from difflib import SequenceMatcher
from statistics import median
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from data_pipeline import OUTPUT_DIR, iter_jsonl_records, matches_test_pattern, normalize_text, safe_float

MARCH_MONTH = "202603"
RETRY_GAP_SECONDS = 15
SESSION_GAP_SECONDS = 90  # 90s matches interactive rhythm better than 180s
PASSIVE_MIN_UTTERANCES = 8
PASSIVE_MAX_COMMIT_DENSITY = 0.10
REPEAT_SIMILARITY_THRESHOLD = 0.72
SHORT_REPEAT_TEXT_LEN = 6
SESSION_GAP_OPTIONS = [60, 90, 120, 180, 240, 300]  # sensitivity test
# Inferred overwrite threshold: validated from 717 real speech_overwrite events
# where P90=4.9s, 93.6% within 6s, matching the 5s draft_timeout_seconds config.
INFERRED_OVERWRITE_GAP_SECONDS = 6

# Punctuation characters to strip for content-length calculation.
# ASR models often emit trailing punctuation that inflates text length.
PUNCTUATION_CHARS = set('.,?。，？!！ ；;：:、…—""''\'\'()（）【】[]{}《》<>~`@#$%^&*_+-=/\\|·\t\n\r')


def strip_punctuation(text: str) -> str:
    """Remove punctuation chars for content-length calculation."""
    return ''.join(ch for ch in text if ch not in PUNCTUATION_CHARS)


@dataclass
class Utterance:
    filename: str
    timestamp: datetime
    date: str
    month: str
    draft_session_id: str
    text: str
    app_name: str
    window_title: str
    duration: float = 0.0
    latency_ms: float = 0.0
    rtf: float = 0.0
    fate: str = "unknown"
    fate_reason: str = ""
    is_test_pattern: bool = False
    replacement_filename: str = ""


@dataclass
class ActivitySession:
    session_id: str
    start_at: datetime
    end_at: datetime
    utterances: List[Utterance] = field(default_factory=list)


def parse_timestamp(value: str) -> Optional[datetime]:
    value = normalize_text(value)
    if not value:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def percentile(values: Sequence[float], pct: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return float(values[0])
    ordered = sorted(values)
    position = (len(ordered) - 1) * pct
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    if lower == upper:
        return float(ordered[lower])
    weight = position - lower
    return float(ordered[lower] * (1 - weight) + ordered[upper] * weight)


def normalize_compare_text(text: str) -> str:
    text = normalize_text(text).lower()
    text = re.sub(r"[\s\W_]+", "", text, flags=re.UNICODE)
    return text


def text_similarity(left: str, right: str) -> float:
    left_norm = normalize_compare_text(left)
    right_norm = normalize_compare_text(right)
    if not left_norm or not right_norm:
        return 0.0
    if left_norm == right_norm:
        return 1.0
    return SequenceMatcher(None, left_norm, right_norm).ratio()


def is_numeric_template_variation(left: str, right: str) -> bool:
    left_norm = normalize_compare_text(left)
    right_norm = normalize_compare_text(right)
    if not left_norm or not right_norm or left_norm == right_norm:
        return False
    numeric_pattern = r"[0-9零一二两三四五六七八九十百千万第]+"
    left_masked = re.sub(numeric_pattern, "#", left_norm)
    right_masked = re.sub(numeric_pattern, "#", right_norm)
    return left_masked == right_masked and "#" in left_masked


def is_repeat_pair(previous: Utterance, current: Utterance) -> Tuple[bool, float]:
    if previous.app_name != current.app_name:
        return False, 0.0
    gap_seconds = (current.timestamp - previous.timestamp).total_seconds()
    if gap_seconds < 0 or gap_seconds > RETRY_GAP_SECONDS:
        return False, 0.0
    if previous.fate == "committed":
        return False, 0.0

    previous_norm = normalize_compare_text(previous.text)
    current_norm = normalize_compare_text(current.text)
    if not previous_norm or not current_norm:
        return False, 0.0

    similarity = text_similarity(previous.text, current.text)
    shorter = min(len(previous_norm), len(current_norm))
    longer = max(len(previous_norm), len(current_norm))
    length_ratio = shorter / longer if longer else 0.0
    exact_short_repeat = previous_norm == current_norm and longer <= SHORT_REPEAT_TEXT_LEN
    same_length_short = len(previous_norm) == len(current_norm) and longer <= SHORT_REPEAT_TEXT_LEN and similarity >= 0.6

    is_repeat = similarity >= REPEAT_SIMILARITY_THRESHOLD or exact_short_repeat or same_length_short
    if length_ratio < 0.75 and not exact_short_repeat:
        is_repeat = False
    if is_numeric_template_variation(previous.text, current.text) and previous_norm != current_norm:
        is_repeat = False
    return is_repeat, similarity


def apply_pending_updates(utterance: Utterance, pending_updates: Dict[str, List[Tuple[str, str, str]]]) -> None:
    updates = pending_updates.pop(utterance.filename, [])
    for fate, reason, replacement_filename in updates:
        utterance.fate = fate
        utterance.fate_reason = reason
        if replacement_filename:
            utterance.replacement_filename = replacement_filename


def queue_or_apply_update(
    utterances: Dict[str, Utterance],
    pending_updates: Dict[str, List[Tuple[str, str, str]]],
    filename: str,
    fate: str,
    reason: str,
    replacement_filename: str = "",
) -> None:
    filename = normalize_text(filename)
    if not filename:
        return
    utterance = utterances.get(filename)
    if utterance is None:
        pending_updates[filename].append((fate, reason, replacement_filename))
        return
    utterance.fate = fate
    utterance.fate_reason = reason
    if replacement_filename:
        utterance.replacement_filename = replacement_filename


def build_utterances() -> Tuple[Dict[str, Utterance], Counter]:
    utterances: Dict[str, Utterance] = {}
    pending_updates: Dict[str, List[Tuple[str, str, str]]] = defaultdict(list)
    event_counter: Counter = Counter()
    _filtered_meta: Dict[str, Tuple[str, dict]] = {}  # filename -> (date_str, record)

    for _, date_str, record in iter_jsonl_records():
        event_type = normalize_text(record.get("event_type"))
        event_counter[event_type] += 1

        if event_type == "clipboard_received":
            continue

        if event_type == "asr_received":
            filename = normalize_text(record.get("filename"))
            timestamp = parse_timestamp(normalize_text(record.get("event_timestamp")) or normalize_text(record.get("source_created_at")))
            if not filename or timestamp is None:
                continue
            text = normalize_text(record.get("text") or record.get("clean_text") or record.get("raw_text"))
            utterance = Utterance(
                filename=filename,
                timestamp=timestamp,
                date=date_str,
                month=date_str[:6],
                draft_session_id=normalize_text(record.get("draft_session_id")),
                text=text,
                app_name=normalize_text(record.get("app_name")) or "unknown",
                window_title=normalize_text(record.get("window_title")),
                duration=safe_float(record.get("duration")),
                latency_ms=safe_float(record.get("asr_time_cost")) * 1000,
                rtf=safe_float(record.get("rtf") or record.get("total_rtf") or record.get("decode_rtf")),
                is_test_pattern=matches_test_pattern(text),
            )
            utterances[filename] = utterance
            apply_pending_updates(utterance, pending_updates)
            continue

        if event_type == "sys_filtered":
            queue_or_apply_update(
                utterances,
                pending_updates,
                normalize_text(record.get("filename")),
                "filtered",
                normalize_text(record.get("filter_reason")) or "unknown",
            )
            # Track metadata for stub creation if no asr_received arrives later
            filename = normalize_text(record.get("filename"))
            if filename and filename not in _filtered_meta:
                _filtered_meta[filename] = (date_str, record)
            continue

        if event_type in ("ui_mark_error", "ui_marked_error"):
            filenames = record.get("filenames") or [normalize_text(record.get("filename"))]
            if not isinstance(filenames, list):
                filenames = [normalize_text(record.get("filename"))]
            for filename in filenames:
                queue_or_apply_update(
                    utterances,
                    pending_updates,
                    normalize_text(filename),
                    "mark_error",
                    normalize_text(record.get("reason")) or "unknown",
                )
            continue

        if event_type == "ui_overridden":
            queue_or_apply_update(
                utterances,
                pending_updates,
                normalize_text(record.get("filename")),
                "speech_overwrite",
                "ui_overridden",
            )
            continue

        if event_type == "ui_discarded":
            filenames = record.get("filenames") or [normalize_text(record.get("filename"))]
            if not isinstance(filenames, list):
                filenames = [normalize_text(record.get("filename"))]
            reason = normalize_text(record.get("reason")) or "unknown"
            replacement_filename = normalize_text(record.get("replacement_filename"))
            for filename in filenames:
                fate = "discarded"
                if reason == "speech_overwrite":
                    fate = "speech_overwrite"
                elif "markerror" in reason.lower() or reason == "right_edge_mark_error":
                    fate = "mark_error"
                queue_or_apply_update(
                    utterances,
                    pending_updates,
                    normalize_text(filename),
                    fate,
                    reason,
                    replacement_filename if fate == "speech_overwrite" else "",
                )
            continue

        if event_type == "ui_committed":
            filenames = record.get("filenames") or [normalize_text(record.get("filename"))]
            if not isinstance(filenames, list):
                filenames = [normalize_text(record.get("filename"))]
            for filename in filenames:
                queue_or_apply_update(
                    utterances,
                    pending_updates,
                    normalize_text(filename),
                    "committed",
                    normalize_text(record.get("reason")) or "unknown",
                )

    # Create stub utterances for sys_filtered items that never got an asr_received event.
    # This happens with newer code versions that filter before logging asr_received.
    for fn, (date_str, record) in _filtered_meta.items():
        if fn not in utterances:
            timestamp = parse_timestamp(
                normalize_text(record.get("event_timestamp"))
                or normalize_text(record.get("source_created_at"))
            )
            stub = Utterance(
                filename=fn,
                timestamp=timestamp or datetime.min,
                date=date_str,
                month=date_str[:6],
                draft_session_id=normalize_text(record.get("draft_session_id")),
                text=normalize_text(record.get("text") or record.get("raw_text") or ""),
                app_name=normalize_text(record.get("app_name")) or "unknown",
                window_title=normalize_text(record.get("window_title")),
                fate="filtered",
                fate_reason=normalize_text(record.get("filter_reason")) or "unknown",
            )
            utterances[fn] = stub

    return utterances, event_counter


def build_explicit_retry_chains(utterances: Dict[str, Utterance]) -> List[Dict[str, object]]:
    next_map = {
        utterance.filename: utterance.replacement_filename
        for utterance in utterances.values()
        if utterance.replacement_filename and utterance.replacement_filename in utterances
    }
    previous_nodes = set(next_map.values())
    roots = sorted(filename for filename in next_map if filename not in previous_nodes)

    chains: List[Dict[str, object]] = []
    for root in roots:
        ordered: List[Utterance] = []
        current = root
        seen: set[str] = set()
        while current in next_map and current not in seen:
            seen.add(current)
            ordered.append(utterances[current])
            current = next_map[current]
        if current in utterances and current not in seen:
            ordered.append(utterances[current])
        if len(ordered) < 2:
            continue

        similarities = [
            round(text_similarity(left.text, right.text), 3)
            for left, right in zip(ordered, ordered[1:])
        ]
        chains.append(
            {
                "chain_root": root,
                "month": ordered[0].month,
                "date": ordered[0].date,
                "app_name": ordered[0].app_name,
                "draft_session_id": ordered[0].draft_session_id,
                "utterance_count": len(ordered),
                "start_time": ordered[0].timestamp.isoformat(timespec="milliseconds"),
                "end_time": ordered[-1].timestamp.isoformat(timespec="milliseconds"),
                "span_sec": round((ordered[-1].timestamp - ordered[0].timestamp).total_seconds(), 3),
                "final_fate": ordered[-1].fate,
                "all_uncommitted": int(all(item.fate != "committed" for item in ordered)),
                "same_phrase_count": sum(1 for score in similarities if score >= 0.9),
                "median_similarity": round(median(similarities), 3) if similarities else 0,
                "texts_preview": " | ".join(item.text[:30] for item in ordered[:5]),
            }
        )
    return chains


def build_similarity_retry_clusters(utterances: Dict[str, Utterance]) -> List[Dict[str, object]]:
    march_items = sorted(
        (item for item in utterances.values() if item.month == MARCH_MONTH and not item.is_test_pattern),
        key=lambda item: item.timestamp,
    )
    clusters: List[Dict[str, object]] = []
    current_cluster: List[Utterance] = []
    similarity_scores: List[float] = []

    def flush_cluster() -> None:
        nonlocal current_cluster, similarity_scores
        if len(current_cluster) < 2:
            current_cluster = []
            similarity_scores = []
            return
        normalized_texts = [normalize_compare_text(item.text) for item in current_cluster]
        exact_repeats = len(set(normalized_texts)) == 1
        final_fate = current_cluster[-1].fate
        clusters.append(
            {
                "cluster_start": current_cluster[0].timestamp.isoformat(timespec="milliseconds"),
                "cluster_end": current_cluster[-1].timestamp.isoformat(timespec="milliseconds"),
                "date": current_cluster[0].date,
                "app_name": current_cluster[0].app_name,
                "utterance_count": len(current_cluster),
                "final_fate": final_fate,
                "all_uncommitted": int(all(item.fate != "committed" for item in current_cluster)),
                "exact_repeat_chain": int(exact_repeats),
                "median_similarity": round(median(similarity_scores), 3) if similarity_scores else 0,
                "mean_text_len": round(sum(len(item.text) for item in current_cluster) / len(current_cluster), 2),
                "texts_preview": " | ".join(item.text[:30] for item in current_cluster[:5]),
            }
        )
        current_cluster = []
        similarity_scores = []

    for item in march_items:
        if not current_cluster:
            current_cluster = [item]
            continue
        is_repeat, similarity = is_repeat_pair(current_cluster[-1], item)
        if is_repeat:
            current_cluster.append(item)
            similarity_scores.append(similarity)
            continue
        flush_cluster()
        current_cluster = [item]

    flush_cluster()
    return clusters


def build_activity_sessions(utterances: Dict[str, Utterance]) -> List[Dict[str, object]]:
    march_items = sorted(
        (item for item in utterances.values() if item.month == MARCH_MONTH and not item.is_test_pattern),
        key=lambda item: item.timestamp,
    )
    sessions: List[ActivitySession] = []
    current: Optional[ActivitySession] = None

    for item in march_items:
        if current is None:
            current = ActivitySession(session_id=f"session_{len(sessions) + 1:04d}", start_at=item.timestamp, end_at=item.timestamp, utterances=[item])
            continue
        gap_seconds = (item.timestamp - current.end_at).total_seconds()
        if gap_seconds > SESSION_GAP_SECONDS:
            sessions.append(current)
            current = ActivitySession(session_id=f"session_{len(sessions) + 1:04d}", start_at=item.timestamp, end_at=item.timestamp, utterances=[item])
            continue
        current.utterances.append(item)
        current.end_at = item.timestamp

    if current is not None:
        sessions.append(current)

    rows: List[Dict[str, object]] = []
    for session in sessions:
        items = session.utterances
        fate_counter = Counter(item.fate for item in items)
        repeat_pairs = 0
        for left, right in zip(items, items[1:]):
            if is_repeat_pair(left, right)[0]:
                repeat_pairs += 1
        asr_count = len(items)
        committed = fate_counter["committed"]
        commit_density = committed / asr_count if asr_count else 0.0
        repeat_density = repeat_pairs / max(asr_count - 1, 1)
        dominant_app = Counter(item.app_name for item in items).most_common(1)[0][0]

        classification = "mixed"
        if asr_count >= PASSIVE_MIN_UTTERANCES and commit_density <= PASSIVE_MAX_COMMIT_DENSITY:
            classification = "passive_capture_candidate"
        elif fate_counter["speech_overwrite"] >= 2 or repeat_density >= 0.3:
            classification = "retry_heavy_input"
        elif committed >= 2 and commit_density >= 0.3:
            classification = "active_input"

        rows.append(
            {
                "session_id": session.session_id,
                "date": items[0].date,
                "start_time": session.start_at.isoformat(timespec="seconds"),
                "end_time": session.end_at.isoformat(timespec="seconds"),
                "span_sec": round((session.end_at - session.start_at).total_seconds(), 3),
                "asr_count": asr_count,
                "committed": committed,
                "speech_overwrite": fate_counter["speech_overwrite"],
                "discarded": fate_counter["discarded"],
                "filtered": fate_counter["filtered"],
                "mark_error": fate_counter["mark_error"],
                "unknown": fate_counter["unknown"],
                "commit_density": round(commit_density, 3),
                "repeat_density": round(repeat_density, 3),
                "dominant_app": dominant_app,
                "mean_text_len": round(sum(len(item.text) for item in items) / asr_count, 2),
                "classification": classification,
                "preview": " | ".join(item.text[:20] for item in items[:4]),
            }
        )
    return rows


def build_latency_summary(utterances: Dict[str, Utterance]) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    months = sorted({item.month for item in utterances.values()})
    for month in months:
        values = [item.latency_ms for item in utterances.values() if item.month == month and item.latency_ms > 0]
        rows.append(
            {
                "month": month,
                "sample_count": len(values),
                "mean_latency_ms": round(sum(values) / len(values), 1) if values else 0,
                "p50_latency_ms": round(percentile(values, 0.50), 1) if values else 0,
                "p95_latency_ms": round(percentile(values, 0.95), 1) if values else 0,
                "p99_latency_ms": round(percentile(values, 0.99), 1) if values else 0,
            }
        )
    return rows


def text_length_bin(text_len: int) -> str:
    if text_len <= 4:
        return "01_1to4"
    if text_len <= 8:
        return "02_5to8"
    if text_len <= 16:
        return "03_9to16"
    if text_len <= 32:
        return "04_17to32"
    return "05_33plus"


def build_text_length_fate(utterances: Dict[str, Utterance]) -> List[Dict[str, object]]:
    """Use content length (punctuation stripped) for bucketing."""
    bucket_map: Dict[str, Counter] = defaultdict(Counter)
    for item in utterances.values():
        if item.month != MARCH_MONTH or item.is_test_pattern:
            continue
        content_len = len(strip_punctuation(item.text))
        bucket = text_length_bin(content_len)
        bucket_map[bucket]["asr_count"] += 1
        bucket_map[bucket][item.fate] += 1

    rows: List[Dict[str, object]] = []
    for bucket in sorted(bucket_map):
        counter = bucket_map[bucket]
        total = counter["asr_count"]
        rows.append(
            {
                "text_len_bin": bucket,
                "asr_count": total,
                "committed": counter["committed"],
                "speech_overwrite": counter["speech_overwrite"],
                "discarded": counter["discarded"],
                "filtered": counter["filtered"],
                "mark_error": counter["mark_error"],
                "unknown": counter["unknown"],
                "commit_rate": round(counter["committed"] / total, 3) if total else 0,
            }
        )
    return rows


def build_latency_by_duration(utterances: Dict[str, Utterance]) -> List[Dict[str, object]]:
    """Break down latency by audio duration buckets for March.

    Reveals that latency scales with audio length, and long recordings
    (especially ambient 30s-capped audio) drive up the tail.
    """
    march_items = [
        item for item in utterances.values()
        if item.month == MARCH_MONTH and item.latency_ms > 0 and item.duration > 0
    ]
    buckets = [
        ("0-1s", 0, 1), ("1-2s", 1, 2), ("2-3s", 2, 3), ("3-5s", 3, 5),
        ("5-10s", 5, 10), ("10-20s", 10, 20), ("20+s", 20, 9999),
    ]
    rows: List[Dict[str, object]] = []
    for name, lo, hi in buckets:
        items = [item for item in march_items if lo <= item.duration < hi]
        if not items:
            continue
        lats = sorted(item.latency_ms for item in items)
        n = len(lats)
        rows.append({
            "duration_bucket": name,
            "count": n,
            "avg_latency_ms": round(sum(lats) / n, 0),
            "p50_latency_ms": round(percentile(lats, 0.50), 0),
            "p95_latency_ms": round(percentile(lats, 0.95), 0),
            "p99_latency_ms": round(percentile(lats, 0.99), 0),
            "avg_content_len": round(sum(len(strip_punctuation(item.text)) for item in items) / n, 1),
        })
    return rows


def build_filter_gap_analysis(utterances: Dict[str, Utterance]) -> List[Dict[str, object]]:
    """Detect days where filtering was likely disabled (config lost).

    Counts ASR events and sys_filtered events per day to find gaps.
    """
    daily_asr: Counter = Counter()
    daily_filtered: Counter = Counter()
    daily_filter_reasons: Dict[str, Counter] = defaultdict(Counter)
    for item in utterances.values():
        daily_asr[item.date] += 1
    # Re-scan for filtered events separately (since filtered items are still in utterances)
    for item in utterances.values():
        if item.fate == "filtered":
            daily_filtered[item.date] += 1
            daily_filter_reasons[item.date][item.fate_reason] += 1

    rows: List[Dict[str, object]] = []
    for date in sorted(set(daily_asr) | set(daily_filtered)):
        asr = daily_asr[date]
        filt = daily_filtered[date]
        reasons = daily_filter_reasons.get(date, Counter())
        top_reason = reasons.most_common(1)[0][0] if reasons else ""
        rows.append({
            "date": date,
            "asr_count": asr,
            "filtered_count": filt,
            "filter_pct": round(filt / asr * 100, 1) if asr > 0 else 0,
            "has_gap": int(asr >= 10 and filt == 0),
            "top_filter_reason": top_reason,
        })
    return rows


def top_unresolved_repeat_examples(clusters: Iterable[Dict[str, object]]) -> List[Dict[str, object]]:
    unresolved = [item for item in clusters if item["all_uncommitted"] and item["utterance_count"] >= 2]
    unresolved.sort(
        key=lambda item: (
            int(item["exact_repeat_chain"]),
            int(item["utterance_count"]),
            float(item["median_similarity"]),
        ),
        reverse=True,
    )
    top_rows: List[Dict[str, object]] = []
    for item in unresolved[:20]:
        top_rows.append(
            {
                "date": item["date"],
                "app_name": item["app_name"],
                "utterance_count": item["utterance_count"],
                "final_fate": item["final_fate"],
                "exact_repeat_chain": item["exact_repeat_chain"],
                "median_similarity": item["median_similarity"],
                "texts_preview": item["texts_preview"],
            }
        )
    return top_rows


def aggregate_exact_repeat_failures(clusters: Iterable[Dict[str, object]]) -> List[Dict[str, object]]:
    grouped: Dict[Tuple[str, str], Dict[str, object]] = {}
    for item in clusters:
        if not item["all_uncommitted"] or not item["exact_repeat_chain"]:
            continue
        preview = str(item["texts_preview"]).split(" | ", 1)[0]
        key = (str(item["app_name"]), preview)
        bucket = grouped.get(key)
        if bucket is None:
            bucket = {
                "app_name": item["app_name"],
                "repeat_text": preview,
                "cluster_count": 0,
                "utterance_total": 0,
                "max_chain_len": 0,
                "sample_final_fate": item["final_fate"],
            }
            grouped[key] = bucket
        bucket["cluster_count"] += 1
        bucket["utterance_total"] += int(item["utterance_count"])
        bucket["max_chain_len"] = max(bucket["max_chain_len"], int(item["utterance_count"]))

    rows = list(grouped.values())
    rows.sort(key=lambda item: (int(item["utterance_total"]), int(item["cluster_count"])), reverse=True)
    return rows


def write_csv(path: str, rows: List[Dict[str, object]], fieldnames: List[str]) -> None:
    with open(path, "w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def build_unknown_fate_analysis(utterances: Dict[str, Utterance]) -> Dict[str, object]:
    """Analyze unknown-fate utterances to infer they were likely implicit overwrites.

    In early schema versions, speech_overwrite events were not logged.
    If an unknown-fate utterance is followed by another voice within 15s,
    it was very likely silently overwritten.
    """
    all_sorted = sorted(
        ((fn, u) for fn, u in utterances.items() if u.month in (MARCH_MONTH, "202601", "202602")),
        key=lambda x: x[1].timestamp,
    )
    fn_to_idx = {fn: i for i, (fn, _) in enumerate(all_sorted)}

    by_month: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))

    for fn, u in all_sorted:
        if u.fate != "unknown":
            continue
        idx = fn_to_idx.get(fn)
        if idx is None:
            continue
        month = u.month
        by_month[month]["total_unknown"] += 1

        if idx + 1 < len(all_sorted):
            _, next_u = all_sorted[idx + 1]
            gap = (next_u.timestamp - u.timestamp).total_seconds()
            if gap <= INFERRED_OVERWRITE_GAP_SECONDS:
                by_month[month]["inferred_overwrite_within_6s"] += 1
            elif gap <= 15:
                by_month[month]["gap_6_to_15s"] += 1
            else:
                by_month[month]["no_close_successor"] += 1
        else:
            by_month[month]["no_close_successor"] += 1
        # Track short-text unknowns separately
        if len(u.text) <= 2:
            by_month[month]["very_short_lte2"] += 1
        elif len(u.text) <= 4:
            by_month[month]["short_lte4"] += 1

    return dict(by_month)


def build_discard_breakdown(utterances: Dict[str, Utterance]) -> List[Dict[str, object]]:
    """Break down discards into active (gesture/speech_overwrite) vs passive (timeout) per month."""
    ACTIVE_REASONS = {
        "speech_overwrite", "speech_overwrite_nocursor", "speech_overwrite_not_ibeam",
        "top_edge_discard", "top_edge_discard_item", "top_right_discard_all",
        "gesture_cancel", "gesture_cancel_ccw", "gesture_cancel_topedge",
        "gesture_markerror_rightedge", "gesture_stash_bottomleft",
        "gesture_top_edge", "injection_dragged", "guesture",
        "replace_inactive_after_leftpress_timeout", "replace_inactive_after_middleclick",
    }
    TIMEOUT_REASONS = {"draft_timeout", "timeout", "timeout_expire", "timeout_not_ibeam"}

    by_month: Dict[str, Counter] = defaultdict(Counter)
    for u in utterances.values():
        if u.fate not in ("discarded", "speech_overwrite"):
            continue
        month = u.month
        reason = u.fate_reason
        if reason in ACTIVE_REASONS or u.fate == "speech_overwrite":
            by_month[month]["active_overwrite_or_gesture"] += 1
        elif reason in TIMEOUT_REASONS:
            by_month[month]["passive_timeout"] += 1
        else:
            by_month[month]["other_discard"] += 1
        by_month[month]["total"] += 1

    rows: List[Dict[str, object]] = []
    for month in sorted(by_month):
        c = by_month[month]
        rows.append({
            "month": month,
            "total_discards": c["total"],
            "active_overwrite_or_gesture": c["active_overwrite_or_gesture"],
            "passive_timeout": c["passive_timeout"],
            "other_discard": c["other_discard"],
        })
    return rows


def build_session_gap_sensitivity(utterances: Dict[str, Utterance]) -> List[Dict[str, object]]:
    """Test session count at multiple gap thresholds (March only)."""
    march_items = sorted(
        (item for item in utterances.values() if item.month == MARCH_MONTH and not item.is_test_pattern),
        key=lambda item: item.timestamp,
    )
    rows: List[Dict[str, object]] = []
    for gap in SESSION_GAP_OPTIONS:
        session_count = 1 if march_items else 0
        for i in range(1, len(march_items)):
            if (march_items[i].timestamp - march_items[i - 1].timestamp).total_seconds() > gap:
                session_count += 1
        avg_len = len(march_items) / session_count if session_count else 0
        rows.append({
            "gap_seconds": gap,
            "session_count": session_count,
            "avg_utterances_per_session": round(avg_len, 1),
        })
    return rows


def build_corrected_funnel(utterances: Dict[str, Utterance]) -> List[Dict[str, object]]:
    """Produce a corrected funnel that reclassifies unknown-fate as inferred_overwrite
    when followed by another voice within 6s, and remaining unknowns as inferred_discard
    (since the only possible outcomes are: committed, overwrite, discard, filtered, mark_error).
    Also tracks committed character counts.
    """
    all_sorted = sorted(
        ((fn, u) for fn, u in utterances.items()),
        key=lambda x: x[1].timestamp,
    )
    fn_to_idx = {fn: i for i, (fn, _) in enumerate(all_sorted)}

    by_month: Dict[str, Counter] = defaultdict(Counter)
    # Track committed characters per month
    committed_chars: Dict[str, int] = defaultdict(int)
    total_chars: Dict[str, int] = defaultdict(int)

    for fn, u in all_sorted:
        month = u.month
        by_month[month]["total"] += 1
        text_len = len(strip_punctuation(u.text))
        total_chars[month] += text_len
        if u.fate == "committed":
            committed_chars[month] += text_len
        if u.fate != "unknown":
            by_month[month][u.fate] += 1
        else:
            idx = fn_to_idx.get(fn)
            if idx is not None and idx + 1 < len(all_sorted):
                _, next_u = all_sorted[idx + 1]
                gap = (next_u.timestamp - u.timestamp).total_seconds()
                if gap <= INFERRED_OVERWRITE_GAP_SECONDS:
                    by_month[month]["inferred_overwrite"] += 1
                else:
                    by_month[month]["inferred_discard"] += 1
            else:
                by_month[month]["inferred_discard"] += 1

    rows: List[Dict[str, object]] = []
    for month in sorted(by_month):
        c = by_month[month]
        total = c["total"]
        total_ow = c["speech_overwrite"] + c["inferred_overwrite"]
        total_disc = c["discarded"] + c["inferred_discard"]
        rows.append({
            "month": month,
            "total": total,
            "committed": c["committed"],
            "speech_overwrite_explicit": c["speech_overwrite"],
            "inferred_overwrite": c["inferred_overwrite"],
            "discarded": c["discarded"],
            "inferred_discard": c["inferred_discard"],
            "filtered": c["filtered"],
            "mark_error": c["mark_error"],
            "total_overwrite_pct": round(total_ow / total * 100, 1) if total else 0,
            "total_discard_pct": round(total_disc / total * 100, 1) if total else 0,
            "committed_chars": committed_chars[month],
            "total_chars": total_chars[month],
        })
    return rows


def write_behavior_summary(
    utterances: Dict[str, Utterance],
    event_counter: Counter,
    retry_chains: List[Dict[str, object]],
    retry_clusters: List[Dict[str, object]],
    activity_sessions: List[Dict[str, object]],
    latency_rows: List[Dict[str, object]],
    text_length_rows: List[Dict[str, object]],
    unknown_analysis: Dict[str, object],
    discard_breakdown: List[Dict[str, object]],
    session_gap_sensitivity: List[Dict[str, object]],
    corrected_funnel: List[Dict[str, object]],
    latency_by_duration: List[Dict[str, object]],
    filter_gap_analysis: List[Dict[str, object]],
) -> None:
    march_items = [item for item in utterances.values() if item.month == MARCH_MONTH]
    march_clusters = [item for item in retry_clusters if item["date"].startswith(MARCH_MONTH)]
    explicit_march_chains = [item for item in retry_chains if item["month"] == MARCH_MONTH]
    passive_sessions = [item for item in activity_sessions if item["classification"] == "passive_capture_candidate"]
    retry_sessions = [item for item in activity_sessions if item["classification"] == "retry_heavy_input"]
    active_sessions = [item for item in activity_sessions if item["classification"] == "active_input"]
    march_latency = next((row for row in latency_rows if row["month"] == MARCH_MONTH), None)

    summary = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "scope": "March 2026 is the primary reliable period for behavioral analysis.",
        "event_counts": event_counter,
        "session_gap_seconds_used": SESSION_GAP_SECONDS,
        "session_gap_sensitivity": session_gap_sensitivity,
        "unknown_fate_analysis": unknown_analysis,
        "discard_breakdown": discard_breakdown,
        "corrected_funnel": corrected_funnel,
        "latency_by_duration": latency_by_duration,
        "filter_gap_analysis": {
            "total_gap_days": sum(1 for row in filter_gap_analysis if row["has_gap"]),
            "gap_dates": [row["date"] for row in filter_gap_analysis if row["has_gap"]],
            "note": "Filter config was gradually added. Mar 5-28 had zero filtering (17 gap days). "
                    "Before Mar 29, filter logged 'no_reason'. After Mar 29, proper reasons (blocklist_word, "
                    "pure_punctuation, too_short) appear.",
        },
        "march": {
            "utterances": len(march_items),
            "latency_ms": march_latency,
            "explicit_retry_chains": {
                "count": len(explicit_march_chains),
                "note": "replacement_filename only available from 2026-03-28 onwards, so chains cover ~7 days only.",
                "chains_ge_3": sum(1 for item in explicit_march_chains if int(item["utterance_count"]) >= 3),
                "all_uncommitted": sum(1 for item in explicit_march_chains if int(item["all_uncommitted"]) == 1),
                "max_chain_len": max((int(item["utterance_count"]) for item in explicit_march_chains), default=0),
            },
            "similarity_retry_clusters": {
                "count": len(march_clusters),
                "all_uncommitted": sum(1 for item in march_clusters if int(item["all_uncommitted"]) == 1),
                "exact_repeat_failures": sum(
                    1 for item in march_clusters if int(item["all_uncommitted"]) == 1 and int(item["exact_repeat_chain"]) == 1
                ),
                "clusters_ge_3": sum(1 for item in march_clusters if int(item["utterance_count"]) >= 3),
            },
            "activity_sessions": {
                "count": len(activity_sessions),
                "active_input": len(active_sessions),
                "retry_heavy_input": len(retry_sessions),
                "passive_capture_candidate": len(passive_sessions),
                "passive_capture_utterances": sum(int(item["asr_count"]) for item in passive_sessions),
            },
            "text_length_fate": text_length_rows,
            "top_unresolved_repeat_examples": top_unresolved_repeat_examples(march_clusters),
        },
        "notes": [
            "replacement_filename only exists from 2026-03-28 onwards — explicit retry chain counts only cover ~7 days.",
            "Inferred overwrite uses 6s threshold, validated against 717 real speech_overwrite events (P90=4.9s, matching 5s draft_timeout).",
            "Remaining unknown fate items (14.1% in March) may include: early-version filter drops without logging, program restarts, or timeout discards without events.",
            "Session gap defaults to 90s; sensitivity table included for 60-300s.",
            "ASR latency (asr_time_cost) measures the time from VAD end (audio dispatch) to ASR result returned. For local SenseVoice, queue_wait ≈ 0, so total_pipeline_time ≈ asr_time_cost. This does NOT include the audio duration itself.",
            "Latency scales linearly with audio duration: 1-2s audio→130ms, 10-20s→778ms, 20+s→1684ms. March P99 spike is driven by long ambient recordings hitting the 30s cap.",
            "Text length uses content length (punctuation stripped). ASR models emit trailing punctuation that inflates raw text_len.",
            "Filter config was gradually added: Mar 5-28 had ZERO filtering (config likely lost). 879 instances of pure '.' were not intercepted. Proper blocklist/punctuation/too_short filters only reliable from Mar 29.",
            "draft_session_id only becomes reliable in late March, so early-month retry chains depend more on timestamps and text similarity.",
            "passive_capture_candidate is a heuristic label: long activity span with very low commit density suggests ambient audio or monitoring rather than active dictation.",
            "similarity_retry_clusters are local diagnostics and should not be published as-is because they retain text previews.",
        ],
    }
    with open(os.path.join(OUTPUT_DIR, "behavior_summary.json"), "w", encoding="utf-8") as handle:
        json.dump(summary, handle, ensure_ascii=False, indent=2)


def main() -> None:
    utterances, event_counter = build_utterances()
    retry_chains = build_explicit_retry_chains(utterances)
    retry_clusters = build_similarity_retry_clusters(utterances)
    exact_repeat_failures = aggregate_exact_repeat_failures(retry_clusters)
    activity_sessions = build_activity_sessions(utterances)
    latency_rows = build_latency_summary(utterances)
    text_length_rows = build_text_length_fate(utterances)
    unknown_analysis = build_unknown_fate_analysis(utterances)
    discard_breakdown = build_discard_breakdown(utterances)
    session_gap_sensitivity = build_session_gap_sensitivity(utterances)
    corrected_funnel = build_corrected_funnel(utterances)
    latency_by_duration = build_latency_by_duration(utterances)
    filter_gap_analysis = build_filter_gap_analysis(utterances)

    write_csv(
        os.path.join(OUTPUT_DIR, "retry_chains.csv"),
        retry_chains,
        [
            "chain_root",
            "month",
            "date",
            "app_name",
            "draft_session_id",
            "utterance_count",
            "start_time",
            "end_time",
            "span_sec",
            "final_fate",
            "all_uncommitted",
            "same_phrase_count",
            "median_similarity",
            "texts_preview",
        ],
    )
    write_csv(
        os.path.join(OUTPUT_DIR, "retry_clusters.csv"),
        retry_clusters,
        [
            "cluster_start",
            "cluster_end",
            "date",
            "app_name",
            "utterance_count",
            "final_fate",
            "all_uncommitted",
            "exact_repeat_chain",
            "median_similarity",
            "mean_text_len",
            "texts_preview",
        ],
    )
    write_csv(
        os.path.join(OUTPUT_DIR, "activity_sessions.csv"),
        activity_sessions,
        [
            "session_id",
            "date",
            "start_time",
            "end_time",
            "span_sec",
            "asr_count",
            "committed",
            "speech_overwrite",
            "discarded",
            "filtered",
            "mark_error",
            "unknown",
            "commit_density",
            "repeat_density",
            "dominant_app",
            "mean_text_len",
            "classification",
            "preview",
        ],
    )
    write_csv(
        os.path.join(OUTPUT_DIR, "exact_repeat_failures.csv"),
        exact_repeat_failures,
        ["app_name", "repeat_text", "cluster_count", "utterance_total", "max_chain_len", "sample_final_fate"],
    )
    write_csv(
        os.path.join(OUTPUT_DIR, "latency_summary.csv"),
        latency_rows,
        ["month", "sample_count", "mean_latency_ms", "p50_latency_ms", "p95_latency_ms", "p99_latency_ms"],
    )
    write_csv(
        os.path.join(OUTPUT_DIR, "text_length_fate.csv"),
        text_length_rows,
        [
            "text_len_bin",
            "asr_count",
            "committed",
            "speech_overwrite",
            "discarded",
            "filtered",
            "mark_error",
            "unknown",
            "commit_rate",
        ],
    )
    write_csv(
        os.path.join(OUTPUT_DIR, "discard_breakdown.csv"),
        discard_breakdown,
        ["month", "total_discards", "active_overwrite_or_gesture", "passive_timeout", "other_discard"],
    )
    write_csv(
        os.path.join(OUTPUT_DIR, "session_gap_sensitivity.csv"),
        session_gap_sensitivity,
        ["gap_seconds", "session_count", "avg_utterances_per_session"],
    )
    write_csv(
        os.path.join(OUTPUT_DIR, "corrected_funnel.csv"),
        corrected_funnel,
        [
            "month", "total", "committed", "speech_overwrite_explicit",
            "inferred_overwrite", "discarded", "inferred_discard", "filtered", "mark_error",
            "total_overwrite_pct", "total_discard_pct", "committed_chars", "total_chars",
        ],
    )
    write_csv(
        os.path.join(OUTPUT_DIR, "latency_by_duration.csv"),
        latency_by_duration,
        ["duration_bucket", "count", "avg_latency_ms", "p50_latency_ms", "p95_latency_ms", "p99_latency_ms", "avg_content_len"],
    )
    write_csv(
        os.path.join(OUTPUT_DIR, "filter_gap_analysis.csv"),
        filter_gap_analysis,
        ["date", "asr_count", "filtered_count", "filter_pct", "has_gap", "top_filter_reason"],
    )
    write_behavior_summary(
        utterances,
        event_counter,
        retry_chains,
        retry_clusters,
        activity_sessions,
        latency_rows,
        text_length_rows,
        unknown_analysis,
        discard_breakdown,
        session_gap_sensitivity,
        corrected_funnel,
        latency_by_duration,
        filter_gap_analysis,
    )
    print(f"Wrote behavior analysis outputs to {OUTPUT_DIR}")


if __name__ == "__main__":
    main()