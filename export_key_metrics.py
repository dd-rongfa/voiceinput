# -*- coding: utf-8 -*-
"""Export rounded key metrics from cleaned analysis outputs."""
from __future__ import annotations

import csv
import json
import os
from typing import Dict, List

BASE_DIR = r"D:\project\voiceinput-0407\analysis"
DATA_DIR = os.path.join(BASE_DIR, "data")
OUTPUT_PATH = os.path.join(DATA_DIR, "key_metrics.json")


def read_csv(path: str) -> List[Dict[str, str]]:
    with open(path, "r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def round_to_hundred(value: int) -> int:
    return int(round(value / 100.0) * 100)


def plus_label(value: int) -> str:
    rounded = round_to_hundred(value)
    return f"{rounded:,}+"


def main() -> None:
    monthly_rows = read_csv(os.path.join(DATA_DIR, "monthly_summary.csv"))
    q1_rows = [row for row in monthly_rows if row["month"] in ("202601", "202602", "202603")]
    march_row = next(row for row in monthly_rows if row["month"] == "202603")

    q1_asr = sum(int(row["asr_received"]) for row in q1_rows)
    q1_tracked = sum(int(row["tracked_voices"]) for row in q1_rows)
    q1_committed = sum(int(row["committed"]) for row in q1_rows)
    q1_audio_files = sum(int(row["audio_files"]) for row in q1_rows)
    q1_audio_hours = round(sum(float(row["audio_hours"]) for row in q1_rows), 1)

    march_tracked = int(march_row["tracked_voices"])
    march_asr = int(march_row["asr_received"])
    march_committed = int(march_row["committed"])
    march_overwrite = int(march_row["speech_overwrite"])
    march_filtered = int(march_row["filtered"])
    march_mark_error = int(march_row["mark_error"])
    march_cross_app = int(march_row["cross_app"])

    # Load behavior analysis summary
    behavior_path = os.path.join(DATA_DIR, "behavior_summary.json")
    with open(behavior_path, "r", encoding="utf-8") as handle:
        behavior = json.load(handle)
    march_behavior = behavior["march"]

    latency_rows = read_csv(os.path.join(DATA_DIR, "latency_summary.csv"))
    march_latency = next((row for row in latency_rows if row["month"] == "202603"), None)

    metrics = {
        "q1": {
            "tracked_voices": q1_tracked,
            "asr_received": q1_asr,
            "committed": q1_committed,
            "audio_files": q1_audio_files,
            "audio_hours": q1_audio_hours,
            "resume_labels": {
                "asr_received": plus_label(q1_asr),
                "committed": plus_label(q1_committed),
                "audio_files": plus_label(q1_audio_files),
            },
        },
        "march": {
            "tracked_voices": march_tracked,
            "asr_received": march_asr,
            "committed": march_committed,
            "speech_overwrite": march_overwrite,
            "filtered": march_filtered,
            "mark_error": march_mark_error,
            "cross_app": march_cross_app,
            "commit_rate_pct": round(march_committed / march_tracked * 100, 1) if march_tracked else 0,
            "speech_overwrite_pct": round(march_overwrite / march_tracked * 100, 1) if march_tracked else 0,
            "filtered_pct": round(march_filtered / march_tracked * 100, 1) if march_tracked else 0,
            "resume_labels": {
                "tracked_voices": plus_label(march_tracked),
                "committed": plus_label(march_committed),
            },
        },
        "march_behavior": {
            "activity_sessions": march_behavior["activity_sessions"]["count"],
            "active_input_sessions": march_behavior["activity_sessions"]["active_input"],
            "retry_heavy_sessions": march_behavior["activity_sessions"]["retry_heavy_input"],
            "passive_capture_sessions": march_behavior["activity_sessions"]["passive_capture_candidate"],
            "passive_capture_utterances": march_behavior["activity_sessions"]["passive_capture_utterances"],
            "explicit_retry_chains": march_behavior["explicit_retry_chains"]["count"],
            "max_retry_chain_len": march_behavior["explicit_retry_chains"]["max_chain_len"],
            "similarity_retry_clusters": march_behavior["similarity_retry_clusters"]["count"],
            "exact_repeat_failures": march_behavior["similarity_retry_clusters"]["exact_repeat_failures"],
            "latency_p50_ms": float(march_latency["p50_latency_ms"]) if march_latency else 0,
            "latency_p95_ms": float(march_latency["p95_latency_ms"]) if march_latency else 0,
            "latency_p99_ms": float(march_latency["p99_latency_ms"]) if march_latency else 0,
        },
        "recommended_resume_sentence": (
            f"基于 2026 年第一季度 {plus_label(q1_asr)} 次真实语音识别与 {plus_label(q1_committed)} 次成功注入日志，"
            f"构建以单条语音为原子单位的全链路评测方法，覆盖语音重说链检测、"
            f"输入会话状态识别、ASR 延迟分位数分析（P50={float(march_latency['p50_latency_ms']) if march_latency else 0:.0f}ms）"
            f"等维度，量化从语音采集到文本注入的端到端质量。"
        ),
    }

    with open(OUTPUT_PATH, "w", encoding="utf-8") as handle:
        json.dump(metrics, handle, ensure_ascii=False, indent=2)

    print(f"Wrote {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
