# -*- coding: utf-8 -*-
"""Render privacy-safe figures from cleaned VoiceInput analysis CSV files."""
from __future__ import annotations

import csv
import json
import os
from typing import Dict, List

import matplotlib.pyplot as plt

BASE_DIR = r"D:\project\voiceinput-0407\analysis"
DATA_DIR = os.path.join(BASE_DIR, "data")
FIG_DIR = os.path.join(BASE_DIR, "figures")

plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "Arial Unicode MS", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False


def read_csv(path: str) -> List[Dict[str, str]]:
    with open(path, "r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def ensure_dir() -> None:
    os.makedirs(FIG_DIR, exist_ok=True)


def save_monthly_trend(rows: List[Dict[str, str]]) -> None:
    months = [row["month"] for row in rows]
    committed = [int(row["committed"]) for row in rows]
    filtered = [int(row["filtered"]) for row in rows]
    overwrites = [int(row["speech_overwrite"]) for row in rows]

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(months, committed, marker="o", linewidth=2.5, label="Committed", color="#2ecc71")
    ax.plot(months, filtered, marker="o", linewidth=2, label="Filtered", color="#e67e22")
    ax.plot(months, overwrites, marker="o", linewidth=2, label="Speech Overwrite", color="#e74c3c")
    ax.set_title("VoiceInput Monthly Trend")
    ax.set_xlabel("Month")
    ax.set_ylabel("Count")
    ax.grid(alpha=0.25)
    ax.legend()
    fig.tight_layout()
    fig.savefig(os.path.join(FIG_DIR, "monthly_trend.png"), dpi=180)
    plt.close(fig)


def save_march_funnel(rows: List[Dict[str, str]]) -> None:
    march_row = next(row for row in rows if row["month"] == "202603")
    total = int(march_row["tracked_voices"])
    labels = ["Committed", "Speech Overwrite", "Discarded", "Filtered", "Mark Error", "Unknown"]
    values = [
        int(march_row["committed"]),
        int(march_row["speech_overwrite"]),
        int(march_row["discarded"]),
        int(march_row["filtered"]),
        int(march_row["mark_error"]),
        int(march_row["unknown"]),
    ]
    pct_labels = [f"{label}\n{value / total * 100:.1f}%" for label, value in zip(labels, values)]
    colors = ["#2E8B57", "#F4A261", "#8D99AE", "#457B9D", "#D62828", "#BDBDBD"]

    fig, ax = plt.subplots(figsize=(10, 5))
    bars = ax.bar(pct_labels, values, color=colors)
    ax.set_title("March 2026 Lifecycle Funnel")
    ax.set_ylabel("Count")
    ax.grid(axis="y", alpha=0.25)
    for bar, value in zip(bars, values):
        ax.text(bar.get_x() + bar.get_width() / 2, value + total * 0.01, str(value), ha="center", va="bottom", fontsize=10)
    fig.tight_layout()
    fig.savefig(os.path.join(FIG_DIR, "march_funnel.png"), dpi=180)
    plt.close(fig)


def save_app_distribution(rows: List[Dict[str, str]]) -> None:
    top_rows = rows[:6]
    labels = [row["app_name"] for row in top_rows]
    values = [int(row["committed_count"]) for row in top_rows]

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.barh(labels, values, color="#264653")
    ax.invert_yaxis()
    ax.set_title("Committed Input Distribution by App")
    ax.set_xlabel("Committed Count")
    for idx, value in enumerate(values):
        ax.text(value + max(values) * 0.01, idx, str(value), va="center")
    fig.tight_layout()
    fig.savefig(os.path.join(FIG_DIR, "app_distribution.png"), dpi=180)
    plt.close(fig)


def save_hourly_distribution(rows: List[Dict[str, str]]) -> None:
    hours = [int(row["hour"]) for row in rows]
    counts = [int(row["asr_received"]) for row in rows]

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar(hours, counts, color="#6D597A")
    ax.set_title("ASR Activity by Hour")
    ax.set_xlabel("Hour of Day")
    ax.set_ylabel("ASR Count")
    ax.set_xticks(hours)
    ax.grid(axis="y", alpha=0.25)
    fig.tight_layout()
    fig.savefig(os.path.join(FIG_DIR, "hourly_distribution.png"), dpi=180)
    plt.close(fig)


def save_data_quality(summary: Dict[str, int]) -> None:
    labels = ["Matched Audio", "Audio Only", "JSONL Only", "Action Only"]
    values = [
        int(summary["matched_audio"]),
        int(summary["audio_only"]),
        int(summary["jsonl_only"]),
        int(summary["action_only_lifecycles"]),
    ]
    colors = ["#2A9D8F", "#E9C46A", "#F4A261", "#E76F51"]

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.pie(values, labels=labels, autopct="%1.1f%%", startangle=90, colors=colors)
    ax.set_title("Data Coverage Overview")
    fig.tight_layout()
    fig.savefig(os.path.join(FIG_DIR, "data_quality.png"), dpi=180)
    plt.close(fig)


def save_session_classification(behavior: Dict) -> None:
    sessions = behavior["march"]["activity_sessions"]
    labels = ["Active Input", "Retry Heavy", "Passive Capture", "Mixed/Other"]
    active = sessions["active_input"]
    retry = sessions["retry_heavy_input"]
    passive = sessions["passive_capture_candidate"]
    mixed = sessions["count"] - active - retry - passive
    values = [active, retry, passive, mixed]
    colors = ["#2E8B57", "#F4A261", "#D62828", "#8D99AE"]

    fig, ax = plt.subplots(figsize=(8, 5))
    wedges, texts, autotexts = ax.pie(
        values, labels=labels, autopct=lambda pct: f"{pct:.1f}%\n({int(pct / 100 * sum(values))})",
        startangle=90, colors=colors, textprops={"fontsize": 10},
    )
    ax.set_title(f"March 2026 Session Classification (n={sessions['count']})")
    fig.tight_layout()
    fig.savefig(os.path.join(FIG_DIR, "session_classification.png"), dpi=180)
    plt.close(fig)


def save_latency_percentiles(rows: List[Dict[str, str]]) -> None:
    months = [row["month"] for row in rows]
    p50 = [float(row["p50_latency_ms"]) for row in rows]
    p95 = [float(row["p95_latency_ms"]) for row in rows]
    p99 = [float(row["p99_latency_ms"]) for row in rows]

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(months, p50, marker="o", linewidth=2.5, label="P50", color="#2A9D8F")
    ax.plot(months, p95, marker="s", linewidth=2.5, label="P95", color="#F4A261")
    ax.plot(months, p99, marker="^", linewidth=2, label="P99", color="#D62828")
    ax.axhline(y=300, color="#888888", linestyle="--", linewidth=1, label="300ms ref")
    ax.set_title("ASR Latency Percentiles by Month")
    ax.set_xlabel("Month")
    ax.set_ylabel("Latency (ms)")
    ax.legend()
    ax.grid(alpha=0.25)
    fig.tight_layout()
    fig.savefig(os.path.join(FIG_DIR, "latency_percentiles.png"), dpi=180)
    plt.close(fig)


def save_text_length_fate(rows: List[Dict[str, str]]) -> None:
    bins = [row["text_len_bin"].split("_", 1)[1] for row in rows]
    committed = [int(row["committed"]) for row in rows]
    overwrite = [int(row["speech_overwrite"]) for row in rows]
    discarded = [int(row["discarded"]) for row in rows]
    other = [int(row["asr_count"]) - int(row["committed"]) - int(row["speech_overwrite"]) - int(row["discarded"]) for row in rows]

    fig, ax = plt.subplots(figsize=(10, 5))
    bottom = [0] * len(bins)
    for values, label, color in [
        (committed, "Committed", "#2E8B57"),
        (overwrite, "Speech Overwrite", "#F4A261"),
        (discarded, "Discarded", "#8D99AE"),
        (other, "Other", "#BDBDBD"),
    ]:
        ax.bar(bins, values, bottom=bottom, label=label, color=color)
        bottom = [b + v for b, v in zip(bottom, values)]

    ax2 = ax.twinx()
    commit_rates = [float(row["commit_rate"]) * 100 for row in rows]
    ax2.plot(bins, commit_rates, marker="D", color="#D62828", linewidth=2, label="Commit Rate %")
    ax2.set_ylabel("Commit Rate (%)")
    ax2.set_ylim(0, 80)

    ax.set_title("March 2026: Content Length vs Fate (punctuation stripped)")
    ax.set_xlabel("Content Length (chars, no punctuation)")
    ax.set_ylabel("Count")
    ax.legend(loc="upper left")
    ax2.legend(loc="upper right")
    ax.grid(axis="y", alpha=0.25)
    fig.tight_layout()
    fig.savefig(os.path.join(FIG_DIR, "text_length_fate.png"), dpi=180)
    plt.close(fig)


def save_retry_chain_distribution(behavior: Dict) -> None:
    chains = behavior["march"]["explicit_retry_chains"]
    clusters = behavior["march"]["similarity_retry_clusters"]
    labels = ["Explicit Chains\n(speech_overwrite)", "Similarity Clusters\n(text match)"]
    total = [chains["count"], clusters["count"]]
    uncommitted = [chains["all_uncommitted"], clusters["all_uncommitted"]]
    committed_or_partial = [t - u for t, u in zip(total, uncommitted)]

    x = range(len(labels))
    width = 0.35
    fig, ax = plt.subplots(figsize=(8, 5))
    bars1 = ax.bar([i - width / 2 for i in x], committed_or_partial, width, label="Resolved (committed eventually)", color="#2A9D8F")
    bars2 = ax.bar([i + width / 2 for i in x], uncommitted, width, label="All Uncommitted", color="#D62828")
    ax.set_xticks(list(x))
    ax.set_xticklabels(labels)
    ax.set_title("March 2026: Retry Pattern Detection")
    ax.set_ylabel("Count")
    ax.legend()
    ax.grid(axis="y", alpha=0.25)
    for bar in bars1 + bars2:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width() / 2, height + 3, str(int(height)), ha="center", va="bottom", fontsize=10)
    fig.tight_layout()
    fig.savefig(os.path.join(FIG_DIR, "retry_patterns.png"), dpi=180)
    plt.close(fig)


def save_corrected_funnel(rows: List[Dict[str, str]]) -> None:
    """Stacked bar chart showing corrected funnel per month."""
    months = [row["month"] for row in rows]
    committed = [int(row["committed"]) for row in rows]
    explicit_ow = [int(row["speech_overwrite_explicit"]) for row in rows]
    inferred_ow = [int(row["inferred_overwrite"]) for row in rows]
    discarded = [int(row["discarded"]) for row in rows]
    inferred_disc = [int(row["inferred_discard"]) for row in rows]
    filtered = [int(row["filtered"]) for row in rows]
    mark_error = [int(row["mark_error"]) for row in rows]

    fig, ax = plt.subplots(figsize=(10, 6))
    bottom = [0] * len(months)
    layers = [
        (committed, "Committed", "#2E8B57"),
        (explicit_ow, "Explicit Overwrite", "#F4A261"),
        (inferred_ow, "Inferred Overwrite (6s rule)", "#E76F51"),
        (discarded, "Discarded (logged)", "#8D99AE"),
        (inferred_disc, "Inferred Discard", "#BFC0C0"),
        (filtered, "Filtered", "#457B9D"),
        (mark_error, "Mark Error", "#D62828"),
    ]
    for values, label, color in layers:
        ax.bar(months, values, bottom=bottom, label=label, color=color)
        bottom = [b + v for b, v in zip(bottom, values)]

    # Add overwrite % annotation on top
    for i, row in enumerate(rows):
        total = int(row["total"])
        ow_pct = float(row["total_overwrite_pct"])
        ax.text(i, bottom[i] + total * 0.02, f"OW {ow_pct:.0f}%", ha="center", fontsize=9, fontweight="bold")

    ax.set_title("Corrected Lifecycle Funnel (all fates sum to 100%)")
    ax.set_xlabel("Month")
    ax.set_ylabel("Count")
    ax.legend(loc="upper left", fontsize=8)
    ax.grid(axis="y", alpha=0.25)
    fig.tight_layout()
    fig.savefig(os.path.join(FIG_DIR, "corrected_funnel.png"), dpi=180)
    plt.close(fig)


def save_session_gap_sensitivity(rows: List[Dict[str, str]]) -> None:
    """Line chart showing session count at different gap thresholds."""
    gaps = [int(row["gap_seconds"]) for row in rows]
    counts = [int(row["session_count"]) for row in rows]
    avg_lens = [float(row["avg_utterances_per_session"]) for row in rows]

    fig, ax1 = plt.subplots(figsize=(8, 5))
    color1 = "#2A9D8F"
    ax1.plot(gaps, counts, marker="o", linewidth=2.5, color=color1, label="Session Count")
    ax1.set_xlabel("Gap Threshold (seconds)")
    ax1.set_ylabel("Session Count", color=color1)
    ax1.tick_params(axis="y", labelcolor=color1)
    # Mark the chosen gap
    chosen_idx = gaps.index(90) if 90 in gaps else 1
    ax1.axvline(x=90, color="#888", linestyle="--", linewidth=1, alpha=0.7)
    ax1.annotate("chosen: 90s", xy=(90, counts[chosen_idx]), fontsize=9,
                 xytext=(120, counts[chosen_idx] + 100), arrowprops=dict(arrowstyle="->", color="#888"))

    ax2 = ax1.twinx()
    color2 = "#F4A261"
    ax2.plot(gaps, avg_lens, marker="s", linewidth=2, color=color2, label="Avg Utterances/Session")
    ax2.set_ylabel("Avg Utterances per Session", color=color2)
    ax2.tick_params(axis="y", labelcolor=color2)

    ax1.set_title("March 2026: Session Gap Sensitivity")
    ax1.grid(alpha=0.25)
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="center right")
    fig.tight_layout()
    fig.savefig(os.path.join(FIG_DIR, "session_gap_sensitivity.png"), dpi=180)
    plt.close(fig)


def save_discard_breakdown(rows: List[Dict[str, str]]) -> None:
    """Stacked bar: active vs passive vs other discards per month."""
    months = [row["month"] for row in rows]
    active = [int(row["active_overwrite_or_gesture"]) for row in rows]
    timeout = [int(row["passive_timeout"]) for row in rows]
    other = [int(row["other_discard"]) for row in rows]

    fig, ax = plt.subplots(figsize=(8, 5))
    bottom = [0] * len(months)
    for values, label, color in [
        (active, "Active (gesture/overwrite)", "#2A9D8F"),
        (timeout, "Passive (timeout)", "#F4A261"),
        (other, "Other", "#BDBDBD"),
    ]:
        ax.bar(months, values, bottom=bottom, label=label, color=color)
        bottom = [b + v for b, v in zip(bottom, values)]
    ax.set_title("Discard Reason Breakdown by Month")
    ax.set_xlabel("Month")
    ax.set_ylabel("Count")
    ax.legend()
    ax.grid(axis="y", alpha=0.25)
    fig.tight_layout()
    fig.savefig(os.path.join(FIG_DIR, "discard_breakdown.png"), dpi=180)
    plt.close(fig)


def save_latency_by_duration(rows: List[Dict[str, str]]) -> None:
    """Bar chart showing latency percentiles by audio duration bucket."""
    buckets = [row["duration_bucket"] for row in rows]
    counts = [int(row["count"]) for row in rows]
    avg_lat = [float(row["avg_latency_ms"]) for row in rows]
    p50 = [float(row["p50_latency_ms"]) for row in rows]
    p95 = [float(row["p95_latency_ms"]) for row in rows]
    p99 = [float(row["p99_latency_ms"]) for row in rows]

    fig, ax1 = plt.subplots(figsize=(10, 5))
    x = range(len(buckets))
    width = 0.2
    ax1.bar([i - width for i in x], p50, width, label="P50", color="#2A9D8F")
    ax1.bar(list(x), p95, width, label="P95", color="#F4A261")
    ax1.bar([i + width for i in x], p99, width, label="P99", color="#D62828")
    ax1.set_xticks(list(x))
    ax1.set_xticklabels(buckets)
    ax1.set_xlabel("Audio Duration")
    ax1.set_ylabel("Latency (ms)")
    ax1.set_title("March 2026: ASR Latency by Audio Duration")
    ax1.legend(loc="upper left")
    ax1.grid(axis="y", alpha=0.25)

    # Annotate sample counts
    for i, count in enumerate(counts):
        ax1.text(i, p99[i] + max(p99) * 0.02, f"n={count}", ha="center", fontsize=8, color="#555")

    fig.tight_layout()
    fig.savefig(os.path.join(FIG_DIR, "latency_by_duration.png"), dpi=180)
    plt.close(fig)


def save_filter_gap_timeline(rows: List[Dict[str, str]]) -> None:
    """Timeline showing filter activity per day, highlighting gap periods."""
    dates = [row["date"] for row in rows]
    asr_counts = [int(row["asr_count"]) for row in rows]
    filter_pcts = [float(row["filter_pct"]) for row in rows]
    has_gaps = [int(row["has_gap"]) for row in rows]

    fig, ax1 = plt.subplots(figsize=(14, 5))
    colors = ["#D62828" if gap else "#2A9D8F" for gap in has_gaps]
    ax1.bar(range(len(dates)), asr_counts, color=colors, alpha=0.7)
    ax1.set_xlabel("Date")
    ax1.set_ylabel("ASR Count")
    ax1.set_title("Daily ASR Activity & Filter Gaps (red = no filtering)")

    # Only show every Nth date label to avoid crowding
    step = max(1, len(dates) // 15)
    ax1.set_xticks(range(0, len(dates), step))
    ax1.set_xticklabels([dates[i][4:] for i in range(0, len(dates), step)], rotation=45, fontsize=8)

    ax2 = ax1.twinx()
    ax2.plot(range(len(dates)), filter_pcts, marker=".", markersize=4, linewidth=1, color="#F4A261", label="Filter %")
    ax2.set_ylabel("Filter %")
    ax2.legend(loc="upper right")

    ax1.grid(axis="y", alpha=0.25)
    fig.tight_layout()
    fig.savefig(os.path.join(FIG_DIR, "filter_gap_timeline.png"), dpi=180)
    plt.close(fig)


def main() -> None:
    ensure_dir()
    monthly_rows = read_csv(os.path.join(DATA_DIR, "monthly_summary.csv"))
    app_rows = read_csv(os.path.join(DATA_DIR, "committed_apps.csv"))
    hourly_rows = read_csv(os.path.join(DATA_DIR, "hourly_distribution.csv"))
    latency_rows = read_csv(os.path.join(DATA_DIR, "latency_summary.csv"))
    text_len_rows = read_csv(os.path.join(DATA_DIR, "text_length_fate.csv"))
    corrected_funnel_rows = read_csv(os.path.join(DATA_DIR, "corrected_funnel.csv"))
    session_gap_rows = read_csv(os.path.join(DATA_DIR, "session_gap_sensitivity.csv"))
    discard_rows = read_csv(os.path.join(DATA_DIR, "discard_breakdown.csv"))
    latency_dur_rows = read_csv(os.path.join(DATA_DIR, "latency_by_duration.csv"))
    filter_gap_rows = read_csv(os.path.join(DATA_DIR, "filter_gap_analysis.csv"))
    with open(os.path.join(DATA_DIR, "pipeline_summary.json"), "r", encoding="utf-8") as handle:
        summary = json.load(handle)
    with open(os.path.join(DATA_DIR, "behavior_summary.json"), "r", encoding="utf-8") as handle:
        behavior = json.load(handle)

    save_monthly_trend(monthly_rows)
    save_march_funnel(monthly_rows)
    save_app_distribution(app_rows)
    save_hourly_distribution(hourly_rows)
    save_data_quality(summary)
    save_session_classification(behavior)
    save_latency_percentiles(latency_rows)
    save_text_length_fate(text_len_rows)
    save_retry_chain_distribution(behavior)
    save_corrected_funnel(corrected_funnel_rows)
    save_session_gap_sensitivity(session_gap_rows)
    save_discard_breakdown(discard_rows)
    save_latency_by_duration(latency_dur_rows)
    save_filter_gap_timeline(filter_gap_rows)
    print(f"Wrote figures to {FIG_DIR}")


if __name__ == "__main__":
    main()
