# 技术附录：推断方法、验证过程与数据局限性

本文档记录分析中涉及的所有推断逻辑、验证过程和已知局限，供面试深入追问时参考。

---

## 1. 延迟指标定义

系统记录 7 维时序指标（代码见 `src_qt/voice_tools.py`）：

| 字段 | 计算方式 | 含义 |
|------|---------|------|
| `duration` | `len(audio_bytes) / (rate × 2)` | 音频时长（秒） |
| `asr_time_cost` | `time.time()` 包裹 `recognize_pcm()` | ASR 引擎调用耗时（含 IPC、推理、文本清洗） |
| `asr_decode_time` | 引擎返回的 `decode_time`，兜底 = `asr_time_cost` | 本地 SenseVoice 不单独返回，始终 = `asr_time_cost` |
| `queue_wait_time` | `perf_counter_now - queue_enqueued_at - asr_time_cost` | 音频在 ASR 排队中的等待时间 |
| `total_pipeline_time` | `queue_wait_time + asr_time_cost` | VAD 结束到 ASR 结果返回的端到端延迟 |
| `rtf` | `asr_time_cost / duration` | ASR 实时率 |
| `total_rtf` | `total_pipeline_time / duration` | 含排队的总实时率 |

**关键说明**：
- `queue_enqueued_at` 在 `_dispatch_segment()` 中设置，与 `vad_ended` 信号同步发出，即**语音录制结束的瞬间**
- 对本地 SenseVoiceSmall，排队近零（`queue_wait_time ≈ 0`），三个 RTF 值几乎相同
- 分析中使用 `asr_time_cost` 作为延迟指标，它等于用户说完话到看到文字的等待时间，**不含语音本身时长**
- `decode_rtf` 和 `total_rtf` 是为未来云端引擎（如讯飞 API）预留的扩展字段

---

## 2. 覆写推断（6s 阈值）

### 问题

早期版本（1–2 月）不记录 `speech_overwrite` 事件。3/28 起才有 `replacement_filename` 字段。

### 方法

用 717 条有 `replacement_filename` 的真实覆写事件测量实际间隔分布：
- P50 = 3.2s，P90 = 4.9s，93.6% 在 6s 以内
- 6s 吻合 `draft_timeout_seconds=5.0` + ~1s 说话/识别延迟

据此推断：如果一条 unknown 命运的语音在 6s 内被另一条语音跟随，则标记为 `inferred_overwrite`。

### 验证

- 阈值来源有理论支撑（5s 草稿超时配置 + 1s 缓冲）
- 3 月推断覆写 9.2%，显式覆写 11.3%，合计 20.5%——比例合理
- 4 月推断覆写几乎为零（0.1%），符合日志完善后不需要推断的预期

### 局限

- 仍是统计推断，非精确
- 1–2 月覆写推断后的剩余未知项已全部归类为推断丢弃（见下节）

---

## 3. 推断丢弃（inferred discard）

### 问题

1–2 月日志不记录 `ui_discarded` 和 `ui_mark_error` 事件。覆写推断后仍有大量未知命运条目（1 月 37%、2 月 45%）。

### 方法

每条语音的最终命运是互斥的：committed / speech_overwrite / discarded / sys_filtered / mark_error。在已确定 committed、inferred_overwrite、sys_filtered 之后，不存在其他退出路径——剩余条目**只能是丢弃**（超时自动丢弃或用户主动丢弃）。因此将这些条目标记为 `inferred_discard`，使各月命运合计 100%。

### 验证

- 3–4 月有完整事件日志，推断丢弃几乎为零（3,022 → 6），与显式 `ui_discarded` 一致
- 1–2 月推断丢弃率 37–45%，与该阶段工具不成熟、用户尚在适应的预期吻合
- 代码逻辑上不存在第六种退出路径

---

## 4. 过滤配置空窗期

### 发现

通过每日拦截计数分析发现 3/5–3/28 共 17 天零拦截。

### 时间线

| 时期 | 状态 | 详情 |
|------|------|------|
| 1/20–2/26 | 有拦截，`no_reason` | 早期日志未记录拦截原因（698 条） |
| 3/5–3/28 | **无拦截** | 配置文件丢失，879 个纯 "." 未被过滤 |
| 3/29–4/8 | 完整拦截 | `blocklist_word` / `pure_punctuation` / `too_short` 全部生效 |

### 影响

- 3 月前半段的 unknown/committed 统计被标点符号和语气词污染
- 纯 "." 占 ASR 输出 4.3%，4 月已全部被正确拦截
- 导出：`data/filter_gap_analysis.csv`、`data/punctuation_analysis.csv`

---

## 5. 文本长度：标点剥离

ASR 模型（SenseVoiceSmall）经常输出尾部标点。用原始 `text_len` 分档会虚高。

去除标点字符集（`.,?。，？!！…—""''` 等）后，约 19% 的条目变更了分档。分析中的 text_length_fate 图表使用的是去除标点后的内容长度。

---

## 6. 会话间隔敏感度

用 60s / 90s / 120s / 180s / 240s / 300s 六个阈值测试：

| 间隔 | 会话数 | 平均条目/会话 |
|------|--------|-------------|
| 60s | 1,960 | 10.4 |
| 90s | 1,418 | 14.4 |
| 120s | 1,115 | 18.3 |
| 180s | 708 | 28.8 |
| 300s | 375 | 54.1 |

选择 90s 平衡粒度与语义完整性。60s 和 90s 差 38%，图表供读者自行判断。

---

## 7. 完整数据局限性清单

1. **`replacement_filename` 字段从 3/28 才出现**——显式重说链检测只覆盖 ~7 天数据
2. **推断覆写依赖 6s 阈值**——统计推断，非精确
3. **1–2 月丢弃率为推断值**——覆写推断后剩余全部归类为丢弃，无法区分主动 vs 超时
4. **3/5–3/28 过滤配置丢失**——17 天内零拦截，标点/语气词污染统计
5. **文字长度去除标点**——19% 条目变更分档
6. **3 月延迟 P99 是长音频驱动**——148 条 20s+ 环境音，非系统退化
7. **拼音级重说检测未实现**——用户可能用更长句子重新表达同一个词
8. **ASR 延迟 = VAD 结束到结果返回**——不含 VAD 缓冲（~0.8s 静默检测），实际用户感知还要加上 VAD 尾部静默
9. **会话间隔选择有主观性**——提供灵敏度图表
10. **分析聚焦于 3 月**——1–2 月日志 schema 不完整且过滤配置不稳定，4 月仅 8 天，3 月是主力分析期但前半月有过滤空窗

---

## 8. 导出文件清单

| 文件 | 用途 |
|------|------|
| `data/corrected_funnel.csv` | 覆写推断后的修正漏斗 |
| `data/discard_breakdown.csv` | 主动/超时/其他丢弃分类 |
| `data/filter_gap_analysis.csv` | 每日拦截活动与空窗标记 |
| `data/punctuation_analysis.csv` | 标点/单字符项统计 |
| `data/latency_by_duration.csv` | 按音频时长分档的延迟百分位 |
| `data/behavior_summary.json` | 行为分析汇总 |
| `data/key_metrics.json` | 核心评测指标 |
| *以下含原文，仅本地生成，不纳入仓库：* | |
| `data/high_latency_march.csv` | 1,079 条 ≥P95 高延迟项，含识别文本 |
| `data/retry_chains.csv` | 显式重说链（含文本预览） |
| `data/retry_clusters.csv` | 相似度重说聚类（含文本预览） |
| `data/activity_sessions.csv` | 会话分段与行为分类（含文本预览） |
