# 审计证据查询与一致性证明 · 接口文档

在"审计事件回放、补偿审批与执行窗口"之上提供**固定读取边界**的跨流证据时间线、
逐页哈希链连续性证明，以及**异步分段 JSONL 证据包**导出（暂停/恢复/取消/失败重试/
幂等重放 + 一次性下载 + 可复算 manifest）。

所有写操作要求 `operator` 与 `idempotency_key`；证据会话与导出的操作者权限严格限定为
**会话创建者**（取证操作）。时间参数均为 ISO 8601（建议 UTC，如 `2026-09-13T00:00:00Z`）。

- 普通游标错误（游标不属于会话/过期/重放/损坏）→ **409**，错误码 `cursor_invalid`。
- 哈希链证明失败（缺失/乱序/重复/stream_hash/prev_hash/边界锚点）→ **422**，
  `detail.breaks[]` 给出机器可读位置与原因，会话立即置 `BROKEN` 并锁定。
- 权限不符/状态不允许/幂等键跨请求复用 → **409/403**；会话或计划不存在 → **404**。

断链原因码（`breaks[].code`）：

| code | 含义 | 关键定位字段 |
| --- | --- | --- |
| `global_seq_duplicate` | 片段内或相对已交付边界 global_seq 重复/非递增 | `global_seq`, `expected_after` |
| `stream_seq_gap` | 单流顺序号不连续（流内事件缺失） | `stream_key`, `stream_seq`, `expected` |
| `stream_seq_duplicate` | 单流顺序号重复 | `stream_key`, `stream_seq` |
| `stream_hash_mismatch` | 重算 stream_hash 与存储不一致（事件被篡改/前序缺失，含片段前边界事件） | `stream_key`, `stream_seq`, `global_seq`, `expected`, `actual` |
| `prev_hash_mismatch` | 事件 prev_stream_hash 与同流前一事件不衔接 | `stream_key`, `stream_seq`, `expected_prev`, `actual_prev` |
| `boundary_anchor_mismatch` | 创建时固化的流尾锚点被删改（边界内删除/篡改） | `stream_key`, `expected`, `actual` |
| `out_of_order` | 事件时间戳倒流超过 `AUDIT_MAX_OUT_OF_ORDER_SECONDS`（默认 300s） | `stream_key`, `stream_seq`, `global_seq` |
| `cursor_invalid` | 游标非本会话签发/已过期/重复使用/漏页/跨会话 | `cursor_page`, `pages_delivered`, `last_cursor` |

---

## 1. 证据查询会话

### POST /api/admin/evidence/sessions  · 创建固定边界会话

请求：
```json
{
  "operator": "alice",
  "idempotency_key": "ev-session-1",
  "plan_id": "P123",
  "start_global_seq": 0,
  "start_ts": "2026-09-13T00:00:00Z",
  "end_ts": "2026-09-13T12:00:00Z",
  "event_types": ["BATCH_CUTOVER", "COMP_EXECUTED"],
  "sources": ["internal", "api", "system"]
}
```
仅 `plan_id` 必填。`event_types` 取统一事件流类型；`sources ∈ {internal,api,system}`；
时间范围为闭区间。流集合在创建时固定为**计划流 `plan_id` + 该计划各步骤批次流
`batch:<batch_id>`**（补偿控制流 `COMP_*` 追加在计划流上，天然包含）。

响应（201，节选）：
```json
{
  "session_id": "ESab12…",
  "status": "ACTIVE",
  "filters": { "start_global_seq": 0, "start_ts": null, "end_ts": null,
               "event_types": [], "sources": [],
               "streams": ["P123", "batch:B1", "batch:B2"] },
  "boundary": {
    "upper_global_seq": 1432,
    "upper_event_id": 5821,
    "upper_event_ts": "2026-09-13T09:00:00",
    "anchors": {
      "P123": { "tail_stream_seq": 88, "tail_stream_hash": "…", "tail_global_seq": 1432 }
    }
  },
  "expected_count": 120,
  "expected_streams": ["P123", "batch:B1"],
  "cursor": { "delivered_global_seq": 0, "last_cursor": null, "pages_delivered": 0 }
}
```
**固定边界语义**：`upper_global_seq` 为创建时刻全库最大 global_seq；之后新写入事件
global_seq 更大，任何翻页都不会读到它们（不漏、不重、不插入新结果）。
`boundary.anchors` 是每条流在边界处的尾事件指纹，每页都复核。无结果是合法边界
（`expected_count=0`，翻页立即到 CLOSED，仍可导出空证据包）。

### POST /api/admin/evidence/sessions/{id}/pages  · 翻一页（带证明）

请求：
```json
{ "operator": "alice", "cursor": "<上一页 next_cursor，第一页省略>", "limit": 100 }
```
响应（200，节选）：
```json
{
  "session_id": "ESab12…", "page_no": 2,
  "filters": { "…": "与会话一致, 不可变" },
  "boundary": { "upper_global_seq": 1432, "…": "每页回显固定边界" },
  "cursor": {
    "last_delivered": 240,
    "next_cursor": "<不透明 HMAC 游标>",
    "has_more": true
  },
  "items": [{
    "global_seq": 231, "stream_key": "P123", "stream_seq": 42,
    "flow": "plan",
    "event_type": "COMP_APPROVAL",
    "plan_id": "P123", "batch_id": null, "scan_id": null, "step_seq": null,
    "correlation_id": "…", "prev_stream_hash": "…", "stream_hash": "…",
    "payload": { "…": "…" }, "operator": "bob", "source": "system",
    "event_ts": "2026-09-13T08:00:00",
    "is_compensation_control": true,
    "comp_task_id": "CT…", "comp_action_seq": 3
  }],
  "item_count": 100,
  "proof": {
    "status": "ok",
    "fragment_digest": "sha256: …(逐行规范化 JSON 拼接的摘要)",
    "stream_boundaries": {
      "P123": { "first_stream_seq": 31, "last_stream_seq": 42,
                "first_global_seq": 201, "last_global_seq": 240,
                "first_stream_hash": "<第31行的 prev_hash>",
                "last_stream_hash": "<第42行 hash>" }
    },
    "prev_global_seq": 200,
    "verified_at": "2026-09-13T09:01:00"
  },
  "session_status": "ACTIVE",
  "pages_delivered": 2,
  "expected_count": 120
}
```

游标纪律（违反一律拒绝且不推进任何状态）：
- 第一页可省略 `cursor`；此后**必须**携带上一页响应里的 `next_cursor`；
- 游标只能沿**同一会话**继续；重放已消费游标、漏页、跨会话、篡改签名 → `cursor_invalid`；
- 翻到最后一页后会话 `CLOSED`；对 CLOSED 会话无游标再请求，幂等返回空终止页。

证明失败（422）示例：
```json
{ "detail": {
  "error": "evidence_chain_broken",
  "reason": "流 P123 顺序号不连续: stream_seq=8, 期望 7 …",
  "breaks": [
    { "code": "stream_seq_gap", "stream_key": "P123", "stream_seq": 8,
      "expected": 7, "global_seq": 255, "message": "…" },
    { "code": "prev_hash_mismatch", "…": "…" },
    { "code": "stream_hash_mismatch", "…": "…" }
  ]
}}
```
此后会话 `BROKEN`：翻页/创建导出一律 409，断链证据（`broken_reason/broken_at`）永久保留；
需要取证时重新创建会话。

### GET /api/admin/evidence/sessions[?plan_id=&status_filter=&limit=]
会话列表（含每个会话的导出任务概要）。

### GET /api/admin/evidence/sessions/{id}[?with_pages=true]
会话详情：固定条件、固定边界与锚点、当前游标/已交付页数、校验状态、断链原因、
逐页留痕（每页范围/条目数/片段摘要/流边界）与导出列表。

---

## 2. 证据导出任务（异步分段 JSONL）

状态机：
```
QUEUED ─claim→ RUNNING ─全部分段+清单完成→ COMPLETED(终态, 包不可变)
  │               ├─ pause(分段边界) → PAUSED ─resume→ QUEUED
  │               ├─ 导出期间检测到事件链变化 → PAUSED(chain_changed, 留断链证据; 绝不出完整包)
  │               └─ 可恢复错误(包缺失/摘要失配/内部错误) → FAILED ─resume→ QUEUED
  └────────────────────────── cancel → CANCELED(终态, 禁止继续)
```
- 段大小创建时固化（默认 `EVIDENCE_SEGMENT_SIZE=200`，1..1000）；
  段范围完全由（固定边界、段序号、段大小）决定，**重跑幂等、不重复写包**：
  已完成分段（文件 sha256 + 逐行摘要落库）直接复用，失配/缺失的段才原子替换重生成。
- 每个分段生成前都重算该段的哈希链证明与边界锚点；打包前对**全量范围再做一次证明**，
  任何链变化都让任务 `PAUSED(chain_changed)`，`chain_break` 记录原因码/位置/受影响流，
  **不会产出标记为 COMPLETED 的包**。
- `resume` 断链暂停前，从固定边界起点对全部涉及流**重新全量校验**：不通过 → 409 保持暂停；
  通过 → 逐段复算，失配段标记重生成，从首个未完成分段继续。
- 重启：遗留 RUNNING 回 QUEUED（段是崩溃边界，先 `.tmp` 再原子替换）。
- 并发：同时 RUNNING 的导出不超过 `EVIDENCE_EXPORT_MAX_CONCURRENCY`（默认 2）。

### POST /api/admin/evidence/exports  · 创建并排队
```json
{ "operator": "alice", "idempotency_key": "ev-exp-1",
  "session_id": "ESab12…", "segment_size": 200 }
```
- 会话 `BROKEN` → 409；操作者不是会话创建者 → 409；
- 同会话活动导出（QUEUED/RUNNING/PAUSED）重复创建幂等回显（换幂等键也回显同一任务）；
  COMPLETED 后重复创建回显同一不可变包；CANCELED/FAILED 后允许重新发起。

### GET /api/admin/evidence/exports[?session_id=&plan_id=&status_filter=]
### GET /api/admin/evidence/exports/{id}[?with_events=true]
详情含：进度（分段/事件数、当前段、起止 global_seq）、暂停原因、断链证据、
失败码与原因、逐段（范围/文件大小/逐行摘要/文件 sha256/尝试次数）、manifest 摘要、
事件流水（创建/认领/分段开始完成/用户暂停/断链暂停/重校验/失败/重试/取消/完成/下载）。

### POST /api/admin/evidence/exports/{id}/pause|resume|cancel
`{operator, idempotency_key}`。同态重复调用返回 `already_in_state:true`，同键重放
`replayed:true`；终态后 resume/pause → 409（重复 cancel 幂等）。

### GET /api/admin/evidence/exports/{id}/verify?operator=
回读 zip 重算逐文件/content/manifest 摘要；不一致或包缺失 → 导出置 FAILED（原包保留取证）。

### POST /api/admin/evidence/exports/{id}/download-token
`{operator, idempotency_key}`，仅 COMPLETED 且为会话创建者。返回**一次性**令牌：
```json
{ "download_id": "ED…", "token": "…(明文仅此一次返回)", "expires_at": "…", "one_time": true }
```
TTL 由 `EVIDENCE_DOWNLOAD_TTL_SECONDS`（默认 600s）控制；同幂等键重放回显首次结果。

### GET /api/admin/evidence/downloads/{token}?operator=
兑换一次性下载（`application/zip`）。首次使用即标记 `used_at/used_by`；重复使用 → 409，
过期/不存在 → 409/404。

---

## 3. 证据包格式（确定性 zip）

固定文件顺序与 zip 时间戳（1980-01-01），同输入字节级一致：

```
metadata.json                 会话/边界/筛选/操作者/段大小/内容清单(不可变标记)
events.jsonl                  全量事件(每行一个规范化事件 JSON, 末尾 \n)
segments/segment-00001.jsonl  分段事件(与 events.jsonl 对应字节区间一致)
segments/segment-00002.jsonl
…
manifest.json                 段清单 / 每条流摘要 / 逐文件 sha256 / content_digest / manifest_hash
```

- 事件规范化字段（`fragment_digest`、页面留痕、段、events.jsonl、流摘要共用同一序列化，
  跨处可复算）：`global_seq, stream_key, stream_seq, event_type, plan_id, batch_id,
  scan_id, step_seq, correlation_id, prev_global_seq, prev_stream_hash, stream_hash,
  payload, operator, source, event_ts`（JSON sort_keys，无空格，行尾 `\n`）。
- `content_digest = sha256(按文件名排序的 "sha256(文件)␠␠文件名\n" 拼接)`（仿 git tree）；
- 每条流摘要：事件数、起止 stream_seq/global_seq、补偿控制事件数、操作者集合、批次集合、
  `stream_digest = sha256(该流逐行规范化事件)`；
- `manifest_hash = sha256(manifest 中除 manifest_hash 外全部字段的规范化 JSON)`；
  离线复算方式：读取 manifest.json，删除 `manifest_hash` 键，规范化（sort_keys）后 sha256，
  应与包内/库内/详情接口返回的 `manifest_hash` 完全一致。

## 4. 配置

| 环境变量 | 默认 | 说明 |
| --- | --- | --- |
| `EVIDENCE_SEGMENT_SIZE` | 200 | 导出默认段大小（1..1000） |
| `EVIDENCE_PAGE_LIMIT` | 100 | 翻页默认页大小（最大 500） |
| `EVIDENCE_EXPORT_MAX_CONCURRENCY` | 2 | 同时 RUNNING 的导出任务数（至少 1） |
| `EVIDENCE_WORKER_POLL_INTERVAL` | 0.5 | worker 轮询秒数 |
| `EVIDENCE_STORE_DIR` | `./evidence_store` | 证据包/分段落盘目录（原子写 `.tmp` 替换） |
| `EVIDENCE_DOWNLOAD_TTL_SECONDS` | 600 | 一次性下载令牌有效期（至少 30s） |
| `EVIDENCE_CURSOR_SECRET` | 按 DATABASE_URL 派生 | 游标 HMAC 密钥（配置后重启旧游标失效，不影响正确性） |
| `AUDIT_MAX_OUT_OF_ORDER_SECONDS` | 300 | 乱序证明容忍秒数（与快照/补录共用） |
| `PLAN_WORKER_ENABLED=0` | 1 | 一并关闭证据导出后台线程（测试手动驱动） |

包文件目录：`${EVIDENCE_STORE_DIR}/<export_id>/segments/segment-*.jsonl` 与
`${EVIDENCE_STORE_DIR}/<export_id>/evidence-<export_id>.zip`。

---

# 证据复核与签署归档 · 接口补充

在固定边界会话与已完成证据包之上提供**双人逐事件复核 → 不可变签署归档**流程。
复核单创建时**固定依据**(筛选条件、起止 global_seq、范围指纹、导出 manifest_hash)；
底层事件链、查询结果(会话/页面)与已完成导出包全程**只读**。

状态机：

```
OPEN ─两名不同操作者对全部事件签完→ archive ─→ ARCHIVED(终态, 签署摘要不可变, 禁止修改)
  │  ▲                                                  
  │  └ reverify 通过(依据修复, scope_version+1, 旧结论留史)
  └─ 提交/归档/reverify 发现依据变化 ─→ INVALIDATED(冻结提交与归档, 机器可读原因)
```

- 逐事件结论：`CONFIRMED`（确认）/`QUESTIONED`（存疑，**说明必填**）/
  `EXCLUDED`（排除，**说明必填**）；结论只能引用固定会话范围内的事件（按
  `global_seq` 定位，越界 409 `event_out_of_scope`）。
- 版本栅栏：每次成功提交结论复核单 `version` +1；提交必须携带
  **`If-Match: <version>`** 请求头（过期/缺失/非法 → 409
  `version_conflict`/`if_match_required`/`if_match_invalid`，后写不覆盖先写）。
- 去重：同一依据版本下 `(event, operator)` 唯一；同操作者重复签署 → 409
  `duplicate_signature`（不覆盖）；归档要求每个事件都有**两名不同操作者**的
  当前版本结论（缺签 409 `signatures_incomplete`，仅一人 409 `two_operators_required`）。
- 依据再校验：每次提交结论、归档以及显式 reverify 都重新
  ① 全量重算固定范围哈希链；② 重算范围指纹（数量/内容/顺序）；
  ③ 比对固定 `manifest_hash`；④ 回读 zip 包重算摘要（只读，不改导出任务状态）。
  任一不一致 → 复核单转 **INVALIDATED** 并记录 `invalid_reason`
  （原因码 `chain_broken` / `scope_changed` / `manifest_hash_mismatch` /
  `package_verification_failed`，带 `breaks`/`detail` 定位信息），提交与归档冻结。
- 修复后重新校验通过：`scope_version+1`，旧结论作为**历史保留**（详情 `history[]`），
  当前签署集合清空，需要基于新版本重新逐事件签署。

## 5. 复核单接口

### POST /api/admin/evidence/reviews · 创建复核单（固定依据）
```json
{ "operator": "alice", "idempotency_key": "rv-1",
  "session_id": "ESab12…", "export_id": "EE…(可空, 默认取最近 COMPLETED 包)" }
```
- 前置：会话必须 **CLOSED**（翻到固定边界）且未 BROKEN；必须存在属于该会话的
  **COMPLETED** 导出包（否则 409 `session_not_closed`/`session_broken`/
  `export_not_completed`/`export_session_mismatch`）；仅会话创建者可创建。
- 创建即做一次完整依据校验；同一(会话,导出)重复创建幂等回显未归档复核单；
  已归档后禁止再建（409 `review_already_archived`）。
- 响应固定依据：`fixed.filters`（筛选条件快照）、`fixed.start_global_seq`、
  `fixed.upper_global_seq`、`fixed.first/last_global_seq`、`fixed.total_events`、
  `fixed.scope_fingerprint`（逐行规范化事件有序拼接的 sha256）、
  `fixed.export_manifest_hash`/`export_content_digest`；
  另含 `version`（If-Match 版本）、`scope_version`（依据版本）、`pending_count`、
  `stats`（签署统计）与 `items[]`（事件详情 + 当前签署）。

### GET /api/admin/evidence/reviews[?session_id=&export_id=&plan_id=&status_filter=&limit=]
复核单列表（状态必须是 `OPEN/INVALIDATED/ARCHIVED`，非法 422）。

### GET /api/admin/evidence/reviews/{id}[?pending_only=&limit=&offset=&with_history=&with_events=]
复核单页面数据：固定依据、版本、待处理数量、**每个事件的详情
（`items[].event`）与结论/说明/操作者/版本（`items[].signatures[]`）**、
失效原因（`invalid_reason`）、事件流水（`events[]`）、历史依据版本签署
（`history[]`）与归档后的不可变 `signed_summary`。

### POST /api/admin/evidence/reviews/{id}/conclusions · 逐事件提交结论
请求头 **`If-Match: <当前 version>`**：
```json
{ "operator": "bob", "idempotency_key": "c-1",
  "global_seq": 231, "verdict": "QUESTIONED", "note": "时间戳与工单不一致" }
```
- 越界引用 / 重复签署 / 缺说明 / 非法结论 / 依据变化 / 已归档 → 409（机器可读
  `detail.code`）；成功返回新 `version` 与 `stats`（含待处理数量）。同幂等键
  重放返回首次结果（`replayed:true`），不产生第二条结论。

### POST /api/admin/evidence/reviews/{id}/reverify · 显式重新校验固定依据
`{operator, idempotency_key}`。返回 `{valid, status, scope_version,
invalid_reason, stats}`；INVALIDATED 依据修复后调用通过即恢复 OPEN（依据版本+1，
旧结论留史）；ARCHIVED 单拒绝（409）。

### POST /api/admin/evidence/reviews/{id}/archive · 归档（不可变签署摘要）
`{operator, idempotency_key}`。全部事件两名不同操作者签署且归档前依据再校验通过
才可归档。归档产物 `signed_summary`：
```json
{
  "review_id": "ER…", "session_id": "ES…", "export_id": "EE…", "plan_id": "P…",
  "scope_version": 1,
  "fixed_filters": { "…固定筛选条件快照…" },
  "event_range": {
    "start_global_seq": 0, "upper_global_seq": 1432,
    "first_global_seq": 1, "last_global_seq": 1432, "total_events": 120,
    "scope_fingerprint": "sha256…"
  },
  "manifest_hash": "…(固定的导出包摘要)", "content_digest": "…",
  "operators": ["bob", "carol"], "operator_count": 2,
  "conclusion_stats": { "total": 240, "events_signed": 120,
    "by_verdict": { "CONFIRMED": 238, "QUESTIONED": 1, "EXCLUDED": 1 } },
  "events": [{ "global_seq": 231, "signers": ["bob", "carol"],
               "verdicts": ["CONFIRMED", "QUESTIONED"] }],
  "archived_by": "dave", "archived_at": "…",
  "signature_hash": "sha256(除 signature_hash 外全部字段的规范化 JSON)"
}
```
归档后提交结论/重新校验/再次创建均被拒绝；重复归档：同键幂等回显，
换键返回 `already_in_state:true` 且无副作用。`signature_hash` 可离线复算
（删除该键后 sort_keys 规范化 sha256，与 `manifest_hash` 同一算法约定）。

## 6. 失效原因码（`invalid_reason.code` / 冲突 `detail.code`）

| code | 含义 | 关键定位字段 |
| --- | --- | --- |
| `chain_broken` | 固定范围哈希链重新校验不一致 | `breaks[]`（复用分页断链原因码） |
| `scope_changed` | 固定范围事件数/内容/顺序指纹变化 | `detail.expected_events/actual_events/*_fingerprint` |
| `manifest_hash_mismatch` | 导出 manifest_hash 与固定值不一致 | `detail.expected/actual_manifest_hash` |
| `package_verification_failed` | zip 包回读重算摘要失败（缺失/损坏/篡改） | `detail.reason_code/issues` |
| `event_out_of_scope` | 结论引用的事件不在固定会话范围内 | `global_seq`, `upper_global_seq`, `streams` |
| `version_conflict` | If-Match 过期（乐观并发失败） | `expected`, `supplied` |
| `if_match_required` / `if_match_invalid` | 缺 If-Match 头 / 版本号非法 | — |
| `duplicate_signature` | 同操作者对同事件在当前依据版本已签署 | `existing_verdict` |
| `signatures_incomplete` | 归档时仍有事件未集齐两名签署 | `pending_count`, `pending_global_seqs` |
| `two_operators_required` | 归档要求两名不同操作者 | — |
| `review_invalidated` / `review_archived` | 复核单已失效（冻结）/已归档（终态不可改） | — |

