# 在线记录结构迁移切换服务

页面与服务共同完成一次**可审计**的结构切换：冻结窗 → 双读校验 → 一次性原子切换 → 可恢复。

## 状态机

```
NORMAL ──freeze──▶ FROZEN ──validate──▶ VALIDATING ──通过──▶ VALIDATED ──cutover──▶ DONE
  ▲                  │                      │ 失败(差异落审计, 保持冻结)      │
  │                  │◀─────────────────────┘                              │
  │                  └────────── recover(必须带 reason) ───────────────────┤
  └────────────────────────────────────────────────────────────────────────┘
```

- **DONE 是终态**，不提供自动回退 —— 从机制上杜绝"两套状态都说自己已切开"。
- 每次迁移动作 `epoch+1`，且以 epoch 为条件更新（栅栏令牌）；Postgres 下另加行锁。
  并发管理员只有一个能推进，其余收到 409。
- cutover 是**单事务**（翻阶段 + 生效新结构 + 写审计），崩溃即整体回滚，无半切开状态。
- 校验回填按**水位**（已处理的最大记录 id）断点续跑，upsert 幂等，服务重启后重跑即可。

## 行为约定

| 阶段 | 写入 | 读取 |
|---|---|---|
| NORMAL | 旧结构可写 | 旧结构 |
| FROZEN / VALIDATING / VALIDATED | **全部拒绝**（423 + 原因 + freeze_version） | 同一编号**同时返回新旧两份 + 差异** |
| DONE | 新结构可写；**旧路径明确失败**（410） | 新结构 |

- 校验发现不一致：列出差异（记录/字段/旧值/新值），保持冻结，**阻止切换**；cutover 前还会复核一次。
- recover：回到冻结前可写状态，同事务清理半迁移数据（新表回填行 + 水位归零），原因必填并落审计。
- 审计：操作者、动作、阶段迁移、epoch、应用版本、freeze_version、水位、差异明细、恢复原因，只追加。

## 运行

```bash
# Docker(推荐, Postgres + 应用)
docker compose up --build

# 本地开发(SQLite)
pip install -r requirements.txt
uvicorn app.main:app --reload
```

控制台： http://localhost:8000 （冻结/校验/切换/恢复 + 写入探测 + 差异 + 审计日志）

## API 摘要

```
GET  /api/status                      当前阶段/epoch/水位/版本/待处理差异数
POST /api/admin/freeze     {operator, idempotency_key}
POST /api/admin/validate   {operator, idempotency_key}
POST /api/admin/cutover    {operator, idempotency_key, expected_epoch?}
POST /api/admin/recover    {operator, idempotency_key, reason}
GET  /api/admin/audit                 审计日志
POST /api/records                     旧结构写入(冻结期 423 / 切换后 410)
POST /api/v2/records                  新结构写入(仅 DONE)
GET  /api/records/{id}                按阶段返回 旧/新/双读+diff
GET  /api/records/{id}/compare        冻结窗内双读比对
```

幂等：所有迁移动作要求 `idempotency_key`，重复执行返回首次结果（`replayed: true`），无副作用；
同键不同请求体返回 409。

## 测试

```bash
python3 -m pytest tests/ -q   # 11 个用例: 冻结拒写/双读差异/幂等重放/epoch 栅栏/
                              # 重启续跑/原子切换/恢复清理半迁移数据/审计完整性
```
