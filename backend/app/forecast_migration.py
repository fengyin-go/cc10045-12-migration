"""预测结果按单次运行（run）存储的版本迁移。

旧版把逐日预测直接堆在 ``forecast_results`` 里，多次运行互相覆盖、无法区分
一轮完整预测。迁移后：

* 每次预测执行落成一行 ``forecast_runs``，逐日记录挂在
  ``forecast_run_points``，区间和置信度原样保留；
* 旧版逐日行通过 ``forecast_migration_ledger`` 幂等搬运，已迁移的行不会
  因为重复执行而再造一份历史预测；
* 能确认属于同一轮的存量标为 ``migrated_legacy`` 并认证；无法证明同一轮
  的存量标为 ``legacy_unconfirmed``，只能按未确认来源追检，不能被拼成
  一轮已认证预测；
* 迁移切换期间（``in_progress``）写入的新运行标为 ``pre_switch_shadow``，
  保留来源说明，待补迁核对后再决定认证；
* 迁移全程运营仍可生成误差报告，报告里写明采用的运行范围以及是否包含
  未完全迁移的老数据。
"""
from __future__ import annotations

from collections import defaultdict
from typing import Any, Sequence

from app.database import db

PHASE_KEY = "forecast_run_migration_phase"
PHASE_IN_PROGRESS = "in_progress"
PHASE_SWITCHED = "switched"

# source_kind 取值与 forecast_runs 表 CHECK 约束保持一致。
KIND_NEW_RUN = "new_run"
KIND_MIGRATED = "migrated_legacy"
KIND_SHADOW = "pre_switch_shadow"
KIND_UNCONFIRMED = "legacy_unconfirmed"


def get_phase() -> str:
    """读取当前迁移阶段；未登记时视为进行中。"""
    row = db.fetchone("SELECT value FROM _schema WHERE key=?", [PHASE_KEY])
    return row["value"] if row else PHASE_IN_PROGRESS


def set_phase(phase: str) -> None:
    if phase not in (PHASE_IN_PROGRESS, PHASE_SWITCHED):
        raise ValueError(f"未知迁移阶段: {phase}")
    db.execute(
        "INSERT INTO _schema(key, value) VALUES(?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        [PHASE_KEY, phase],
    )


def is_switched() -> bool:
    return get_phase() == PHASE_SWITCHED


def _next_run_seq(task_id: int) -> int:
    row = db.fetchone(
        "SELECT COALESCE(MAX(run_seq), 0) + 1 AS next_seq FROM forecast_runs WHERE task_id=?",
        [task_id],
    )
    return int(row["next_seq"]) if row else 1


def _create_run(
    task_id: int,
    source_kind: str,
    *,
    certified: bool,
    migration_batch: str | None,
    source_note: str,
    completed_at: str | None = None,
) -> int:
    run_id = db.insert("forecast_runs", {
        "task_id": task_id,
        "run_seq": _next_run_seq(task_id),
        "source_kind": source_kind,
        "certified": 1 if certified else 0,
        "migration_batch": migration_batch,
        "source_note": source_note,
        "status": "completed",
        "completed_at": completed_at or db.now(),
    })
    return run_id


def _insert_point(run_id: int, row: dict[str, Any]) -> None:
    db.insert("forecast_run_points", {
        "run_id": run_id,
        "task_id": row["task_id"],
        "forecast_date": row["forecast_date"],
        "predicted_value": row["predicted_value"],
        "lower_bound": row.get("lower_bound"),
        "upper_bound": row.get("upper_bound"),
        "confidence": row.get("confidence", 0.95),
        "legacy_result_id": row["id"],
    })


def migrate_legacy_batch(batch_size: int = 500, batch_id: str | None = None) -> dict[str, Any]:
    """搬运一批尚未迁移的旧版预测行，幂等且可重复执行。

    分组启发式（旧表没有 run 概念，只能按可得证据保守判断）：
    同一任务下 ``computed_at`` 完全相同的一组逐日记录，视为同一轮存量；
    一个任务只有这一组时认证为 ``migrated_legacy``，出现多组则说明该任务
    历史上发生过多次覆盖、来源不明确，全部按 ``legacy_unconfirmed`` 追检。

    返回本批迁移计数与累计进度，便于运营核对“读到一半”的真实范围。
    """
    batch = batch_id or f"MIG_{db.now().replace(':', '').replace('-', '')}"

    # 一旦发现仍有旧版存量要搬，说明迁移尚未真正完成：即使库此前处于
    # switched（例如切换后又补录了老数据），也要回到 in_progress，避免
    # 把切换前来源的数据误当成切换后的已认证新运行。
    if _count_pending_legacy() > 0 and get_phase() == PHASE_SWITCHED:
        set_phase(PHASE_IN_PROGRESS)

    legacy_rows = db.fetchall(
        "SELECT r.* FROM forecast_results r "
        "LEFT JOIN forecast_migration_ledger l ON l.legacy_result_id = r.id "
        "WHERE l.id IS NULL ORDER BY r.task_id ASC, r.computed_at ASC, r.forecast_date ASC "
        "LIMIT ?",
        [batch_size],
    )
    if not legacy_rows:
        return {
            "ok": True,
            "batch": batch,
            "migrated_rows": 0,
            "created_runs": 0,
            "unconfirmed_runs": 0,
            "remaining_legacy": _count_pending_legacy(),
        }

    grouped: dict[int, dict[str, list[dict[str, Any]]]] = defaultdict(lambda: defaultdict(list))
    for row in legacy_rows:
        grouped[row["task_id"]][row["computed_at"]].append(row)

    created_runs = 0
    unconfirmed_runs = 0
    migrated_rows = 0

    for task_id, groups in grouped.items():
        # 统计该任务在旧表中的全量 computed_at 分组，判断来源是否唯一。
        all_groups = db.fetchall(
            "SELECT computed_at, COUNT(*) AS cnt FROM forecast_results WHERE task_id=? GROUP BY computed_at",
            [task_id],
        )
        single_round = len(all_groups) <= 1

        for computed_at, rows in groups.items():
            rows.sort(key=lambda r: r["forecast_date"])
            dates = [r["forecast_date"] for r in rows]
            # 即使只有一组，逐日日期不连续也不能认证为一轮完整预测。
            contiguous = _dates_are_contiguous(dates)
            if single_round and contiguous:
                source_kind = KIND_MIGRATED
                certified = True
                note = f"旧版存量按 computed_at={computed_at} 归并为单轮且日期连续，迁移认证"
            else:
                source_kind = KIND_UNCONFIRMED
                certified = False
                unconfirmed_runs += 1
                reason = (
                    f"存在 {len(all_groups)} 个 computed_at 分组，来源不明确"
                    if not single_round
                    else "逐日业务日期不连续，无法证明是一轮完整预测"
                )
                note = f"旧版存量{reason}，按未确认来源追检"

            run_id = _create_run(
                task_id,
                source_kind,
                certified=certified,
                migration_batch=batch,
                source_note=note,
                completed_at=computed_at,
            )
            if single_round:
                created_runs += 1

            for row in rows:
                _insert_point(run_id, row)
                db.insert("forecast_migration_ledger", {
                    "legacy_result_id": row["id"],
                    "task_id": task_id,
                    "run_id": run_id,
                    "migration_batch": batch,
                    "source_kind": source_kind,
                })
                migrated_rows += 1

    return {
        "ok": True,
        "batch": batch,
        "migrated_rows": migrated_rows,
        "created_runs": created_runs,
        "unconfirmed_runs": unconfirmed_runs,
        "remaining_legacy": _count_pending_legacy(),
    }


def _count_pending_legacy() -> int:
    row = db.fetchone(
        "SELECT COUNT(*) AS cnt FROM forecast_results r "
        "LEFT JOIN forecast_migration_ledger l ON l.legacy_result_id = r.id "
        "WHERE l.id IS NULL"
    )
    return int(row["cnt"]) if row else 0


def migrate_all_legacy(batch_size: int = 500) -> dict[str, Any]:
    """循环搬运直到旧表没有遗留行；重复执行不会新增任何预测点。"""
    total_rows = 0
    total_runs = 0
    total_unconfirmed = 0
    batches = 0
    while True:
        result = migrate_legacy_batch(batch_size=batch_size)
        if result["migrated_rows"] == 0:
            break
        total_rows += result["migrated_rows"]
        total_runs += result["created_runs"]
        total_unconfirmed += result["unconfirmed_runs"]
        batches += 1
    return {
        "ok": True,
        "batches": batches,
        "migrated_rows": total_rows,
        "certified_runs": total_runs,
        "unconfirmed_runs": total_unconfirmed,
        "remaining_legacy": _count_pending_legacy(),
    }


def switch_to_runs() -> dict[str, Any]:
    """完成切换：补迁全部旧行，并把切换前影子运行转正。

    切换前写入的 ``pre_switch_shadow`` 运行只在其逐日点完整（与任务预测
    周期一致、日期连续）时才认证；不完整的保持未确认，等待补迁核对。
    """
    migration = migrate_all_legacy()

    shadow_runs = db.fetchall(
        "SELECT * FROM forecast_runs WHERE source_kind=? ORDER BY id",
        [KIND_SHADOW],
    )
    promoted = 0
    kept_pending = 0
    for run in shadow_runs:
        if _run_is_complete(run):
            db.update("forecast_runs", {
                "source_kind": KIND_NEW_RUN,
                "certified": 1,
                "source_note": run["source_note"] + "；切换时核对完整，转正认证",
            }, "id=?", [run["id"]])
            promoted += 1
        else:
            kept_pending += 1

    set_phase(PHASE_SWITCHED)
    return {
        "ok": True,
        "phase": PHASE_SWITCHED,
        "migration": migration,
        "shadow_promoted": promoted,
        "shadow_kept_unconfirmed": kept_pending,
    }


def _run_is_complete(run: dict[str, Any]) -> bool:
    """一轮运行是否完整：点数达到任务周期且业务日期逐日连续。"""
    points = db.fetchall(
        "SELECT forecast_date FROM forecast_run_points WHERE run_id=? ORDER BY forecast_date",
        [run["id"]],
    )
    if not points:
        return False
    task = db.fetchone("SELECT forecast_period_days FROM forecast_tasks WHERE id=?", [run["task_id"]])
    expected = int(task["forecast_period_days"]) if task else len(points)
    if len(points) < expected:
        return False
    return _dates_are_contiguous([p["forecast_date"] for p in points])


def _dates_are_contiguous(dates: Sequence[str]) -> bool:
    from datetime import date, timedelta

    parsed = []
    for d in dates:
        try:
            parsed.append(date.fromisoformat(d[:10]))
        except ValueError:
            return False
    parsed.sort()
    for prev, cur in zip(parsed, parsed[1:]):
        if cur - prev != timedelta(days=1):
            return False
    return True


def list_unconfirmed_runs(task_id: int | None = None) -> list[dict[str, Any]]:
    """列出所有未认证运行，供运营按未确认来源逐条追检。"""
    if task_id is None:
        return db.fetchall(
            "SELECT * FROM forecast_runs WHERE certified=0 ORDER BY task_id, id"
        )
    return db.fetchall(
        "SELECT * FROM forecast_runs WHERE certified=0 AND task_id=? ORDER BY id",
        [task_id],
    )


def certify_run(run_id: int, note: str = "人工核对来源后认证") -> dict[str, Any]:
    """补迁核对后显式认证一轮运行。

    只有逐日点日期连续的运行才允许认证；来源不明的存量不会被自动拼进
    任何已认证轮次。
    """
    run = db.fetchone("SELECT * FROM forecast_runs WHERE id=?", [run_id])
    if run is None:
        return {"ok": False, "message": f"预测运行 {run_id} 不存在"}
    if not _run_is_complete(run):
        return {"ok": False, "message": "运行逐日点不完整或日期不连续，不能认证为一轮完整预测"}

    db.update("forecast_runs", {
        "certified": 1,
        "source_note": (run["source_note"] or "") + f"；{note}",
    }, "id=?", [run_id])
    return {"ok": True, "run_id": run_id, "source_kind": run["source_kind"]}


def migration_status() -> dict[str, Any]:
    """汇总迁移进度，供切换后回查当时采用的真实数据范围。"""
    ledger_total = db.count("forecast_migration_ledger")
    legacy_total = db.count("forecast_results")
    by_kind = {
        KIND_NEW_RUN: 0,
        KIND_MIGRATED: 0,
        KIND_SHADOW: 0,
        KIND_UNCONFIRMED: 0,
    }
    for row in db.fetchall("SELECT source_kind, COUNT(*) AS cnt FROM forecast_runs GROUP BY source_kind"):
        by_kind[row["source_kind"]] = row["cnt"]
    return {
        "phase": get_phase(),
        "legacy_total": legacy_total,
        "ledger_migrated": ledger_total,
        "remaining_legacy": _count_pending_legacy(),
        "runs_by_source": by_kind,
        "certified_runs": db.count("forecast_runs", "certified=1"),
        "unconfirmed_runs": db.count("forecast_runs", "certified=0"),
    }


def ensure_startup_migration() -> dict[str, Any]:
    """启动时确保新表可用并尽量完成存量迁移。

    已有旧数据时只做搬运、不擅自切换（保留迁移期可观测性）；没有任何旧
    预测行时直接处于 switched，全新部署无需迁移。
    """
    if db.count("forecast_results") == 0 and db.count("forecast_runs") == 0:
        set_phase(PHASE_SWITCHED)
        return {"phase": PHASE_SWITCHED, "skipped": "no_legacy_data"}
    summary = migrate_all_legacy()
    if _count_pending_legacy() == 0 and get_phase() != PHASE_SWITCHED:
        # 旧行全部有台账后仍保持 in_progress，由运营显式 switch_to_runs 切换，
        # 这样切换前写入的影子运行来源可以被完整回查。
        pass
    return {"phase": get_phase(), **summary}
