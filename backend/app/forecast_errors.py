"""预测误差报告：从按运行存储的预测结果集读取预测，与实际指标值按业务日关联。

核心不变量：

* 数据来源只认 ``forecast_runs`` / ``forecast_run_points``（迁移后的预测
  结果集），旧版 ``forecast_results`` 仅在迁移台账里留痕，不再直接喂给报告；
* 每个业务日的预测点必须记录它来自哪一轮运行（``run_scope``），报告写明
  采用的运行范围，以及是否包含未完全迁移的老数据；
* 一份新签发报告里，被标记为“已认证”的连续日期只能来自同一轮完整预测
  （``certified=1`` 的同一个 run）。存量行来源不明确时，只能落在
  ``legacy_unconfirmed`` 段，绝不允许把不同轮次（例如历史 6 与新运行 9）
  拼成一轮已认证预测；
* 切换前生成的报告会冻结当时实际采用的逐日记录，切换后可凭
  :func:`audit_error_report` 回查真实数据范围。
"""
from __future__ import annotations

import json
from collections import defaultdict
from typing import Any

from app.database import db
from app.forecast_migration import (
    KIND_MIGRATED, KIND_NEW_RUN, KIND_SHADOW, KIND_UNCONFIRMED,
    get_phase, is_switched,
)

LEGACY_KINDS = {KIND_MIGRATED, KIND_UNCONFIRMED}


def _business_day(value: str) -> str:
    """把 snapshot_time / forecast_date 统一截成业务日（YYYY-MM-DD）。"""
    return str(value)[:10]


def _actuals_by_day(metric_id: int, limit: int = 90) -> dict[str, float]:
    """取指标实际值，按业务日取当天最新一条快照。"""
    rows = db.fetchall(
        "SELECT snapshot_value, snapshot_time FROM metric_values "
        "WHERE metric_id=? ORDER BY snapshot_time DESC LIMIT ?",
        [metric_id, limit * 3],
    )
    latest: dict[str, tuple[str, float]] = {}
    for row in rows:
        day = _business_day(row["snapshot_time"])
        if day not in latest or row["snapshot_time"] > latest[day][0]:
            latest[day] = (row["snapshot_time"], float(row["snapshot_value"]))
    return {day: payload[1] for day, payload in latest.items()}


def _candidate_points(task_id: int) -> list[dict[str, Any]]:
    """读取任务下全部迁移后预测点，附带运行来源与认证标记。"""
    return db.fetchall(
        "SELECT p.forecast_date, p.predicted_value, p.lower_bound, p.upper_bound, "
        "p.confidence, p.run_id, r.source_kind, r.certified, r.run_seq "
        "FROM forecast_run_points p JOIN forecast_runs r ON r.id = p.run_id "
        "WHERE p.task_id=? ORDER BY p.forecast_date ASC, r.id DESC",
        [task_id],
    )


def _run_priority(source_kind: str, switched: bool) -> int:
    """同一业务日多轮竞争时的来源优先级；未确认来源永远最低。"""
    if switched:
        order = (KIND_NEW_RUN, KIND_MIGRATED, KIND_SHADOW, KIND_UNCONFIRMED)
    else:
        order = (KIND_MIGRATED, KIND_SHADOW, KIND_NEW_RUN, KIND_UNCONFIRMED)
    return order.index(source_kind) if source_kind in order else len(order)


def _select_point_per_day(
    points: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """为报告选择逐日预测点，且绝不跨轮拼接。

    选择是“按轮”而不是“按日各自挑最优”：
    1. 先按来源优先级确定一个主运行（切换后优先已认证新运行，切换前优先
       已迁移存量，未确认来源永远排在最后）；
    2. 报告日期段内每个业务日只取该主运行的点，保证连续日期来自同一轮；
    3. 主运行缺失的日期才允许回退，但回退点会保留自身来源，且整份报告
       因此不可能被认证——历史 6 与新运行 9 永远不会被拼成一轮。
    """
    switched = is_switched()
    by_run: dict[int, list[dict[str, Any]]] = defaultdict(list)
    run_meta: dict[int, dict[str, Any]] = {}
    for point in points:
        by_run[point["run_id"]].append(point)
        run_meta[point["run_id"]] = point

    if not by_run:
        return {}

    def run_rank(run_id: int) -> tuple[int, int]:
        meta = run_meta[run_id]
        # 已认证运行优先于未认证运行，再按来源优先级、最后新 run 优先。
        certified_rank = 0 if meta["certified"] else 1
        return (
            certified_rank,
            _run_priority(meta["source_kind"], switched),
            -run_id,
        )

    primary_run_id = sorted(by_run, key=run_rank)[0]
    primary_days = {p["forecast_date"]: p for p in by_run[primary_run_id]}

    chosen: dict[str, dict[str, Any]] = {}
    all_days = sorted({p["forecast_date"] for p in points})
    for day in all_days:
        if day in primary_days:
            chosen[day] = primary_days[day]
            continue
        # 主运行缺这一天：回退到当天优先级最高的其它运行（保留来源，不认证）。
        same_day = [p for p in points if p["forecast_date"] == day]
        same_day.sort(key=lambda p: (
            0 if p["certified"] else 1,
            _run_priority(p["source_kind"], switched),
            -p["run_id"],
        ))
        chosen[day] = same_day[0]
    return chosen


def _max_certified_contiguous_span(
    segments: list[dict[str, Any]],
) -> tuple[int, int] | None:
    """找出最长的、连续日期且全部来自同一轮已认证运行的日期段下标区间。"""
    best: tuple[int, int] | None = None
    start = 0
    while start < len(segments):
        seg = segments[start]
        if not (seg["run_certified"] and seg["run_id"] is not None):
            start += 1
            continue
        end = start
        run_id = seg["run_id"]
        while end + 1 < len(segments):
            nxt = segments[end + 1]
            same_run = nxt["run_id"] == run_id and nxt["run_certified"]
            contiguous = _is_next_business_day(segments[end]["business_date"], nxt["business_date"])
            if same_run and contiguous:
                end += 1
            else:
                break
        if best is None or (end - start) > (best[1] - best[0]):
            best = (start, end)
        start = end + 1
    return best


def _is_next_business_day(day_a: str, day_b: str) -> bool:
    from datetime import date, timedelta

    try:
        return date.fromisoformat(day_b) == date.fromisoformat(day_a) + timedelta(days=1)
    except ValueError:
        return False


def generate_forecast_error_report(
    task_id: int,
    *,
    report_id: int | None = None,
    limit: int = 30,
) -> dict[str, Any]:
    """生成一份预测误差报告并落库，返回报告内容与认证结论。"""
    task = db.fetchone("SELECT * FROM forecast_tasks WHERE id=?", [task_id])
    if task is None:
        return {"ok": False, "message": f"预测任务 {task_id} 不存在"}

    metric = db.fetchone(
        "SELECT id FROM metrics WHERE metric_name LIKE ?",
        [f"%{task['target_metric']}%"],
    )
    if not metric:
        return {"ok": False, "message": "关联指标不存在"}

    actuals = _actuals_by_day(metric["id"], limit=limit)
    if not actuals:
        return {"ok": False, "message": "暂无实际指标值，无法按业务日关联"}

    chosen = _select_point_per_day(_candidate_points(task_id))
    overlap_days = sorted(set(actuals) & set(chosen))[-limit:]
    if not overlap_days:
        return {"ok": False, "message": "预测日期与实际业务日没有交集"}

    segments: list[dict[str, Any]] = []
    run_scope_set: set[int] = set()
    contains_legacy_unconfirmed = False

    for day in overlap_days:
        point = chosen[day]
        actual = actuals[day]
        predicted = float(point["predicted_value"])
        abs_error = abs(predicted - actual)
        pct_error = abs_error / max(abs(actual), 0.0001) * 100
        run_id = point["run_id"]
        run_scope_set.add(run_id)
        if point["source_kind"] == KIND_UNCONFIRMED:
            contains_legacy_unconfirmed = True
        segments.append({
            "business_date": day,
            "run_id": run_id,
            "source_kind": point["source_kind"],
            "run_certified": bool(point["certified"]),
            "predicted_value": round(predicted, 4),
            "actual_value": round(actual, 4),
            "abs_error": round(abs_error, 4),
            "pct_error": round(pct_error, 4),
        })

    # 已认证的连续日期只能来自同一轮完整预测；其余日期一律不认证。
    certified_span = _max_certified_contiguous_span(segments)
    certified_indices = set(range(certified_span[0], certified_span[1] + 1)) if certified_span else set()
    for idx, seg in enumerate(segments):
        seg["certified_in_report"] = idx in certified_indices

    # 报告整体只有在“所有段都落在同一轮已认证运行”时才算已认证报告。
    certified_run_ids = {s["run_id"] for s in segments if s["run_certified"]}
    report_certified = (
        len(certified_run_ids) == 1
        and len(run_scope_set) == 1
        and not contains_legacy_unconfirmed
        and certified_span is not None
        and certified_span == (0, len(segments) - 1)
    )

    mape = sum(s["pct_error"] for s in segments) / len(segments)
    run_scope = _build_run_scope(sorted(run_scope_set), task_id)

    report_id_out = db.insert("forecast_error_reports", {
        "report_id": report_id,
        "task_id": task_id,
        "business_date_from": segments[0]["business_date"],
        "business_date_to": segments[-1]["business_date"],
        "run_scope": json.dumps(run_scope, ensure_ascii=False),
        "contains_legacy_unconfirmed": 1 if contains_legacy_unconfirmed else 0,
        "certified": 1 if report_certified else 0,
        "mape_pct": round(mape, 4),
    })

    for seg in segments:
        db.insert("forecast_error_segments", {
            "error_report_id": report_id_out,
            "task_id": task_id,
            "business_date": seg["business_date"],
            "run_id": seg["run_id"],
            "source_kind": seg["source_kind"],
            "run_certified": 1 if seg["run_certified"] else 0,
            "predicted_value": seg["predicted_value"],
            "actual_value": seg["actual_value"],
            "abs_error": seg["abs_error"],
            "pct_error": seg["pct_error"],
        })

    return {
        "ok": True,
        "error_report_id": report_id_out,
        "task_id": task_id,
        "phase_when_generated": get_phase(),
        "business_date_from": segments[0]["business_date"],
        "business_date_to": segments[-1]["business_date"],
        "run_scope": run_scope,
        "contains_legacy_unconfirmed": contains_legacy_unconfirmed,
        "certified": report_certified,
        "certified_contiguous_span": (
            [segments[certified_span[0]]["business_date"],
             segments[certified_span[1]]["business_date"]]
            if certified_span else None
        ),
        "mape_pct": round(mape, 4),
        "segments": segments,
    }


def _build_run_scope(run_ids: list[int], task_id: int) -> list[dict[str, Any]]:
    scope: list[dict[str, Any]] = []
    for run_id in run_ids:
        run = db.fetchone("SELECT * FROM forecast_runs WHERE id=?", [run_id])
        if run is None:
            continue
        point_count = db.count("forecast_run_points", "run_id=?", [run_id])
        scope.append({
            "run_id": run_id,
            "task_id": task_id,
            "run_seq": run["run_seq"],
            "source_kind": run["source_kind"],
            "certified": bool(run["certified"]),
            "migration_batch": run["migration_batch"],
            "source_note": run["source_note"],
            "point_count": point_count,
        })
    return scope


def audit_error_report(error_report_id: int) -> dict[str, Any]:
    """切换后回查一份报告当时采用的逐日记录与真实数据范围。

    重新按报告冻结的 run_id / business_date 取数，验证：
    1. 每个误差段的预测点当前仍能追溯到同一轮运行；
    2. 报告里被认证的连续日期确实只来自一轮已认证完整预测；
    3. 是否混入未确认来源的老数据（例如历史 6 与新运行 9 的拼接）。
    """
    report = db.fetchone(
        "SELECT * FROM forecast_error_reports WHERE id=?", [error_report_id]
    )
    if report is None:
        return {"ok": False, "message": f"误差报告 {error_report_id} 不存在"}

    frozen_segments = db.fetchall(
        "SELECT * FROM forecast_error_segments WHERE error_report_id=? ORDER BY business_date",
        [error_report_id],
    )

    findings: list[dict[str, Any]] = []
    mixed_runs: set[int | None] = set()
    unconfirmed_dates: list[str] = []
    traceable = True

    for seg in frozen_segments:
        run_id = seg["run_id"]
        mixed_runs.add(run_id)
        point = db.fetchone(
            "SELECT p.*, r.source_kind, r.certified FROM forecast_run_points p "
            "JOIN forecast_runs r ON r.id = p.run_id "
            "WHERE p.run_id=? AND p.forecast_date=?",
            [run_id, seg["business_date"]],
        )
        if point is None:
            traceable = False
            findings.append({
                "business_date": seg["business_date"],
                "status": "trace_missing",
                "message": "冻结的预测点已无法在迁移结果集中追溯",
            })
            continue

        value_match = abs(float(point["predicted_value"]) - float(seg["predicted_value"])) < 1e-6
        if point["source_kind"] == KIND_UNCONFIRMED or not point["certified"]:
            unconfirmed_dates.append(seg["business_date"])
        findings.append({
            "business_date": seg["business_date"],
            "run_id": run_id,
            "source_kind": point["source_kind"],
            "run_certified": bool(point["certified"]),
            "frozen_predicted_value": seg["predicted_value"],
            "current_predicted_value": point["predicted_value"],
            "value_unchanged": value_match,
            "status": "ok" if value_match else "value_changed",
        })

    distinct_certified_runs = {
        s["run_id"] for s in frozen_segments if s["run_certified"]
    }
    can_form_certified_round = (
        len(distinct_certified_runs) == 1
        and len(mixed_runs) == 1
        and not unconfirmed_dates
        and _frozen_dates_contiguous(frozen_segments)
    )

    return {
        "ok": True,
        "error_report_id": error_report_id,
        "task_id": report["task_id"],
        "generated_phase": report["generated_at"],
        "frozen_date_range": [report["business_date_from"], report["business_date_to"]],
        "stored_run_scope": json.loads(report["run_scope"]),
        "report_was_certified": bool(report["certified"]),
        "contains_legacy_unconfirmed": bool(report["contains_legacy_unconfirmed"]),
        "all_points_traceable": traceable,
        "unconfirmed_dates": unconfirmed_dates,
        "distinct_run_ids": sorted(r for r in mixed_runs if r is not None),
        "can_form_certified_round": can_form_certified_round,
        "findings": findings,
    }


def _frozen_dates_contiguous(segments: list[dict[str, Any]]) -> bool:
    dates = [s["business_date"] for s in segments]
    return all(_is_next_business_day(a, b) for a, b in zip(dates, dates[1:]))


def list_error_reports(task_id: int | None = None) -> list[dict[str, Any]]:
    if task_id is None:
        return db.fetchall(
            "SELECT * FROM forecast_error_reports ORDER BY id DESC"
        )
    return db.fetchall(
        "SELECT * FROM forecast_error_reports WHERE task_id=? ORDER BY id DESC",
        [task_id],
    )
