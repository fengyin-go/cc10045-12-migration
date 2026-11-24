"""预测模型领域服务：基于历史指标数据执行趋势和时间序列预测。

真实数据流：定义预测任务（指标/周期/方法/基线天数）
→ pipeline.compute_forecast 执行 SES/移动平均/线性趋势预测
→ 按单次运行写入 forecast_runs / forecast_run_points
→ 迁移模块搬运旧版 forecast_results → 下游误差报告消费者。
"""
from __future__ import annotations

from typing import Any

from app.database import db
from app.forecast_errors import audit_error_report, generate_forecast_error_report
from app.forecast_migration import (
    certify_run, list_unconfirmed_runs, migrate_all_legacy,
    migrate_legacy_batch, migration_status, switch_to_runs,
)
from app.pipeline import compute_forecast
from app.services.base import BaseModuleService


class ForecastModelService(BaseModuleService):
    MODULE = "forecast_model"
    ENTITY = "预测任务"
    OPERATION_TERM = "预测"
    LIST_FIELDS = ["任务名称", "预测指标", "预测周期", "预测方式", "历史基线天数"]
    REQUIRED_FIELDS = ["任务名称", "预测指标", "预测周期"]
    STATUS_ORDER = ["待执行", "执行中", "已完成", "执行失败"]
    ACTION_RULES = {"启动预测": "已完成", "取消预测": "执行失败"}
    NEGATIVE_ACTIONS = []

    def run_forecast(self, task_id: int) -> dict[str, Any]:
        """执行预测任务：每次执行产生一轮独立 run，不覆盖历史运行。"""
        task = db.fetchone("SELECT * FROM forecast_tasks WHERE id=?", [task_id])
        if task is None:
            return {"ok": False, "message": f"预测任务 {task_id} 不存在"}

        db.update("forecast_tasks", {"status": "执行中"}, "id=?", [task_id])
        results = compute_forecast(dict(task))
        db.update("forecast_tasks", {"status": "已完成"}, "id=?", [task_id])

        run_row = db.fetchone(
            "SELECT * FROM forecast_runs WHERE task_id=? ORDER BY id DESC LIMIT 1",
            [task_id],
        )
        return {
            "ok": True,
            "task_name": task["task_name"],
            "method": task["forecast_method"],
            "run_id": run_row["id"] if run_row else None,
            "source_kind": run_row["source_kind"] if run_row else None,
            "certified": bool(run_row["certified"]) if run_row else False,
            "forecasts": results,
        }

    def list_runs(self, task_id: int) -> list[dict[str, Any]]:
        """列出任务的全部预测运行（含迁移来的存量轮次与未确认来源）。"""
        runs = db.fetchall(
            "SELECT * FROM forecast_runs WHERE task_id=? ORDER BY id DESC",
            [task_id],
        )
        for run in runs:
            run["point_count"] = db.count("forecast_run_points", "run_id=?", [run["id"]])
            run["certified"] = bool(run["certified"])
        return runs

    def forecast_data(self, task_id: int, run_id: int | None = None) -> list[dict[str, Any]]:
        """获取预测结果：默认取最新一轮，可指定 run_id 回查任意一轮。

        区间（lower/upper bound）与置信度 confidence 随逐日点一并读出，
        迁移后的存量数据保持同样可读。
        """
        if run_id is None:
            latest = db.fetchone(
                "SELECT id FROM forecast_runs WHERE task_id=? ORDER BY id DESC LIMIT 1",
                [task_id],
            )
            if latest is None:
                return []
            run_id = latest["id"]
        return db.fetchall(
            "SELECT p.*, r.source_kind, r.certified, r.run_seq "
            "FROM forecast_run_points p JOIN forecast_runs r ON r.id = p.run_id "
            "WHERE p.run_id=? ORDER BY p.forecast_date ASC",
            [run_id],
        )

    def evaluate_accuracy(self, task_id: int) -> dict[str, Any]:
        """评估预测准确率：委托误差报告管道，按业务日关联预测与实际值。"""
        return generate_forecast_error_report(task_id)

    def generate_error_report(self, task_id: int, report_id: int | None = None) -> dict[str, Any]:
        """签发一份预测误差报告（含运行范围与老数据迁移标记）。"""
        return generate_forecast_error_report(task_id, report_id=report_id)

    def audit_error_report(self, error_report_id: int) -> dict[str, Any]:
        """切换后回查报告曾用到的逐日记录与真实运行范围。"""
        return audit_error_report(error_report_id)

    # ---- 迁移运维入口 ----
    def migrate_batch(self, batch_size: int = 500) -> dict[str, Any]:
        return migrate_legacy_batch(batch_size=batch_size)

    def migrate_all(self) -> dict[str, Any]:
        return migrate_all_legacy()

    def switch_storage(self) -> dict[str, Any]:
        return switch_to_runs()

    def migration_status(self) -> dict[str, Any]:
        return migration_status()

    def unconfirmed_runs(self, task_id: int | None = None) -> list[dict[str, Any]]:
        return list_unconfirmed_runs(task_id)

    def certify_run(self, run_id: int, note: str | None = None) -> dict[str, Any]:
        return certify_run(run_id, note or "运营补迁核对后认证")

    def domain_snapshot(self) -> dict[str, Any]:
        snapshot = super().domain_snapshot()
        snapshot["total_tasks"] = db.count("forecast_tasks")
        snapshot["total_forecast_points"] = db.count("forecast_run_points")
        snapshot["total_runs"] = db.count("forecast_runs")
        snapshot["migration"] = migration_status()
        return snapshot
