"""预测结果按运行存储迁移的端到端验收测试。

覆盖需求场景：
1. 旧版逐日期预测迁移到按运行区分的结果集，区间/置信度可读取，迁移期可生成误差报告；
2. 数据报告从迁移后结果集读取预测与实际值，按业务日关联，记录运行范围与老数据标记；
3. 新签发报告里连续日期只能来自一轮完整预测，存量来源不明不能拼成已认证轮次；
4. 重复迁移/重复运行不添出同一份历史预测；
5. 迁移读到一半时新一轮预测写入（影子运行），切换前后报告回查真实数据范围；
6. 历史两天预测 6/8、新运行 7/9：迁移中的报告不能拼出 6 和 9，老记录无法
   确认同一轮时按未确认来源追检，不能变成已认证一轮。
"""
from __future__ import annotations

import os
import tempfile
import unittest
from datetime import date, timedelta


class ForecastMigrationTestBase(unittest.TestCase):
    """每个用例使用独立临时 SQLite 库，互不污染。"""

    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        os.environ["CC10045_DB_DIR"] = self.tmpdir.name
        # 延迟导入，确保 AnalyticsDB 单例绑定到本用例的临时库。
        from pathlib import Path
        from app.database import AnalyticsDB
        import app.database as database_mod
        # DB_DIR/DB_PATH 在模块导入时已按环境变量定型，这里显式重定向。
        database_mod.DB_DIR = Path(self.tmpdir.name)
        database_mod.DB_PATH = database_mod.DB_DIR / "cc10045.db"
        AnalyticsDB._instance = None
        self.db = AnalyticsDB.get()
        database_mod.db = self.db
        self._patch_singletons()
        self._seed_task_and_metric()

    def _patch_singletons(self) -> None:
        import app.forecast_errors as errors_mod
        import app.forecast_migration as migration_mod
        import app.pipeline as pipeline_mod
        import app.services.forecast_model as fm_service_mod
        import app.services.data_report as dr_service_mod
        for mod in (errors_mod, migration_mod, pipeline_mod,
                    fm_service_mod, dr_service_mod):
            mod.db = self.db

    def tearDown(self) -> None:
        from app.database import AnalyticsDB
        AnalyticsDB._instance = None
        self.tmpdir.cleanup()

    def _seed_task_and_metric(self, period_days: int = 2) -> None:
        self.db.insert("metrics", {
            "metric_name": "验收指标(DAU)", "metric_type": "gauge",
            "source_module": "forecast_model", "calibration_version": 1, "unit": "人",
        })
        self.metric_id = self.db.fetchone("SELECT id FROM metrics")["id"]
        self.db.insert("forecast_tasks", {
            "task_name": "验收预测任务", "target_metric": "验收指标",
            "forecast_period_days": period_days, "forecast_method": "moving_average",
            "baseline_days": 10, "status": "待执行",
        })
        self.task_id = self.db.fetchone("SELECT id FROM forecast_tasks")["id"]

    def _insert_legacy(self, rows: list[tuple[str, float, str]]) -> None:
        """写旧版 forecast_results 行：(forecast_date, predicted, computed_at)。"""
        for forecast_date, value, computed_at in rows:
            self.db.insert("forecast_results", {
                "task_id": self.task_id,
                "forecast_date": forecast_date,
                "predicted_value": value,
                "lower_bound": value - 1.0,
                "upper_bound": value + 1.0,
                "confidence": 0.9,
                "computed_at": computed_at,
            })

    def _insert_actuals(self, day_values: dict[str, float]) -> None:
        for day, value in day_values.items():
            self.db.insert("metric_values", {
                "metric_id": self.metric_id,
                "snapshot_value": value,
                "snapshot_time": f"{day}T12:00:00",
                "calibration_version": 1,
            })

    @staticmethod
    def _day(offset: int) -> str:
        return (date(2026, 11, 20) + timedelta(days=offset)).isoformat()


class MigrationStorageTests(ForecastMigrationTestBase):
    def test_legacy_rows_migrate_into_runs_with_bounds_and_confidence(self) -> None:
        from app.forecast_migration import migrate_all_legacy, is_switched
        self._insert_legacy([
            (self._day(0), 6.0, "2026-11-18T08:00:00"),
            (self._day(1), 8.0, "2026-11-18T08:00:00"),
        ])
        summary = migrate_all_legacy()
        self.assertEqual(summary["migrated_rows"], 2)
        self.assertEqual(summary["remaining_legacy"], 0)

        runs = self.db.fetchall("SELECT * FROM forecast_runs WHERE task_id=?", [self.task_id])
        self.assertEqual(len(runs), 1)
        run = runs[0]
        self.assertEqual(run["source_kind"], "migrated_legacy")
        self.assertEqual(run["certified"], 1)

        points = self.db.fetchall(
            "SELECT * FROM forecast_run_points WHERE run_id=? ORDER BY forecast_date",
            [run["id"]],
        )
        self.assertEqual([p["predicted_value"] for p in points], [6.0, 8.0])
        # 存量区间与置信度必须保持可读取。
        self.assertEqual(points[0]["lower_bound"], 5.0)
        self.assertEqual(points[0]["upper_bound"], 7.0)
        self.assertEqual(points[0]["confidence"], 0.9)
        self.assertIsNotNone(points[0]["legacy_result_id"])
        self.assertFalse(is_switched())

    def test_repeated_migration_is_idempotent(self) -> None:
        from app.forecast_migration import migrate_all_legacy, migrate_legacy_batch
        self._insert_legacy([
            (self._day(0), 6.0, "2026-11-18T08:00:00"),
            (self._day(1), 8.0, "2026-11-18T08:00:00"),
        ])
        first = migrate_all_legacy()
        second = migrate_legacy_batch()
        third = migrate_all_legacy()
        self.assertEqual(first["migrated_rows"], 2)
        self.assertEqual(second["migrated_rows"], 0)
        self.assertEqual(third["migrated_rows"], 0)
        self.assertEqual(self.db.count("forecast_run_points"), 2)
        self.assertEqual(self.db.count("forecast_migration_ledger"), 2)
        self.assertEqual(self.db.count("forecast_runs"), 1)

    def test_multiple_legacy_groups_become_unconfirmed_not_certified(self) -> None:
        from app.forecast_migration import migrate_all_legacy, list_unconfirmed_runs
        # 同一任务两组 computed_at：旧表被多轮覆盖过，来源不明确。
        self._insert_legacy([
            (self._day(0), 6.0, "2026-11-18T08:00:00"),
            (self._day(1), 8.0, "2026-11-18T08:00:00"),
            (self._day(0), 7.0, "2026-11-19T08:00:00"),
            (self._day(1), 9.0, "2026-11-19T08:00:00"),
        ])
        summary = migrate_all_legacy()
        self.assertEqual(summary["unconfirmed_runs"], 2)
        runs = self.db.fetchall("SELECT * FROM forecast_runs ORDER BY id")
        self.assertTrue(all(r["source_kind"] == "legacy_unconfirmed" for r in runs))
        self.assertTrue(all(r["certified"] == 0 for r in runs))
        self.assertEqual(len(list_unconfirmed_runs(self.task_id)), 2)

    def test_repeated_forecast_runs_keep_separate_history(self) -> None:
        from app.forecast_migration import switch_to_runs
        from app.services.forecast_model import ForecastModelService
        # 提供足够基线快照让预测管道可计算。
        for i in range(6):
            self._insert_actuals({self._day(i - 10): 10.0 + i})
        switch_to_runs()
        svc = ForecastModelService()
        r1 = svc.run_forecast(self.task_id)
        r2 = svc.run_forecast(self.task_id)
        self.assertNotEqual(r1["run_id"], r2["run_id"])
        self.assertEqual(r1["source_kind"], "new_run")
        runs = svc.list_runs(self.task_id)
        self.assertEqual(len(runs), 2)
        # 两轮各自独立存点，总量等于两轮之和，没有覆盖。
        self.assertEqual(self.db.count("forecast_run_points", "task_id=?", [self.task_id]), 4)


class ErrorReportTests(ForecastMigrationTestBase):
    def test_report_joins_forecast_and_actual_by_business_day(self) -> None:
        from app.forecast_migration import migrate_all_legacy
        from app.forecast_errors import generate_forecast_error_report
        self._insert_legacy([
            (self._day(0), 6.0, "2026-11-18T08:00:00"),
            (self._day(1), 8.0, "2026-11-18T08:00:00"),
        ])
        self._insert_actuals({self._day(0): 5.0, self._day(1): 10.0})
        migrate_all_legacy()

        report = generate_forecast_error_report(self.task_id)
        self.assertTrue(report["ok"])
        self.assertEqual(len(report["segments"]), 2)
        self.assertEqual(report["business_date_from"], self._day(0))
        self.assertEqual(report["business_date_to"], self._day(1))
        # 运行范围被写明，且只有一轮已认证迁移运行。
        self.assertEqual(len(report["run_scope"]), 1)
        self.assertEqual(report["run_scope"][0]["source_kind"], "migrated_legacy")
        self.assertFalse(report["contains_legacy_unconfirmed"])
        self.assertTrue(report["certified"])
        # 误差段：|6-5|/5=20%，|8-10|/10=20%。
        self.assertEqual(report["segments"][0]["pct_error"], 20.0)
        self.assertEqual(report["segments"][1]["pct_error"], 20.0)

    def test_unconfirmed_legacy_flagged_and_never_certified_round(self) -> None:
        from app.forecast_migration import migrate_all_legacy
        from app.forecast_errors import generate_forecast_error_report
        self._insert_legacy([
            (self._day(0), 6.0, "2026-11-18T08:00:00"),
            (self._day(1), 8.0, "2026-11-18T08:00:00"),
            (self._day(0), 7.0, "2026-11-19T08:00:00"),
            (self._day(1), 9.0, "2026-11-19T08:00:00"),
        ])
        self._insert_actuals({self._day(0): 5.0, self._day(1): 10.0})
        migrate_all_legacy()

        report = generate_forecast_error_report(self.task_id)
        self.assertTrue(report["contains_legacy_unconfirmed"])
        self.assertFalse(report["certified"])
        self.assertIsNone(report["certified_contiguous_span"])
        for seg in report["segments"]:
            self.assertFalse(seg["certified_in_report"])
            self.assertEqual(seg["source_kind"], "legacy_unconfirmed")

    def test_certify_run_requires_contiguous_complete_points(self) -> None:
        from app.forecast_migration import migrate_all_legacy, certify_run
        self._insert_legacy([
            (self._day(0), 6.0, "2026-11-18T08:00:00"),
            (self._day(2), 8.0, "2026-11-18T08:00:00"),  # 日期不连续
        ])
        migrate_all_legacy()
        run = self.db.fetchone("SELECT id FROM forecast_runs")
        result = certify_run(run["id"])
        self.assertFalse(result["ok"])
        self.assertEqual(self.db.fetchone("SELECT certified FROM forecast_runs WHERE id=?",
                                          [run["id"]])["certified"], 0)


class HalfMigrationInterleavingTests(ForecastMigrationTestBase):
    """迁移读到一半、新一轮预测在切换前写入的交错场景。"""

    def _seed_legacy_two_rounds(self) -> None:
        # 历史轮：6、8；新运行（旧表后期覆盖）：7、9，不同 computed_at。
        self._insert_legacy([
            (self._day(0), 6.0, "2026-11-18T08:00:00"),
            (self._day(1), 8.0, "2026-11-18T08:00:00"),
            (self._day(0), 7.0, "2026-11-19T08:00:00"),
            (self._day(1), 9.0, "2026-11-19T08:00:00"),
        ])
        self._insert_actuals({self._day(0): 5.0, self._day(1): 10.0})

    def test_shadow_run_during_migration_is_kept_with_source(self) -> None:
        from app.forecast_migration import (
            get_phase, migrate_legacy_batch, switch_to_runs,
        )
        from app.services.forecast_model import ForecastModelService
        self._seed_legacy_two_rounds()
        for i in range(6):
            self._insert_actuals({self._day(i - 10): 10.0 + i})

        # 只迁移一批（本数据一次可搬完，但阶段仍停留在 in_progress）。
        migrate_legacy_batch()
        self.assertEqual(get_phase(), "in_progress")

        svc = ForecastModelService()
        shadow = svc.run_forecast(self.task_id)
        self.assertEqual(shadow["source_kind"], "pre_switch_shadow")
        self.assertFalse(shadow["certified"])
        # 切换前写的预测数据必须留得下来源。
        run_row = self.db.fetchone("SELECT * FROM forecast_runs WHERE id=?",
                                   [shadow["run_id"]])
        self.assertIn("迁移切换期间", run_row["source_note"])

        # 迁移期间仍可生成误差报告。
        report = svc.generate_error_report(self.task_id)
        self.assertTrue(report["ok"])
        self.assertEqual(report["phase_when_generated"], "in_progress")

        # 切换：影子运行完整应转正，存量两组保持未确认。
        switched = switch_to_runs()
        self.assertEqual(switched["phase"], "switched")
        self.assertEqual(switched["shadow_promoted"], 1)
        self.db.fetchone("SELECT certified FROM forecast_runs WHERE id=?",
                         [shadow["run_id"]])
        self.assertEqual(
            self.db.fetchone("SELECT source_kind, certified FROM forecast_runs WHERE id=?",
                             [shadow["run_id"]])["source_kind"],
            "new_run",
        )

    def test_mid_migration_report_cannot_stitch_six_and_nine(self) -> None:
        """核心验收：迁移中提交的报告，切换后回查逐日记录，不能拼出 6 和 9。"""
        from app.forecast_migration import migrate_all_legacy, switch_to_runs
        from app.forecast_errors import (
            generate_forecast_error_report, audit_error_report,
        )
        self._seed_legacy_two_rounds()

        # 迁移完成但尚未切换：两组旧存量都无法确认同一轮。
        migrate_all_legacy()
        report = generate_forecast_error_report(self.task_id)
        report_id = report["error_report_id"]

        # 报告里每个业务日只能选一个预测点，绝不允许跨轮拼接。
        day_values = {(s["business_date"], s["run_id"]): s["predicted_value"]
                      for s in report["segments"]}
        day0_runs = {rid for (d, rid) in day_values if d == self._day(0)}
        day1_runs = {rid for (d, rid) in day_values if d == self._day(1)}
        self.assertEqual(len(day0_runs), 1)
        self.assertEqual(len(day1_runs), 1)
        # 同一报告里两天要么同属历史轮(6,8)，要么同属后一轮(7,9)，不会是 6+9。
        picked = [s["predicted_value"] for s in report["segments"]]
        self.assertNotEqual(picked, [6.0, 9.0])
        self.assertIn(picked, ([6.0, 8.0], [7.0, 9.0]))
        # 来源不明 => 整份报告不能认证为一轮已确认预测。
        self.assertFalse(report["certified"])
        self.assertTrue(report["contains_legacy_unconfirmed"])

        # 切换后回查这份迁移中报告曾用到的真实数据范围。
        switch_to_runs()
        audit = audit_error_report(report_id)
        self.assertTrue(audit["ok"])
        self.assertTrue(audit["all_points_traceable"])
        self.assertEqual(audit["frozen_date_range"],
                         [self._day(0), self._day(1)])
        # 未确认老数据不能在回查时被洗白成一轮已认证预测。
        self.assertFalse(audit["can_form_certified_round"])
        self.assertEqual(set(audit["unconfirmed_dates"]),
                         {self._day(0), self._day(1)})
        self.assertEqual(len(audit["stored_run_scope"]), 1)

    def test_audit_detects_mixed_runs_as_uncertifiable(self) -> None:
        """人为构造未认证 run 的冻结段，回查必须拒绝认证（防御性校验）。"""
        from app.forecast_migration import migrate_legacy_batch
        from app.forecast_errors import audit_error_report
        # 日期不连续 => 迁移后该 run 保持未认证（legacy_unconfirmed）。
        self._insert_legacy([
            (self._day(0), 6.0, "2026-11-18T08:00:00"),
            (self._day(2), 8.0, "2026-11-18T08:00:00"),
        ])
        self._insert_actuals({self._day(0): 5.0, self._day(2): 10.0})
        migrate_legacy_batch()
        run = self.db.fetchone("SELECT id, certified FROM forecast_runs")
        self.assertEqual(run["certified"], 0)
        # 手工伪造一份谎报已认证的冻结报告，审计应识别其不成立。
        report_id = self.db.insert("forecast_error_reports", {
            "task_id": self.task_id,
            "business_date_from": self._day(0),
            "business_date_to": self._day(2),
            "run_scope": "[]",
            "contains_legacy_unconfirmed": 1,
            "certified": 1,  # 谎报已认证
            "mape_pct": 1.0,
        })
        for day, pred, actual in ((self._day(0), 6.0, 5.0), (self._day(2), 8.0, 10.0)):
            self.db.insert("forecast_error_segments", {
                "error_report_id": report_id, "task_id": self.task_id,
                "business_date": day, "run_id": run["id"],
                "source_kind": "legacy_unconfirmed", "run_certified": 0,
                "predicted_value": pred, "actual_value": actual,
                "abs_error": abs(pred - actual), "pct_error": 20.0,
            })
        audit = audit_error_report(report_id)
        self.assertFalse(audit["can_form_certified_round"])
        self.assertTrue(audit["unconfirmed_dates"])


class PostSwitchNewRunReportTests(ForecastMigrationTestBase):
    def test_new_run_report_after_switch_is_certified_single_round(self) -> None:
        from app.forecast_migration import switch_to_runs
        from app.services.forecast_model import ForecastModelService
        # 历史基线快照（预测管道需要 >=5 条）。
        for i in range(6):
            self._insert_actuals({self._day(i - 10): 10.0 + i})
        switch_to_runs()
        svc = ForecastModelService()
        run = svc.run_forecast(self.task_id)
        self.assertTrue(run["certified"])

        # 新运行的逐日点日期由管道按“当前日期+1/+2天”生成，
        # 在同样的业务日上补实际值，才能按业务日关联出误差段。
        points = svc.forecast_data(self.task_id, run_id=run["run_id"])
        self.assertEqual(len(points), 2)
        for idx, point in enumerate(points):
            self._insert_actuals({point["forecast_date"]: float(point["predicted_value"]) + 1.0})

        report = svc.generate_error_report(self.task_id)
        self.assertTrue(report["ok"])
        self.assertFalse(report["contains_legacy_unconfirmed"])
        # 连续日期全部来自同一轮新运行 => 可认证。
        self.assertTrue(report["certified"])
        scope_kinds = {s["source_kind"] for s in report["run_scope"]}
        self.assertEqual(scope_kinds, {"new_run"})
        self.assertEqual(len({s["run_id"] for s in report["segments"]}), 1)


class PhaseAndStitchingTests(ForecastMigrationTestBase):
    def test_pending_legacy_after_switch_flips_phase_back(self) -> None:
        """切换后又出现未迁移老数据时，阶段必须回到 in_progress。"""
        from app.forecast_migration import (
            get_phase, migrate_legacy_batch, set_phase, PHASE_SWITCHED,
        )
        set_phase(PHASE_SWITCHED)
        self._insert_legacy([
            (self._day(0), 6.0, "2026-11-18T08:00:00"),
            (self._day(1), 8.0, "2026-11-18T08:00:00"),
        ])
        migrate_legacy_batch()
        self.assertEqual(get_phase(), "in_progress")

    def test_report_never_stitches_across_runs(self) -> None:
        """主运行缺失日期的回退点保留来源，报告不能认证、不能跨轮拼接。"""
        from app.forecast_migration import migrate_all_legacy, switch_to_runs
        from app.services.forecast_model import ForecastModelService
        # 单一来源的存量轮（6,8），可认证迁移。
        self._insert_legacy([
            (self._day(0), 6.0, "2026-11-18T08:00:00"),
            (self._day(1), 8.0, "2026-11-18T08:00:00"),
        ])
        self._insert_actuals({self._day(0): 5.0, self._day(1): 10.0})
        for i in range(6):
            self._insert_actuals({self._day(i - 10): 10.0 + i})
        migrate_all_legacy()
        switch_to_runs()

        svc = ForecastModelService()
        run = svc.run_forecast(self.task_id)
        points = svc.forecast_data(self.task_id, run_id=run["run_id"])
        # 只在新运行的第一个业务日补实际值，第二个业务日无实际值，
        # 报告日期段因此同时包含存量轮日期与新运行日期。
        first_new_day = points[0]["forecast_date"]
        self._insert_actuals({first_new_day: float(points[0]["predicted_value"]) + 1.0})

        report = svc.generate_error_report(self.task_id)
        run_ids = {s["run_id"] for s in report["segments"]}
        self.assertGreater(len(run_ids), 1)  # 确实跨了轮次
        self.assertFalse(report["certified"])  # 跨轮 => 绝不认证
        # 每个业务日仍只对应一个预测点，没有任何日期被重复计数。
        days = [s["business_date"] for s in report["segments"]]
        self.assertEqual(len(days), len(set(days)))
        # 回退来的存量段保留其真实来源标记。
        legacy_segments = [s for s in report["segments"]
                           if s["source_kind"] == "migrated_legacy"]
        self.assertTrue(legacy_segments)


if __name__ == "__main__":
    unittest.main(verbosity=2)
