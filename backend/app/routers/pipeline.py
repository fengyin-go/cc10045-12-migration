"""数据分析管道路由：事件采集、聚合执行、指标快照、分群计算等核心操作入口。"""
from __future__ import annotations

from fastapi import APIRouter

from app.database import db
from app.pipeline import (
    check_alert, compute_aggregation, compute_cohort_comparison,
    compute_forecast, compute_funnel, compute_metric_snapshot,
    compute_retention, compute_segment, generate_report,
    ingest_events, precompute_cube, seed_real_data,
)
from app.services import service_for

router = APIRouter(prefix="/api/pipeline", tags=["分析管线"])


@router.post("/seed")
def api_seed_real_data() -> dict:
    """初始化全部种子数据：事件、指标、聚合窗口、分群、漏斗等。"""
    counts = seed_real_data()
    return {"ok": True, "counts": counts}


@router.post("/events/ingest")
def api_ingest_events() -> dict:
    """对所有已启用的事件定义批量摄入模拟事件。"""
    svc = service_for("event_tracker")
    result = svc.ingest_all_enabled()
    return result


@router.post("/aggregation/run-all")
def api_run_all_aggregations() -> dict:
    """对所有已启用聚合窗口执行计算。"""
    svc = service_for("realtime_agg")
    result = svc.run_all_active()
    return result


@router.post("/aggregation/{window_id}/run")
def api_run_aggregation(window_id: int) -> dict:
    """对指定聚合窗口执行单次计算。"""
    svc = service_for("realtime_agg")
    return svc.run_aggregation(window_id)


@router.post("/metrics/compute-all")
def api_compute_all_metrics() -> dict:
    """对所有指标执行快照计算。"""
    svc = service_for("metric_store")
    return svc.compute_all()


@router.post("/metrics/{metric_id}/snapshot")
def api_compute_snapshot(metric_id: int) -> dict:
    """对指定指标执行快照计算。"""
    svc = service_for("metric_store")
    return svc.compute_snapshot(metric_id)


@router.post("/segments/{segment_id}/compute")
def api_compute_segment(segment_id: int) -> dict:
    """对指定分群执行计算。"""
    svc = service_for("segment_engine")
    return svc.run_computation(segment_id)


@router.post("/funnels/{funnel_id}/compute")
def api_compute_funnel(funnel_id: int) -> dict:
    """对指定漏斗执行分析计算。"""
    svc = service_for("funnel_analyzer")
    return svc.run_computation(funnel_id)


@router.post("/retention/{task_id}/compute")
def api_compute_retention(task_id: int) -> dict:
    """对指定留存任务执行计算。"""
    svc = service_for("retention_analysis")
    return svc.run_computation(task_id)


@router.post("/forecast/{task_id}/compute")
def api_compute_forecast(task_id: int) -> dict:
    """对指定预测任务执行计算（每次执行生成一轮独立 run）。"""
    svc = service_for("forecast_model")
    return svc.run_forecast(task_id)


@router.get("/forecast/{task_id}/runs")
def api_forecast_runs(task_id: int) -> dict:
    """列出预测任务的全部运行（含迁移轮次与未确认来源）。"""
    svc = service_for("forecast_model")
    return {"ok": True, "task_id": task_id, "items": svc.list_runs(task_id)}


@router.get("/forecast/{task_id}/runs/{run_id}/points")
def api_forecast_run_points(task_id: int, run_id: int) -> dict:
    """读取某一轮运行的逐日预测点（含区间与置信度）。"""
    svc = service_for("forecast_model")
    return {"ok": True, "task_id": task_id, "run_id": run_id,
            "items": svc.forecast_data(task_id, run_id=run_id)}


@router.get("/forecast/migration/status")
def api_forecast_migration_status() -> dict:
    """查看预测存储迁移进度与各来源运行计数。"""
    return {"ok": True, **service_for("forecast_model").migration_status()}


@router.post("/forecast/migration/migrate-batch")
def api_forecast_migrate_batch(batch_size: int = 500) -> dict:
    """搬运一批旧版逐日预测到按运行结果集（幂等，可重复执行）。"""
    return service_for("forecast_model").migrate_batch(batch_size=batch_size)


@router.post("/forecast/migration/migrate-all")
def api_forecast_migrate_all() -> dict:
    """搬运全部旧版预测行；重复执行不会再造同一份历史预测。"""
    return service_for("forecast_model").migrate_all()


@router.post("/forecast/migration/switch")
def api_forecast_switch() -> dict:
    """完成迁移切换：补迁旧行并把切换前影子运行按完整性转正。"""
    return service_for("forecast_model").switch_storage()


@router.get("/forecast/migration/unconfirmed")
def api_forecast_unconfirmed(task_id: int | None = None) -> dict:
    """列出未认证运行，供按未确认来源追检。"""
    svc = service_for("forecast_model")
    return {"ok": True, "items": svc.unconfirmed_runs(task_id)}


@router.post("/forecast/runs/{run_id}/certify")
def api_forecast_certify_run(run_id: int, note: str | None = None) -> dict:
    """补迁核对后显式认证一轮完整预测。"""
    return service_for("forecast_model").certify_run(run_id, note)


@router.post("/forecast/{task_id}/error-report")
def api_forecast_error_report(task_id: int, report_id: int | None = None) -> dict:
    """生成预测误差报告：按业务日关联预测与实际，记录运行范围与迁移标记。"""
    return service_for("forecast_model").generate_error_report(task_id, report_id)


@router.get("/forecast/error-reports")
def api_forecast_error_reports(task_id: int | None = None) -> dict:
    """列出预测误差报告。"""
    from app.forecast_errors import list_error_reports
    return {"ok": True, "items": list_error_reports(task_id)}


@router.get("/forecast/error-reports/{error_report_id}/audit")
def api_forecast_error_report_audit(error_report_id: int) -> dict:
    """切换后回查误差报告曾用到的逐日记录与真实运行范围。"""
    return service_for("forecast_model").audit_error_report(error_report_id)


@router.post("/comparison/{comparison_id}/compute")
def api_compute_comparison(comparison_id: int) -> dict:
    """对指定分群对比执行计算。"""
    svc = service_for("cohort_compare")
    return svc.run_comparison(comparison_id)


@router.post("/cube/{cube_id}/precompute")
def api_precompute_cube(cube_id: int) -> dict:
    """对指定Cube执行预计算。"""
    svc = service_for("cube_builder")
    return svc.run_precomputation(cube_id)


@router.post("/report/{report_id}/generate")
def api_generate_report(report_id: int) -> dict:
    """对指定报告模板执行生成。"""
    svc = service_for("data_report")
    return svc.generate(report_id)


@router.post("/alerts/check-all")
def api_check_all_alerts() -> dict:
    """检查所有已生效告警规则。"""
    svc = service_for("alert_policy")
    return svc.check_all()


@router.get("/analytics/full-pipeline")
def api_full_pipeline() -> dict:
    """执行完整分析链路：事件采集→聚合→指标→分群→漏斗→留存→预测→报告。"""
    steps = {}

    # 1. 事件采集
    svc_events = service_for("event_tracker")
    steps["events"] = svc_events.ingest_all_enabled()

    # 2. 聚合
    svc_agg = service_for("realtime_agg")
    steps["aggregation"] = svc_agg.run_all_active()

    # 3. 指标快照
    svc_metrics = service_for("metric_store")
    steps["metrics"] = svc_metrics.compute_all()

    # 4. 分群
    segments = db.fetchall("SELECT id FROM segments LIMIT 6")
    seg_results = []
    for s in segments:
        r = service_for("segment_engine").run_computation(s["id"])
        seg_results.append(r)
    steps["segments"] = {"processed": len(segments), "results": seg_results}

    # 5. 漏斗
    funnels = db.fetchall("SELECT id FROM funnels LIMIT 3")
    funnel_results = []
    for f in funnels:
        r = service_for("funnel_analyzer").run_computation(f["id"])
        funnel_results.append(r)
    steps["funnels"] = {"processed": len(funnels), "results": funnel_results}

    # 6. 留存
    ret_tasks = db.fetchall("SELECT id FROM retention_tasks LIMIT 3")
    ret_results = []
    for t in ret_tasks:
        r = service_for("retention_analysis").run_computation(t["id"])
        ret_results.append(r)
    steps["retention"] = {"processed": len(ret_tasks), "results": ret_results}

    # 7. 预测
    forecast_tasks = db.fetchall("SELECT id FROM forecast_tasks LIMIT 2")
    fc_results = []
    for t in forecast_tasks:
        r = service_for("forecast_model").run_forecast(t["id"])
        fc_results.append(r)
    steps["forecast"] = {"processed": len(forecast_tasks), "results": fc_results}

    # 8. 报告
    reports = db.fetchall("SELECT id FROM reports LIMIT 2")
    report_results = []
    for rep in reports:
        r = service_for("data_report").generate(rep["id"])
        report_results.append(r)
    steps["reports"] = {"processed": len(reports), "results": report_results}

    return {"ok": True, "pipeline": steps}