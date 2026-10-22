from airflow.utils.trigger_rule import TriggerRule


@task(trigger_rule=TriggerRule.ALL_DONE)
def log_pipeline_run(
    dag_id: str,
    run_id: str,
    start_time: str,
    rows_extracted: int = 0,
    rows_loaded: int = 0,
    rows_quarantined: int = 0,
):
    import duckdb
    from datetime import datetime

    conn = duckdb.connect(
        "/usr/local/airflow/include/warehouse_Ankit_Rawat.duckdb"
    )

    conn.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id VARCHAR PRIMARY KEY,
            dag_id VARCHAR,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            rows_extracted INT,
            rows_loaded INT,
            rows_quarantined INT,
            status VARCHAR
        );
    """)

    conn.execute(
        """
        INSERT INTO pipeline_runs 
        VALUES (?, ?, TRY_CAST(? AS TIMESTAMP), CURRENT_TIMESTAMP, ?, ?, ?, 'SUCCESS')
        ON CONFLICT (run_id) DO UPDATE SET
            end_time = EXCLUDED.end_time,
            rows_loaded = EXCLUDED.rows_loaded,
            rows_quarantined = EXCLUDED.rows_quarantined,
            status = EXCLUDED.status;
    """,
        (
            run_id,
            dag_id,
            start_time,
            rows_extracted,
            rows_loaded,
            rows_quarantined,
        ),
    )

    conn.close()
    print(f"[Audit Logged]: {run_id} for {dag_id}")