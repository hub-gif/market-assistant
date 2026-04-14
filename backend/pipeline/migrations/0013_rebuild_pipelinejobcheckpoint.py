# 0012 曾标记为已应用，但部分环境上 checkpoint 表仍为旧版 schema（stage/page_done 等）。
# 与当前 PipelineJobCheckpoint（phase/payload/hint_zh）对齐：删表后按 0012 预期 DDL 重建。

from django.db import migrations


_REBUILD_SQL = """
DROP TABLE IF EXISTS pipeline_pipelinejobcheckpoint;
CREATE TABLE "pipeline_pipelinejobcheckpoint" (
    "id" integer NOT NULL PRIMARY KEY AUTOINCREMENT,
    "phase" varchar(32) NOT NULL,
    "payload" text NOT NULL CHECK ((JSON_VALID("payload") OR "payload" IS NULL)),
    "hint_zh" text NOT NULL,
    "updated_at" datetime NOT NULL,
    "job_id" bigint NOT NULL UNIQUE REFERENCES "pipeline_pipelinejob" ("id") DEFERRABLE INITIALLY DEFERRED
);
CREATE INDEX "pipeline_pipelinejobcheckpoint_phase_12e50a62" ON "pipeline_pipelinejobcheckpoint" ("phase");
"""


class Migration(migrations.Migration):

    dependencies = [
        ("pipeline", "0012_job_pause_checkpoint"),
    ]

    operations = [
        migrations.SeparateDatabaseAndState(
            state_operations=[],
            database_operations=[
                migrations.RunSQL(_REBUILD_SQL, reverse_sql=migrations.RunSQL.noop),
            ],
        ),
    ]
