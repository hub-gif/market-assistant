# SQLite 上曾出现「迁移已记录但表缺少 lean 商详列」的不一致；补全缺失列以便与模型一致。

from django.db import migrations


def _repair(apps, schema_editor) -> None:
    conn = schema_editor.connection
    if conn.vendor != "sqlite":
        return
    JdJobMergedRow = apps.get_model("pipeline", "JdJobMergedRow")
    table = JdJobMergedRow._meta.db_table
    # 与 models.JdJobMergedRow 商详块一致
    additions: dict[str, str] = {
        "detail_brand": "TEXT NOT NULL DEFAULT ''",
        "detail_price_final": "TEXT NOT NULL DEFAULT ''",
        "detail_shop_name": "TEXT NOT NULL DEFAULT ''",
        "detail_category_path": "TEXT NOT NULL DEFAULT ''",
        "detail_product_attributes": "TEXT NOT NULL DEFAULT ''",
    }
    qn = conn.ops.quote_name
    t = qn(table)
    with conn.cursor() as cursor:
        desc = conn.introspection.get_table_description(cursor, table)
        have = {d.name for d in desc}
        for col, ddl in additions.items():
            if col in have:
                continue
            cursor.execute(f"ALTER TABLE {t} ADD COLUMN {qn(col)} {ddl}")


class Migration(migrations.Migration):

    dependencies = [
        ("pipeline", "0007_drop_merged_http_status_columns"),
    ]

    operations = [
        migrations.RunPython(_repair, migrations.RunPython.noop),
    ]
