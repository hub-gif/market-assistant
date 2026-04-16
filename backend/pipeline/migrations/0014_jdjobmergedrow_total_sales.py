# Generated manually for keyword_pipeline_merged.csv column 销量展示(totalSales).

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("pipeline", "0013_rebuild_pipelinejobcheckpoint"),
    ]

    operations = [
        migrations.AddField(
            model_name="jdjobmergedrow",
            name="total_sales",
            field=models.TextField(blank=True, default=""),
        ),
    ]
