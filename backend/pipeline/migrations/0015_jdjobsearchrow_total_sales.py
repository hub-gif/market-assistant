# Generated manually: align JdJobSearchRow with JD_SEARCH_INTERNAL_KEYS / pc_search_export.

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("pipeline", "0014_jdjobmergedrow_total_sales"),
    ]

    operations = [
        migrations.AddField(
            model_name="jdjobsearchrow",
            name="total_sales",
            field=models.TextField(blank=True, default=""),
        ),
    ]
