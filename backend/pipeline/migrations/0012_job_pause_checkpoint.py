# Generated manually for cookie pause / resume checkpoint

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("pipeline", "0011_job_cancel_and_status"),
    ]

    operations = [
        migrations.AddField(
            model_name="pipelinejob",
            name="resume_from_checkpoint",
            field=models.BooleanField(db_index=True, default=False),
        ),
        migrations.AlterField(
            model_name="pipelinejob",
            name="status",
            field=models.CharField(
                choices=[
                    ("pending", "待执行"),
                    ("running", "执行中"),
                    ("success", "成功"),
                    ("failed", "失败"),
                    ("cancelled", "已终止"),
                    ("paused", "已暂停（待换 Cookie 续跑）"),
                ],
                db_index=True,
                default="pending",
                max_length=16,
            ),
        ),
        migrations.CreateModel(
            name="PipelineJobCheckpoint",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("phase", models.CharField(db_index=True, max_length=32)),
                ("payload", models.JSONField(blank=True, default=dict)),
                ("hint_zh", models.TextField(blank=True, default="")),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "job",
                    models.OneToOneField(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="checkpoint_row",
                        to="pipeline.pipelinejob",
                    ),
                ),
            ],
            options={
                "ordering": ["-updated_at"],
            },
        ),
    ]
