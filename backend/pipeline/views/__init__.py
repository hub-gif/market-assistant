"""Pipeline HTTP API 视图（按领域拆分子模块，本包对外保持与原 ``views.py`` 一致的导出）。"""
from __future__ import annotations

from .dataset_views import (
    JobDatasetCommentsView,
    JobDatasetDetailView,
    JobDatasetExportView,
    JobDatasetMergedView,
    JobDatasetSearchView,
    JobDatasetSummaryView,
)
from .ingest_views import JobImportMergedView
from .job_report_views import (
    JobCompetitorBriefPackView,
    JobCompetitorBriefView,
    JobExportDocumentView,
    JobReportAssetView,
    JobStrategyDraftView,
)
from .job_views import (
    JobCancelView,
    JobDetailView,
    JobDownloadView,
    JobListCreateView,
    JobPreviewView,
    JobRegenerateReportView,
    JobResumeView,
    ReportConfigDefaultsView,
)
from .product_views import (
    JdProductDetailView,
    JdProductListView,
    JdProductSnapshotDetailView,
    JdProductSnapshotListView,
)

__all__ = [
    "JobDatasetCommentsView",
    "JobDatasetDetailView",
    "JobDatasetExportView",
    "JobDatasetMergedView",
    "JobDatasetSearchView",
    "JobDatasetSummaryView",
    "JobImportMergedView",
    "JobCompetitorBriefPackView",
    "JobCompetitorBriefView",
    "JobExportDocumentView",
    "JobReportAssetView",
    "JobStrategyDraftView",
    "JobCancelView",
    "JobDetailView",
    "JobDownloadView",
    "JobListCreateView",
    "JobPreviewView",
    "JobRegenerateReportView",
    "JobResumeView",
    "ReportConfigDefaultsView",
    "JdProductDetailView",
    "JdProductListView",
    "JdProductSnapshotDetailView",
    "JdProductSnapshotListView",
]
