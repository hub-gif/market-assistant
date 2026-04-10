"""
Django settings for config project.

环境变量**仅**从 ``market_assistant/.env`` 加载（与 ``.env.example`` 同目录），
勿再在 ``backend/.env`` 或仓库根目录使用第二份 .env，以免部署混淆。
"""
from pathlib import Path

from dotenv import load_dotenv
import os

BASE_DIR = Path(__file__).resolve().parent.parent
MA_ROOT = BASE_DIR.parent
load_dotenv(MA_ROOT / ".env")

_raw_low = (os.environ.get("LOW_GI_PROJECT_ROOT") or "").strip().strip('"').strip("'")
if _raw_low:
    LOW_GI_PROJECT_ROOT = str(Path(_raw_low).expanduser().resolve())
else:
    LOW_GI_PROJECT_ROOT = str(MA_ROOT.resolve())
os.environ["LOW_GI_PROJECT_ROOT"] = LOW_GI_PROJECT_ROOT
Path(LOW_GI_PROJECT_ROOT).joinpath("data", "JD").mkdir(parents=True, exist_ok=True)

CRAWLER_JD_ROOT = BASE_DIR / "crawler_copy" / "jd_pc_search"

SECRET_KEY = os.environ.get("DJANGO_SECRET_KEY", "django-insecure-dev-only-change-in-env")
DEBUG = os.environ.get("DJANGO_DEBUG", "True").lower() in ("1", "true", "yes")
ALLOWED_HOSTS = [
    h.strip()
    for h in os.environ.get("DJANGO_ALLOWED_HOSTS", "localhost,127.0.0.1").split(",")
    if h.strip()
]

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "rest_framework",
    "corsheaders",
    "pipeline",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "corsheaders.middleware.CorsMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "config.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "config.wsgi.application"

_sqlite = os.environ.get("DJANGO_SQLITE_PATH", "").strip()
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": Path(_sqlite).expanduser().resolve()
        if _sqlite
        else (BASE_DIR / "db.sqlite3"),
    }
}

AUTH_PASSWORD_VALIDATORS = [
    {"NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator"},
    {"NAME": "django.contrib.auth.password_validation.MinimumLengthValidator"},
    {"NAME": "django.contrib.auth.password_validation.CommonPasswordValidator"},
    {"NAME": "django.contrib.auth.password_validation.NumericPasswordValidator"},
]

LANGUAGE_CODE = "zh-hans"
TIME_ZONE = "Asia/Shanghai"
USE_I18N = True
USE_TZ = True

STATIC_URL = "static/"
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

REST_FRAMEWORK = {
    "DEFAULT_RENDERER_CLASSES": [
        "rest_framework.renderers.JSONRenderer",
    ],
}

_cors = os.environ.get("CORS_ALLOWED_ORIGINS", "http://localhost:5173,http://127.0.0.1:5173")
CORS_ALLOWED_ORIGINS = [x.strip() for x in _cors.split(",") if x.strip()]
CORS_ALLOW_CREDENTIALS = True

_csrf = os.environ.get("CSRF_TRUSTED_ORIGINS", "http://localhost:5173,http://127.0.0.1:5173")
CSRF_TRUSTED_ORIGINS = [x.strip() for x in _csrf.split(",") if x.strip()]
