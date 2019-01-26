"""
Django settings for back project.

For more information on this file, see
https://docs.djangoproject.com/en/1.6/topics/settings/

For the full list of settings and their values, see
https://docs.djangoproject.com/en/1.6/ref/settings/
"""

# Build paths inside the project like this: os.path.join(BASE_DIR, ...)
import os
import sys
from django.utils.text import ugettext_lazy as _
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from local_settings import *


# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/1.6/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = 'v43^2@pxv9(f3zds(cqy3m0btnf2pbw&urf#%tajzn)#1s7a29'

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = True

TEMPLATE_DEBUG = True

ALLOWED_HOSTS = []


# Application definition

INSTALLED_APPS = (
    'grappelli',
    'filebrowser',
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'core',
    'django_rq'
)

MIDDLEWARE_CLASSES = (
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
)

ROOT_URLCONF = 'back.urls'

WSGI_APPLICATION = 'back.wsgi.application'

# Database
# https://docs.djangoproject.com/en/1.6/ref/settings/#databases

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql_psycopg2',
        'NAME': DATA_NAME,
        'USER': DATA_USER, 
        'PASSWORD': DATA_PASSWORD,
        'HOST': DATA_HOST, 
        'PORT': '', 
    }
}

# Internationalization
# https://docs.djangoproject.com/en/1.6/topics/i18n/

LANGUAGE_CODE = 'en-us'

TIME_ZONE = 'Asia/Shanghai'

USE_I18N = True

USE_L10N = True

USE_TZ = True

MEDIA_ROOT = (__file__.rpartition('/')[0] or '.') + '/../media/'
MEDIA_URL = BASE_URL + '/media/'
# MEDIA_ROOT = (__file__.rpartition('/')[0] or '.') + '/../media/'
# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/1.6/howto/static-files/
#STATIC_ROOT = (__file__.rpartition('/')[0] or '.') + '/../static/'
STATIC_URL = '/static/'
#STATIC_URL = BASE_URL + '/static/'
STATICFILES_DIRS = (
    os.path.join(BASE_DIR, 'static'),
    # Put strings here, like "/home/html/static" or "C:/www/django/static".
    # Always use forward slashes, even on Windows.
    # Don't forget to use absolute paths, not relative paths.
)

# List of finder classes that know how to find static files in
# various locations.
STATICFILES_FINDERS = (
    'django.contrib.staticfiles.finders.FileSystemFinder',
    'django.contrib.staticfiles.finders.AppDirectoriesFinder',
    #    'django.contrib.staticfiles.finders.DefaultStorageFinder',
)

TEMPLATE_DIRS = (
    os.path.join(BASE_DIR, 'templates'),
    # Put strings here, like "/home/html/django_templates" or "C:/www/django/templates".
    # Always use forward slashes, even on Windows.
    # Don't forget to use absolute paths, not relative paths.
)
# Filebrowser
FILEBROWSER_DIRECTORY = 'up/'
FILEBROWSER_VERSIONS_BASEDIR = 'up/v/'
FILEBROWSER_STRICT_PIL = True
FILEBROWSER_IMAGE_MAXBLOCK = 1920 * 1080
FILEBROWSER_MAX_UPLOAD_SIZE = 209715200  # 200m
FILEBROWSER_VERSIONS = {
    'admin_thumbnail': {'verbose_name': _('Admin Thumbnail'), 'width': 32, 'height': 32, 'opts': 'crop'},
    'thumbnail': {'verbose_name': _('Thumbnail'), 'width': 46, 'height': 46, 'opts': 'crop upscale'},
    'thumbnail_': {'verbose_name': _('Thumbnail_'), 'width': 31, 'height': 31, 'opts': 'crop upscale'},
    'small': {'verbose_name': _('Small'), 'width': 320, 'height': '', 'opts': 'crop upscale'},
    'small_': {'verbose_name': _('Small_'), 'width': 213, 'height': '', 'opts': 'crop upscale'},

    'medium': {'verbose_name': _('Medium'), 'width': 640, 'height': '', 'opts': 'crop upscale'},
    'medium_': {'verbose_name': _('Medium_'), 'width': 427, 'height': '', 'opts': 'crop upscale'},

    'big': {'verbose_name': _('Big'), 'width': 1024, 'height': '', 'opts': 'crop upscale'},
    'big_': {'verbose_name': _('Big_'), 'width': 683, 'height': '', 'opts': 'crop upscale'},

    'large': {'verbose_name': _('Large'), 'width': 1280, 'height': '', 'opts':  'crop upscale'},
    'large_': {'verbose_name': _('Large_'), 'width': 853, 'height': '', 'opts':  'crop upscale'},

    'ad': {'verbose_name': _('Ad'), 'width': 360, 'height': 770, 'opts':  'crop upscale'},
    'ad_': {'verbose_name': _('Ad'), 'width': 240, 'height': 513, 'opts':  'crop upscale'},

    '720p_': {'verbose_name': _('720p_'), 'width': 1280, 'height': '', 'opts':  'upscale'},
}
FILEBROWSER_VERSION_QUALITY = 85

################################################################################
RQ_QUEUES = {
    'default': {
        'HOST': 'localhost',
        'PORT': 6379,
        'DB': 2,
        'PASSWORD': '',
        },
    'low': {
        'HOST': 'localhost',
        'PORT': 6379,
        'DB': 2,
        'PASSWORD': '',
        },
    'high': {
        'HOST': 'localhost',
        'PORT': 6379,
        'DB': 2,
        'PASSWORD': '',
        }
}
