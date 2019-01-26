from django.conf.urls import patterns, include, url

from django.contrib import admin
admin.autodiscover()
from filebrowser.sites import site as filebrowser

urlpatterns = patterns('',
    # Examples:
    # url(r'^$', 'back.views.home', name='home'),
    url(r'^grappelli/', include('grappelli.urls')),
    url(r'^admin/filebrowser/', include(filebrowser.urls)),
    url(r'^admin/', include(admin.site.urls)),
    # url(r'^gm/', 'core.views.gm', name='gm'),
    #url(r'^mail/', 'core.views.mail', name='mail'),
    url(r'^accounts/login/$', 'django.contrib.auth.views.login'),
    # url(r'^user/', 'core.views.user', name='user'),

)
urlpatterns += patterns('',
                        (r'^django-rq/', include('django_rq.urls')),
                        )