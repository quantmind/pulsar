from django.conf.urls import patterns, url, include
from django.contrib import admin
from django.contrib.staticfiles.urls import staticfiles_urlpatterns


admin.autodiscover()


urlpatterns = patterns(
    '',
    url(r'^$', 'chat.views.home'),
    url(r'^admin/', include(admin.site.urls))
)


urlpatterns += staticfiles_urlpatterns()
