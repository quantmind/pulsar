from django.conf.urls.defaults import patterns, url

urlpatterns = patterns('',
    url(r'^acsv$', 'djangotest.views.acsv'),
    url(r'^$', 'djangotest.views.home'),
    
)