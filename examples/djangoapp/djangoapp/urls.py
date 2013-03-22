from django.conf.urls.defaults import patterns, url

urlpatterns = patterns('',
    url(r'^acsv$', 'djangoapp.views.acsv'),
    url(r'^$', 'djangoapp.views.home'),
    
)