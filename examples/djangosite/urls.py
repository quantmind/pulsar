from django.conf.urls.defaults import patterns, include, url

urlpatterns = patterns('',
    (r'^', include("testing.urls")),
    # Uncomment the next line to enable the admin:
    # url(r'^admin/', include(admin.site.urls)),
)
