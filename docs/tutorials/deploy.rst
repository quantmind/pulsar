.. _tutorials-deploy:

===========================
Deploy Web Applications
===========================

Pulsar has a fully fledged wsgi http server which can used to serve dynamic
web applications. The best way to do that is to run pulsar server behind
another HTTP server such as nginx_ and apache_.

Nginx
==========

The best way to deploy web apps using pulsar wsgi http server is
to use nginx as reverse proxy server to pulsar server.

For example, this minimal configuration does the trick::

    server {
        listen 80;
        server_name example.org;
        access_log  /var/log/nginx/example.log;

        location / {
            proxy_pass http://127.0.0.1:8060;
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_redirect off;
        }
    }


Link the configuration file to the nginx site_enabled directory
(usually in ``/etc/nginx/site_enabled``)::

    sudo ln -s site.conf /etc/nginx/sites-enabled/site.conf

and `reload nginx`_::

    nginx -s reload

Apache
=========

An alternative option to nginx is to use apache_ web server.
Apache is a complex server with lots of configuration parameters to fine tune
how it works.

To use pulsar behind apache you need the mod_proxy_ installed.


.. _nginx: http://nginx.org/en/
.. _apache: http://httpd.apache.org/
.. _mod_proxy: http://httpd.apache.org/docs/current/mod/mod_proxy.html
.. _`reload nginx`: http://wiki.nginx.org/CommandLine
