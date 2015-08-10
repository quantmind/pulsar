//
(function (root) {
    "use strict";

    if (!root.lux)
        root.lux = {};

    // If a file assign http as protocol (https does not work with PhantomJS)
    var protocol = root.location ? (root.location.protocol === 'file:' ? 'http:' : '') : '',
        end = '.js',
        ostring = Object.prototype.toString,
        lux = root.lux;


    function isArray(it) {
        return ostring.call(it) === '[object Array]';
    }

    function minify () {
        if (root.lux.context)
            return lux.context.MINIFIED_MEDIA;
    }

    function baseUrl () {
        if (root.lux.context)
            return lux.context.MEDIA_URL;
    }

    function extend (o1, o2) {
        if (o2) {
            for (var key in o2) {
                if (o2.hasOwnProperty(key))
                    o1[key] = o2[key];
            }
        }
        return o1;
    }

    function defaultPaths () {
        return {
            "angular": "//ajax.googleapis.com/ajax/libs/angularjs/1.3.15/angular",
            "angular-animate": "//ajax.googleapis.com/ajax/libs/angularjs/1.3.15/angular-animate",
            "angular-mocks": "//ajax.googleapis.com/ajax/libs/angularjs/1.3.15/angular-mocks.js",
            "angular-sanitize": "//ajax.googleapis.com/ajax/libs/angularjs/1.3.15/angular-sanitize",
            "angular-touch": "//cdnjs.cloudflare.com/ajax/libs/angular.js/1.3.15/angular-touch",
            "angular-strap": "//cdnjs.cloudflare.com/ajax/libs/angular-strap/2.2.1/angular-strap",
            "angular-strap-tpl": "//cdnjs.cloudflare.com/ajax/libs/angular-strap/2.2.4/angular-strap.tpl",
            "angular-ui-router": "//cdnjs.cloudflare.com/ajax/libs/angular-ui-router/0.2.14/angular-ui-router",
            "angular-pusher": "//cdn.jsdelivr.net/angular.pusher/latest/pusher-angular.min.js",
            "async": "//cdnjs.cloudflare.com/ajax/libs/requirejs-async/0.1.1/async.js",
            "pusher": "//js.pusher.com/2.2/pusher",
            "codemirror": "//cdnjs.cloudflare.com/ajax/libs/codemirror/3.21.0/codemirror",
            "codemirror-markdown": "//cdnjs.cloudflare.com/ajax/libs/codemirror/3.21.0/mode/markdown/markdown",
            "codemirror-javascript": "//cdnjs.cloudflare.com/ajax/libs/codemirror/3.21.0/mode/javascript/javascript",
            "codemirror-xml": "//cdnjs.cloudflare.com/ajax/libs/codemirror/3.21.0/mode/xml/xml",
            "codemirror-css": "//cdnjs.cloudflare.com/ajax/libs/codemirror/3.21.0/mode/css/css",
            "codemirror-htmlmixed": "//cdnjs.cloudflare.com/ajax/libs/codemirror/3.21.0/mode/htmlmixed/htmlmixed",
            "crossfilter": "//cdnjs.cloudflare.com/ajax/libs/crossfilter/1.3.11/crossfilter",
            "d3": "//cdnjs.cloudflare.com/ajax/libs/d3/3.5.5/d3",
            "google-analytics": "//www.google-analytics.com/analytics.js",
            "gridster": "//cdnjs.cloudflare.com/ajax/libs/jquery.gridster/0.5.6/jquery.gridster",
            "holder": "//cdnjs.cloudflare.com/ajax/libs/holder/2.3.1/holder",
            "highlight": "//cdnjs.cloudflare.com/ajax/libs/highlight.js/8.3/highlight.min.js",
            "katex": "//cdnjs.cloudflare.com/ajax/libs/KaTeX/0.3.0/katex.min.js",
            "leaflet": "//cdnjs.cloudflare.com/ajax/libs/leaflet/0.7.3/leaflet.js",
            "lodash": "//cdnjs.cloudflare.com/ajax/libs/lodash.js/2.4.1/lodash",
            "marked": "//cdnjs.cloudflare.com/ajax/libs/marked/0.3.2/marked",
            "mathjax": "//cdn.mathjax.org/mathjax/latest/MathJax.js?config=TeX-AMS-MML_HTMLorMML",
            "moment": "//cdnjs.cloudflare.com/ajax/libs/moment.js/2.10.3/moment",
            "restangular": "//cdnjs.cloudflare.com/ajax/libs/restangular/1.4.0/restangular",
            "sockjs": "//cdnjs.cloudflare.com/ajax/libs/sockjs-client/0.3.4/sockjs.min.js",
            "stats": "//cdnjs.cloudflare.com/ajax/libs/stats.js/r11/Stats",
            "topojson": "//cdnjs.cloudflare.com/ajax/libs/topojson/1.6.19/topojson"
        };
    }


    // Default shims
    function defaultShim () {
        return {
            angular: {
                exports: "angular"
            },
            "angular-strap-tpl": {
                deps: ["angular", "angular-strap"]
            },
            "google-analytics": {
                exports: root.GoogleAnalyticsObject || "ga"
            },
            highlight: {
                exports: "hljs"
            },
            lux: {
                deps: ["angular"]
            },
            "ui-bootstrap": {
                deps: ["angular"]
            },
            "codemirror": {
                exports: "CodeMirror"
            },
            "codemirror-markdown": {
                deps: ["codemirror"]
            },
            "codemirror-xml": {
                deps: ["codemirror"]
            },
            "codemirror-javascript": {
                deps: ["codemirror"]
            },
            "codemirror-css": {
                deps: ["codemirror"]
            },
            "codemirror-htmlmixed": {
                deps: ["codemirror", "codemirror-xml", "codemirror-javascript", "codemirror-css"],
            },
            restangular: {
                deps: ["angular"]
            },
            crossfilter: {
                exports: "crossfilter"
            },
            trianglify: {
                deps: ["d3"],
                exports: "Trianglify"
            },
            mathjax: {
                exports: "MathJax"
            }
        };
    }


    function newPaths (cfg) {
        var all = {},
            min = minify() ? '.min' : '',
            prefix = root.local_require_prefix,
            paths = extend(defaultPaths(), cfg.paths);

        for(var name in paths) {
            if(paths.hasOwnProperty(name)) {
                var path = paths[name];

                if (prefix && path.substring(0, prefix.length) === prefix)
                    path = path.substring(prefix.length);

                if (!cfg.shim[name]) {
                    // Add angular dependency
                    if (name.substring(0, 8) === "angular-")
                        cfg.shim[name] = {
                            deps: ["angular"]
                        };
                    else if (name.substring(0, 3) === "d3-")
                        cfg.shim[name] = {
                            deps: ["d3"]
                        };
                }

                if (typeof(path) !== 'string') {
                    // Don't maanipulate it, live it as it is
                    path = path.url;
                } else {
                    var params = path.split('?');
                    if (params.length === 2) {
                        path = params[0];
                        params = params[1];
                    } else
                        params = '';
                    if (path.substring(path.length-3) !== end)
                        path += min;
                    if (params) {
                        if (path.substring(path.length-3) !== end)
                            path += end;
                        path += '?' + params;
                    }
                    // Add protocol
                    if (path.substring(0, 2) === '//' && protocol)
                        path = protocol + path;

                    if (path.substring(path.length-3) === end)
                        path = path.substring(0, path.length-3);
                }
                all[name] = path;
            }
        }
        return all;
    }

    // require.config override
    lux.config = function (cfg) {
        if(!cfg.baseUrl) {
            var url = baseUrl();
            if (url !== undefined) cfg.baseUrl = url;
        }
        cfg.shim = extend(defaultShim(), cfg.shim);
        cfg.paths = newPaths(cfg);
        require.config(cfg);
    };

}(this));
