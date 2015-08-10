
	angular.module('twitter-example', ['templates-tweets'])

		.directive('twitter', ['$rootScope', '$log', function (root, log) {

			function connectSock(scope, url) {
				if (!root.websockets) root.websockets = {};
				var hnd = root.websockets[url];
				if (!hnd)
					root.websockets[url] = hnd = createSocket(url);
				return hnd;
			}

			function createSocket (url) {
				var sock = new WebSocket(url),
					listeners = [];

				sock.onopen = function() {
					log.info('New connection with ' + url);
				};

				sock.onmessage = function (e) {
					var msg = angular.fromJson(e.data);
					msg.timestamp = +msg.timestamp;
					msg.url = 'https://twitter.com/' + msg.user.screen_name + '/status/' + msg.id_str;
					angular.forEach(listeners, function (listener) {
						listener(sock, msg);
					});

				};

				return {
					sock: sock,
					listeners: listeners
				};
			}

			// Closure which handle incoming messages fro the server
			function tweetArrived (scope) {

				return function (sock, msg) {
					scope.messages.push(msg);
					scope.$apply();
				};
			}

			return {
				restrict: 'AE',
				templateUrl: 'tweets/templates/tweets.tpl.html',
				link: function (scope, element, attrs) {
					var options = attrs.twitter;
					if (options) options = angular.fromJson(options);

					scope.messages = [];
					if (options && options.url) {
						var hnd = connectSock(scope, options.url);
						hnd.listeners.push(tweetArrived(scope));
					} else
						log.error('Twitter directive improperly configured, no url found');
				}
			};
		}]);
