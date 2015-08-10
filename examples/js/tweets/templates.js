angular.module('templates-tweets', ['tweets/templates/tweets.tpl.html']);

angular.module("tweets/templates/tweets.tpl.html", []).run(["$templateCache", function($templateCache) {
  $templateCache.put("tweets/templates/tweets.tpl.html",
    "<div class=\"media\" ng-repeat=\"msg in messages | orderBy: ['-timestamp']\">\n" +
    "	<div class=\"media-left\">\n" +
    "    	<a ng-href=\"{{ msg.url }}\">\n" +
    "      		<img class=\"media-object\" ng-src=\"{{msg.user.profile_image_url_https}}\"\n" +
    "			alt=\"{{msg.user.name}}\" class=\"img-thumbnail\">\n" +
    "    	</a>\n" +
    "  	</div>\n" +
    "	<div class=\"media-body\">\n" +
    "		<p class='list-group-item-text message'>{{msg.text}}</p>\n" +
    "	</div>\n" +
    "</div>\n" +
    "");
}]);
