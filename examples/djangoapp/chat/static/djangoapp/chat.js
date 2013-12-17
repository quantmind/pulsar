(function ($) {
    "use strict";
    $.wschat = function (ws) {
        var messages = $('#messages'),
            users = $('#users'),
            message = $('#message'),
            user_label,
            who_ami,
            wall = function (user, message) {
                messages.prepend('<p>' + user + '&nbsp;' + message + '</p>');
            };

        ws.onmessage = function (e) {
            var data = $.parseJSON(e.data),
                label = 'info">@',
                user = data.user,
                userid = 'chatuser-' + user;
            if (!data.authenticated) {
                label = 'inverse">';
            }
            if (!who_ami) {
                who_ami = user;
            }
            user_label = '<span class="label label-' + label + user + '</span>';
            if (data.channel === 'webchat') {
                wall(user_label, data.message);
            } else if (data.channel === 'chatuser') {
                if (data.message === 'joined') {
                    if (users.find('#' + userid).length === 0) {
                        users.append('<p id="' + userid + '">' + user_label + '</p>');
                        wall(user_label, 'joined the chat');
                    }
                } else {
                    wall(user_label, 'left the chat');
                    users.find('#' + userid).remove();
                }
            }
        };
        $('#publish').click(function () {
            var msg = message.val();
            ws.send(msg);
            message.val('');
        });
    };

}(jQuery));
