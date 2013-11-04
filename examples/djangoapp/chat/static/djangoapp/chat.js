(function($) {

    $.wschat = function(ws) {
        var messages = $('#messages'),
            users = $('#users'),
            message = $('#message');

        ws.onmessage = function(e) {
            var data = $.parseJSON(e.data),
                channel = data.channel,
                label = 'info">@',
                user;
            if (data.user === 'anonymous') {
                label = 'inverse">';
            }
            user = '<span class="label label-' + label + data.user + '</span>';
            if (data.channel == 'webchat') {
                messages.prepend('<p>' + user + '&nbsp;' + data.message + '</p>');
            } else if (data.channel == 'chatuser') {
                users.append('<p>' + user + '</p>');
            }
        };
        $('#publish').click(function () {
            var msg = message.val();
            ws.send(msg);
            message.val('');
        });
    };

}(jQuery));
