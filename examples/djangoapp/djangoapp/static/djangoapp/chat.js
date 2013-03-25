(function($) {
    
    $.wschat = function(ws) {
        messages = $('#messages'),
        message = $('#message');
        ws.onmessage = function(e) {
            var data = $.parseJSON(e.data),
            	label= 'info">@';
            if (data.user === 'anonymous') {
            	label = 'inverse">';
            }
            data.user = '<span class="label label-' + label + data.user + '</span>'
            messages.prepend('<p>' + data.user + '&nbsp;' + data.message + '</p>');
        };
        $('#publish').click(function () {
            var msg = message.val();
            ws.send(msg);
            message.val('');
        });
        ws.onopen = function() {
            // Send empty message so that we connect this client
            ws.send('');
        };      
    };
    
}(jQuery));