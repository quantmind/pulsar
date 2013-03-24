(function($) {
    
    $.wschat = function(ws) {
        messages = $('#messages'),
        message = $('#message');
        ws.onmessage = function(e) {
            var data = $.parseJSON(e.data);
            messages.prepend('<p>'+data.message+'</p>');
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