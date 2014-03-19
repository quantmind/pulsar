(function($) {
    var ws, current_mailbox,
        mailboxes = $('#mailboxes');

    $.start_web_mail = function (addr) {
        ws = new WebSocket(addr);

        ws.onmessage = function(e) {
            var data = $.parseJSON(e.data),
                list = data.list;
            if (list) {
                add_mailboxes(list);
            }
        };
    }

    function add_mailboxes (list) {
        var m = mailboxes.html('');
        $.each(list, function () {
            m.append('<li><a class="mailbox" href="#">' + this + '</a></li>');
        });
    }

    $(document).on('click', 'a.mailbox', function () {
        var elem = $(this);
            name = elem.html();
        ws.send(JSON.stringify({'mailbox': name}));
    });
}(jQuery));
