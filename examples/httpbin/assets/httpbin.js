(function($) {
    function ajax_request(method) {
        return function (e) {
            e.preventDefault();
            $.ajax({
                url: e.target.href,
                type: method,
                data: {
                    body: 'this is a message',
                    type: method,
                },
                success: function (data, status) {
                    alert('Got succesfull response: ' + data['method']);
                }
            });
        }
    }
    $(document).ready(function() {
        $.each(['post', 'delete', 'put', 'patch'], function (i, name) {
            $('a.' + name).click(ajax_request(name));
        });
    });
}(jQuery));