
function(doc) {
    var statements= {{statements}},
        fields={{fields}},
        operators = {
            eq: function (doc, field, value) {
                return doc[field] === value;
            },
            gt: function (doc, field, value) {
                return doc[field] > value;
            },
            gte: function (doc, field, value) {
                return doc[field] >= value;
            },
            lt: function (doc, field, value) {
                return doc[field] < value;
            },
            lte: function (doc, field, value) {
                return doc[field] <= value;
            },
            ne: function (doc, field, value) {
                return doc[field] !== value;
            }
        },
        valid=true,
        statement, op, value;
    for(var i=0; i<statements.length; i++) {
        statetemt = statements[i];
        op = operators[statement.op];
        if !(op && op(doc, statement.field, statement.value)) {
            valid = false;
            break;
        }
    }
    if (valid) {
        if (fields.length) {
            var values = {};
            for (var j=0; j<fields.length; j++) {
                values[fields[j]] = doc[fields[j]];
            }
            emit(doc._id, values);
        } else {
            emit(doc._id);
        }
    }
}
