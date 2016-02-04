from collections import Mapping

from pulsar.utils.structures import recursive_update

HTML_VISITORS = {}


__all__ = ['HtmlVisitor']


newline = frozenset(('meta', 'link', 'script', 'head', 'body', 'title'))


def html_visitor(tag):
    return HTML_VISITORS.get(tag, base)


class HtmlType(type):

    def __new__(cls, name, bases, attrs):
        abstract = attrs.pop('abstract', False)
        new_class = super().__new__(cls, name, bases, attrs)
        if not abstract:
            HTML_VISITORS[name.lower()] = new_class()
        return new_class


class HtmlVisitor(HtmlType('HtmlVisitorBase', (object,), {'abstract': True})):
    '''A visitor class for :class:`Html`.'''
    abstract = True

    def get_form_value(self, html):
        return html.attr('value')

    def set_form_value(self, html, value):
        html.attr('value', value)

    def add_data(self, html, key, value):
        if value is not None:
            data = html._data
            if key in data and isinstance(value, Mapping):
                target = data[key]
                if isinstance(target, Mapping):
                    return recursive_update(target, value)
            data[key] = value


base = HtmlVisitor()


class Textarea(HtmlVisitor):

    def get_form_value(self, html):
        return html.children[0] if html.children else ''

    def set_form_value(self, html, value):
        html.remove_all()
        html.append(value)


class Select(HtmlVisitor):

    def get_form_value(self, html):
        values = []
        for child in html.children:
            if child.attr('selected') == 'selected':
                values.append(child.attr('value'))
        if html.attr('multiple') == 'multiple':
            return values
        elif values:
            return values[0]

        return html.children[0] if html.children else ''

    def set_form_value(self, html, value):
        if html.attr('multiple') == 'multiple':
            for child in html.children:
                if child.attr('value') == value:
                    child.attr('selected', 'selected')
                    break
        else:
            for child in html.children:
                if child.attr('value') == value:
                    child.attr('selected', 'selected')
                else:
                    child._attr.pop('selected', None)
