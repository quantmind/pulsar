HTML_VISITORS = {}


def html_visitor(tag):
    return HTML_VISITORS.get(tag, base)


class HtmlType(type):
    
    def __new__(cls, name, bases, attrs):
        new_class = super(HtmlType,cls).__new__(cls, name, bases, attrs)
        HTML_VISITORS[name.lower()] = new_class()
        return new_class

class Base(object):
    
    def get_form_value(self, html):
        return html.attr('value')
    
    def set_form_value(self, html, value):
        html.attr('value', value)
    

base = Base()
HtmlVisitor = HtmlType('HtmlVisitor', (Base,), {})
    
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
                if child.attr('value' == value):
                    child.attr('selected', 'selected')
                    break
        else:
            for child in html.children:
                if child.attr('value' == value):
                    child.attr('selected', 'selected')
                else:
                    child._attr.pop('selected', None)
    