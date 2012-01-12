try:
    import markdown
except ImportError:
    markdown = None


def safe_markdown(text, **kwargs):
    if markdown:
        return markdown.markdown(text)
    else:
        return text


def render_text(text, syntax = 'markdown', **kwargs):
    if syntax == 'markdown':
        return safe_markdown(text, **kwargs)
    else:
        return text 