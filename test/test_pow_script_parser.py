"""Differential tests for script-only HTML parsing, with the former stdlib parser as oracle."""
import random
import re
from html.parser import HTMLParser

import pytest

from utils.pow import ScriptSrcParser, parse_pow_resources, parse_pow_resources_bytes


class ReferenceParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.script_sources = []
        self.data_build = ''

    def handle_starttag(self, tag, attrs):
        if tag != 'script':
            return
        src = dict(attrs).get('src')
        if src:
            self.script_sources.append(src)
            match = re.search(r'c/[^/]*/_', src)
            if match:
                self.data_build = match.group(0)


def compare(html, widths=(None,), scripting=None):
    for width in widths:
        old, new = ReferenceParser(), ScriptSrcParser()
        if scripting is not None:
            old.scripting = new.scripting = scripting
        parts = [html] if width is None else [html[i:i+width] for i in range(0, len(html), width)]
        for part in parts:
            old.feed(part)
            new.feed(part)
            assert (new.script_sources, new.data_build) == (old.script_sources, old.data_build)
        old.close()
        new.close()
        assert (new.script_sources, new.data_build) == (old.script_sources, old.data_build)


@pytest.mark.parametrize('html', [
    '<html data-build="version"><div class="x&amp;y"><script src="/c/version/_app.js"></script></div>',
    '<!-- <script src="fake"> --><SCRIPT SRC="/sdk?a=1&amp;b=2"></SCRIPT>',
    '<style>.x { content: "<script src=fake>"; }</style><script src=real></script>',
    '<script>const a = "<script src=fake>";</script><script src=real></script>',
    '<textarea><script src=maybe></script></textarea><title><script src=next></script></title>',
    '<div data-value="<script src=fake>"><script src=real></script></div>',
    '<div data-value=\'a>b\' title="<"><script src="/c/build/_sdk.js"/></div>',
    '<div x="unterminated <script src=inside></script>',
    '<div / data-x=q><script src=x src=y></script>',
    '<script src="&#x2F;sdk&#63;a=1&amp;b=2"></script><script src=""></script>',
    '<?xml version="1.0"?><!DOCTYPE html><script src=real></script>',
    '<div\x00bad foo=bar><script src=after></script>',
    '<div\u2003attr=x><script src=after></script>',
    '<custom-tag x="one&amp;two"><script src=/sdk /></custom-tag>',
    '<plaintext><script src=after></script>',
    '<![CDATA[<script src=fake>]]><script src=real></script>',
    '<a href="/?a=&amp;b"><span/> &lt;script src=fake&gt;</a><script src=real></script>',
    '<SCRIPT\nSRC=\"/sdk\"/><script src=/c/b/_x></script>',
    '<div data-x=`weird`><?bogus><script src=x></script></>',
    '<div<<script src=x></script>',
    '<><script src=x></script><div / >',
    '',
])
def test_ignored_attributes_preserve_stdlib_script_parsing(html):
    compare(html, (None, 1, 7, 29))


def test_generated_malformed_and_quoted_tags_match_stdlib():
    rng = random.Random(20261101)
    fragments = [
        '<div class="a&amp;b" data-x="<script src=fake>">', '</div>',
        '<span title="a>b">', '</span>', '<p a=one b=two/>',
        '<script src="/c/b/_sdk.js?a=1&amp;b=2"></script>',
        '<script>let x="<script src=fake>";</script>',
        '<style>.a{content:"<script src=fake>"}</style>',
        '<!-- <script src=fake> -->', '<textarea>', '</textarea>',
        '&lt;script&gt;', '<input disabled>', '<div x="unfinished',
        '"/>', '<svg><path d="a,b"/></svg>', '<!--', '-->',
        '<script src=x src=y/>', '<div / data-x=q>', '</>',
    ]
    for _ in range(300):
        html = ''.join(rng.choices(fragments, k=rng.randrange(1, 35)))
        compare(html, (None, 13))


@pytest.mark.parametrize('encoding', ['utf-8', 'gb18030', 'utf-16'])
def test_bytes_and_string_entrypoints_keep_decoding_and_build_fallback(encoding):
    html = '<html data-build="版本"><div title="账户"><script src="/sdk?a=1&amp;b=2"></script></div></html>'
    expected = (['/sdk?a=1&b=2'], '版本')
    assert parse_pow_resources(html) == expected
    assert parse_pow_resources_bytes(html.encode(encoding), encoding) == expected


@pytest.mark.parametrize("scripting", [False, True])
@pytest.mark.parametrize("tag", ["plaintext", "noscript", "style", "script", "textarea", "title", "xmp", "iframe", "noembed", "noframes"])
@pytest.mark.parametrize("end", [">", "/>"])
def test_raw_text_and_scripting_modes_keep_stdlib_state(tag, end, scripting):
    html = f'<{tag}{end}<script src="inside"></script></{tag}><script src="after"></script>'
    compare(html, (None, 1, 7), scripting=scripting)
