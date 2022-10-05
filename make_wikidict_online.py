#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2010 Radim Rehurek <radimrehurek@seznam.cz>
# Copyright (C) 2012 Lars Buitinck <larsmans@gmail.com>
# Licensed under the GNU LGPL v2.1 - http://www.gnu.org/licenses/lgpl.html


"""
USAGE: %(program)s WIKI_XML_DUMP OUTPUT_PREFIX [VOCABULARY_SIZE]
Convert articles from a Wikipedia dump to (sparse) vectors. The input is a
bz2-compressed dump of Wikipedia articles, in XML format.
This actually creates several files:
* `OUTPUT_PREFIX_wordids.txt.bz2`: mapping between words and their integer ids
* `OUTPUT_PREFIX_bow.mm`: bag-of-words (word counts) representation in Matrix Market format
* `OUTPUT_PREFIX_bow.mm.index`: index for `OUTPUT_PREFIX_bow.mm`
* `OUTPUT_PREFIX_bow.mm.metadata.cpickle`: titles of documents
* `OUTPUT_PREFIX_tfidf.mm`: TF-IDF representation in Matrix Market format
* `OUTPUT_PREFIX_tfidf.mm.index`: index for `OUTPUT_PREFIX_tfidf.mm`
* `OUTPUT_PREFIX.tfidf_model`: TF-IDF model
The output Matrix Market files can then be compressed (e.g., by bzip2) to save
disk space; gensim's corpus iterators can work with compressed input, too.
`VOCABULARY_SIZE` controls how many of the most frequent words to keep (after
removing tokens that appear in more than 10%% of all documents). Defaults to
100,000.
Example:
  python -m gensim.scripts.make_wikicorpus ~/gensim/results/enwiki-latest-pages-articles.xml.bz2 ~/gensim/results/wiki
"""


import logging
import sys
import os
import re
import bz2
from collections import Counter

from lxml import etree
from concurrent.futures import ProcessPoolExecutor
import multiprocessing

# Wiki is first scanned for all distinct word types (~7M). The types that
# appear in more than 10% of articles are removed and from the rest, the
# DEFAULT_DICT_SIZE most frequent types are kept.
DEFAULT_DICT_SIZE = 100000



ARTICLE_MIN_WORDS = 50
"""Ignore shorter articles (after full preprocessing)."""

# default thresholds for lengths of individual tokens
TOKEN_MIN_LEN = 2
TOKEN_MAX_LEN = 15

RE_P0 = re.compile(r'<!--.*?-->', re.DOTALL | re.UNICODE)
"""Comments."""
RE_P1 = re.compile(r'<ref([> ].*?)(</ref>|/>)', re.DOTALL | re.UNICODE)
"""Footnotes."""
RE_P2 = re.compile(r'(\n\[\[[a-z][a-z][\w-]*:[^:\]]+\]\])+$', re.UNICODE)
"""Links to languages."""
RE_P3 = re.compile(r'{{([^}{]*)}}', re.DOTALL | re.UNICODE)
"""Template."""
RE_P4 = re.compile(r'{{([^}]*)}}', re.DOTALL | re.UNICODE)
"""Template."""
RE_P5 = re.compile(r'\[(\w+):\/\/(.*?)(( (.*?))|())\]', re.UNICODE)
"""Remove URL, keep description."""
RE_P6 = re.compile(r'\[([^][]*)\|([^][]*)\]', re.DOTALL | re.UNICODE)
"""Simplify links, keep description."""
RE_P7 = re.compile(r'\n\[\[[iI]mage(.*?)(\|.*?)*\|(.*?)\]\]', re.UNICODE)
"""Keep description of images."""
RE_P8 = re.compile(r'\n\[\[[fF]ile(.*?)(\|.*?)*\|(.*?)\]\]', re.UNICODE)
"""Keep description of files."""
RE_P9 = re.compile(r'<nowiki([> ].*?)(</nowiki>|/>)', re.DOTALL | re.UNICODE)
"""External links."""
RE_P10 = re.compile(r'<math([> ].*?)(</math>|/>)', re.DOTALL | re.UNICODE)
"""Math content."""
RE_P11 = re.compile(r'<(.*?)>', re.DOTALL | re.UNICODE)
"""All other tags."""
RE_P12 = re.compile(r'(({\|)|(\|-(?!\d))|(\|}))(.*?)(?=\n)', re.UNICODE)
"""Table formatting."""
RE_P13 = re.compile(r'(?<=(\n[ ])|(\n\n)|([ ]{2})|(.\n)|(.\t))(\||\!)([^[\]\n]*?\|)*', re.UNICODE)
"""Table cell formatting."""
RE_P14 = re.compile(r'\[\[Category:[^][]*\]\]', re.UNICODE)
"""Categories."""
RE_P15 = re.compile(r'\[\[([fF]ile:|[iI]mage)[^]]*(\]\])', re.UNICODE)
"""Remove File and Image templates."""
RE_P16 = re.compile(r'\[{2}(.*?)\]{2}', re.UNICODE)
"""Capture interlinks text and article linked"""
RE_P17 = re.compile(
    r'(\n.{0,4}((bgcolor)|(\d{0,1}[ ]?colspan)|(rowspan)|(style=)|(class=)|(align=)|(scope=))(.*))|'
    r'(^.{0,2}((bgcolor)|(\d{0,1}[ ]?colspan)|(rowspan)|(style=)|(class=)|(align=))(.*))',
    re.UNICODE
)

RE_HTML_ENTITY = re.compile(r'&(#?)([xX]?)(\w{1,8});', re.UNICODE)

"""Table markup"""
IGNORED_NAMESPACES = [
    'Wikipedia', 'Category', 'File', 'Portal', 'Template',
    'MediaWiki', 'User', 'Help', 'Book', 'Draft', 'WikiProject',
    'Special', 'Talk'
]
"""MediaWiki namespaces that ought to be ignored."""

def to_unicode(raw, encoding='utf8', errors='strict') -> str:
    if isinstance(raw, str):
        return raw
    return str(raw, encoding, errors=errors)

def decode_htmlentities(text):
    """Decode all HTML entities in text that are encoded as hex, decimal or named entities.
    Adapted from `python-twitter-ircbot/html_decode.py
    <http://github.com/sku/python-twitter-ircbot/blob/321d94e0e40d0acc92f5bf57d126b57369da70de/html_decode.py>`_.
    Parameters
    ----------
    text : str
        Input HTML.
    Examples
    --------
    .. sourcecode:: pycon
        >>> from gensim.utils import decode_htmlentities
        >>>
        >>> u = u'E tu vivrai nel terrore - L&#x27;aldil&#xE0; (1981)'
        >>> print(decode_htmlentities(u).encode('UTF-8'))
        E tu vivrai nel terrore - L'aldilà (1981)
        >>> print(decode_htmlentities("l&#39;eau"))
        l'eau
        >>> print(decode_htmlentities("foo &lt; bar"))
        foo < bar
    """
    def substitute_entity(match):
        try:
            ent = match.group(3)
            if match.group(1) == "#":
                # decoding by number
                if match.group(2) == '':
                    # number is in decimal
                    return safe_unichr(int(ent))
                elif match.group(2) in ['x', 'X']:
                    # number is in hex
                    return safe_unichr(int(ent, 16))
            else:
                # they were using a name
                cp = n2cp.get(ent)
                if cp:
                    return safe_unichr(cp)
                else:
                    return match.group()
        except Exception:
            # in case of errors, return original input
            return match.group()

    return RE_HTML_ENTITY.sub(substitute_entity, text)
        
def filter_wiki(raw, promote_remaining=True, simplify_links=True):
    """Filter out wiki markup from `raw`, leaving only text.
    Parameters
    ----------
    raw : str
        Unicode or utf-8 encoded string.
    promote_remaining : bool
        Whether uncaught markup should be promoted to plain text.
    simplify_links : bool
        Whether links should be simplified keeping only their description text.
    Returns
    -------
    str
        `raw` without markup.
    """
    # parsing of the wiki markup is not perfect, but sufficient for our purposes
    # contributions to improving this code are welcome :)
    text = to_unicode(raw, 'utf8', errors='ignore')
    text = decode_htmlentities(text)  # '&amp;nbsp;' --> '\xa0'
    markup_removed = remove_markup(text, promote_remaining, simplify_links)
    return markup_removed


def remove_markup(text, promote_remaining=True, simplify_links=True):
    """Filter out wiki markup from `text`, leaving only text.
    Parameters
    ----------
    text : str
        String containing markup.
    promote_remaining : bool
        Whether uncaught markup should be promoted to plain text.
    simplify_links : bool
        Whether links should be simplified keeping only their description text.
    Returns
    -------
    str
        `text` without markup.
    """
    text = re.sub(RE_P2, '', text)  # remove the last list (=languages)
    # the wiki markup is recursive (markup inside markup etc)
    # instead of writing a recursive grammar, here we deal with that by removing
    # markup in a loop, starting with inner-most expressions and working outwards,
    # for as long as something changes.
    text = remove_template(text)
    text = remove_file(text)
    iters = 0
    while True:
        old, iters = text, iters + 1
        text = re.sub(RE_P0, '', text)  # remove comments
        text = re.sub(RE_P1, '', text)  # remove footnotes
        text = re.sub(RE_P9, '', text)  # remove outside links
        text = re.sub(RE_P10, '', text)  # remove math content
        text = re.sub(RE_P11, '', text)  # remove all remaining tags
        text = re.sub(RE_P14, '', text)  # remove categories
        text = re.sub(RE_P5, '\\3', text)  # remove urls, keep description

        if simplify_links:
            text = re.sub(RE_P6, '\\2', text)  # simplify links, keep description only
        # remove table markup
        text = text.replace("!!", "\n|")  # each table head cell on a separate line
        text = text.replace("|-||", "\n|")  # for cases where a cell is filled with '-'
        text = re.sub(RE_P12, '\n', text)  # remove formatting lines
        text = text.replace('|||', '|\n|')  # each table cell on a separate line(where |{{a|b}}||cell-content)
        text = text.replace('||', '\n|')  # each table cell on a separate line
        text = re.sub(RE_P13, '\n', text)  # leave only cell content
        text = re.sub(RE_P17, '\n', text)  # remove formatting lines

        # remove empty mark-up
        text = text.replace('[]', '')
        # stop if nothing changed between two iterations or after a fixed number of iterations
        if old == text or iters > 2:
            break

    if promote_remaining:
        text = text.replace('[', '').replace(']', '')  # promote all remaining markup to plain text

    return text


def remove_template(s):
    """Remove template wikimedia markup.
    Parameters
    ----------
    s : str
        String containing markup template.
    Returns
    -------
    str
        Сopy of `s` with all the `wikimedia markup template <http://meta.wikimedia.org/wiki/Help:Template>`_ removed.
    Notes
    -----
    Since template can be nested, it is difficult remove them using regular expressions.
    """
    # Find the start and end position of each template by finding the opening
    # '{{' and closing '}}'
    n_open, n_close = 0, 0
    starts, ends = [], [-1]
    in_template = False
    prev_c = None
    for i, c in enumerate(s):
        if not in_template:
            if c == '{' and c == prev_c:
                starts.append(i - 1)
                in_template = True
                n_open = 1
        if in_template:
            if c == '{':
                n_open += 1
            elif c == '}':
                n_close += 1
            if n_open == n_close:
                ends.append(i)
                in_template = False
                n_open, n_close = 0, 0
        prev_c = c

    # Remove all the templates
    starts.append(None)
    return ''.join(s[end + 1:start] for end, start in zip(ends, starts))


def remove_file(s):
    """Remove the 'File:' and 'Image:' markup, keeping the file caption.
    Parameters
    ----------
    s : str
        String containing 'File:' and 'Image:' markup.
    Returns
    -------
    str
        Сopy of `s` with all the 'File:' and 'Image:' markup replaced by their `corresponding captions
        <http://www.mediawiki.org/wiki/Help:Images>`_.
    """
    # The regex RE_P15 match a File: or Image: markup
    for match in re.finditer(RE_P15, s):
        m = match.group(0)
        caption = m[:-2].split('|')[-1]
        s = s.replace(m, caption, 1)
    return s

def _job(current_lines:list):
    _wiki_word_dict = Counter()
    etree_str = etree.fromstring(b''.join(current_lines).decode())
    text_filtered = filter_wiki(raw=("".join(etree_str.xpath('//text()'))))
    for l in text_filtered.split('\n'):
        if len(l) > 60 and not re.match('^(\*|=+).*',l):
            cleaned_line_1 = re.sub("'+([A-Za-z]+)'+", r'\1', l)
            cleaned_line_2 = re.sub("[()]", "", cleaned_line_1)
            cleaned_line_3 = re.sub('[,.:;?!]','',cleaned_line_2).lower()
            cleaned_line_4 = re.sub('["\']','',cleaned_line_3)
            _wiki_word_dict.update(Counter(cleaned_line_4.split()))
    return _wiki_word_dict

if __name__ == '__main__':
    program = os.path.basename(sys.argv[0].split('.')[0])
    # check and process input arguments
    outp = sys.argv[2]
    inp = sys.argv[1]
    #cpu_count = multiprocessing.cpu_count()
    concurrent_tasks = 16  # selected ad-hoc, increase if you want..
    online = 'online' in program
    debug = 'nodebug' not in program
    import uuid

    wiki_word_dict = Counter()

    count=0
    articles=0
    xml_lines = []
    started = False
    active_jobs=[]

    with ProcessPoolExecutor(max_workers=concurrent_tasks) as executor, bz2.BZ2File(inp, "rb") as f:
        while True:
            try:
                line = f.readline()
                count+=1
            except Exception as e:
                print(e)
                break
            if b"<page>\n" in line:
                # start of page (article)
                started = True
                xml_lines.append(line)
            elif b"</page>\n" in line:
                articles+=1
                #end of page (article)
                started = False
                xml_lines.append(line)
                if len(active_jobs) < concurrent_tasks:
                    active_jobs.append(executor.submit(_job, xml_lines))
                elif len(active_jobs) == concurrent_tasks:
                    while True:
                        for active_job_idx in range(len(active_jobs)):
                            if active_jobs[active_job_idx].done():
                                wiki_word_dict.update(active_jobs[active_job_idx].result())
                                break
                        active_jobs.pop(active_job_idx)
                        break
                xml_lines = []
            elif started:
                xml_lines.append(line)

            if not line:
                break

            # Every 500,000 lines, dump hapax legomenon, and continue processing
            if ((count % 500000) == 0):
                print(f"processed lines: {count:,} articles: {articles:,}, words: {len(wiki_word_dict):,}")
                wiki_word_dict = Counter({k:v for k,v in wiki_word_dict.items() if v>1})
                print(len(wiki_word_dict))
                print("Writing to file wiki_dict", end=' ')
                with open(outp, 'w') as f_partial:
                    for k,v in  wiki_word_dict.most_common():
                        f_partial.write("{} {}\n".format(k,v) )
