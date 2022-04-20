#!/bin/bash
curl "https://kaikki.org/dictionary/raw-wiktextract-data.json" \
	| jq -a -s -c '. | select(.lang_code=="en")? | { word: .word, related: .related, derived: .derived, forms: .forms, categories: .categories, pos: .pos, head_templates: .head_templates, senses: .senses, etymology_text: .etymology_text, etymology_templates: .etymology_templates}' > kaikki_wiki_dict.json
