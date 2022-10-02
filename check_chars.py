import sys
import json
from pathlib import Path
#from itertools import takewhile, islice, count
from ruamel.yaml import YAML
import re
from assets import (
    CONSONANTS,
    AFFRICATES,
    SHORT_VOWELS,
    LONG_VOWELS,
    DIPTHONGS,
    REMOVE,
)

PHONEMES = CONSONANTS | AFFRICATES | SHORT_VOWELS | LONG_VOWELS | DIPTHONGS
regex_phonemes = re.compile('('+'|'.join(sorted(list(PHONEMES), key=lambda x: len(x), reverse=True))+')')

# 2 chars
dipthongs_match = re.compile(f"({'|'.join(DIPTHONGS)})", flags=0)
long_vowels_match = re.compile(f"({'|'.join(LONG_VOWELS)})", flags=0)
affricates_match = re.compile(f"({'|'.join(AFFRICATES)})", flags=0)

# 1 char
short_vowels_match = re.compile(f"[{''.join(SHORT_VOWELS)}]", flags=0)
consonants_match = re.compile(f"[{''.join(CONSONANTS)}]", flags=0)

# 1st char is biphone
first_char_biphone = re.compile(
    f"({'|'.join([a[0] for a in AFFRICATES])}|" + \
    f"{'|'.join([l[0] for l in LONG_VOWELS])}|" + \
    f"{'|'.join([d[0] for d in DIPTHONGS])})",
    flags=0
)

is_biphone = re.compile(
    f"({'|'.join(AFFRICATES)}|" + \
    f"{'|'.join(LONG_VOWELS)}|" + \
    f"{'|'.join(DIPTHONGS)})",
    flags=0
)

VOWELS = SHORT_VOWELS | LONG_VOWELS | DIPTHONGS

def check_char(entry):
    if '[' in entry.get('ipa'):
        error = True
    ipa = entry.get('ipa').strip('/')
    ipa = ipa.replace(".", "").replace('(ɹ)','').replace('(ə)','')

    ipa_all = [[ipa[c], c, c+1] for c in range(0, len(ipa))]
    last_was_biphone = False
    biphone_0 = None

    phones = []
    error = False

    for c in ipa_all:
        c[0] = c[0].replace("ɚ", "ə")

        if last_was_biphone:
            last_was_biphone = False
            if is_biphone.match(last_c+c[0]):
                phones.append(last_c+c[0])
            elif regex_phonemes.match(c[0]):
                phones.append(last_c)
                phones.append(c[0])
            else:
                error = True
        elif first_char_biphone.match(c[0]) and ipa_all[-1][0] != c[0]:
            last_was_biphone = True
        else:
            phones.append(c[0])
        last_c = c[0]
        
    phones_str = ' '.join(phones) 

    CONS_OR_AFF = '(['+''.join(CONSONANTS)+']|'+'|'.join(AFFRICATES)+')'

    phones_str1 = re.sub(fr"( |^)([ˌˈ]) {CONS_OR_AFF} {CONS_OR_AFF} {CONS_OR_AFF} {CONS_OR_AFF} ({'|'.join(VOWELS)})", r"\1\3 \4 \5 \6 \2\7", phones_str)
    phones_str2 = re.sub(fr"( |^)([ˌˈ]) {CONS_OR_AFF} {CONS_OR_AFF} {CONS_OR_AFF} ({'|'.join(VOWELS)})", r"\1\3 \4 \5 \2\6", phones_str1)
    phones_str3 = re.sub(fr"( |^)([ˌˈ]) {CONS_OR_AFF} {CONS_OR_AFF} ({'|'.join(VOWELS)})", r"\1\3 \4 \2\5", phones_str2)
    phones_str4 = re.sub(fr"( |^)([ˌˈ]) {CONS_OR_AFF} ({'|'.join(VOWELS)})", r"\1\3 \2\4", phones_str3)
    phones_str5 = phones_str4.strip(' ').replace('  ', ' ')
    phones_str6 = re.sub(fr"[{REMOVE}]", "", phones_str5)
    phones_str7 = phones_str6.replace("ˌ ","ˌ").replace("ˈ ","ˈ")
    phones_str8 = phones_str7.replace("d ʒ", "dʒ").replace("t ʃ", "tʃ")

    final_check = (
        (
            set(
                DIPTHONGS | LONG_VOWELS | SHORT_VOWELS | AFFRICATES | CONSONANTS
            ) & set(
                re.sub('[ˈˌ]','',phones_str7).split())
        ) == set(re.sub('[ˈˌ]','',phones_str7).split())
    )

    if not final_check:
        error = True

    entry.update({'ipa': phones_str7})
    return error, entry


def readInChunks(stream, chunkSize=2048):
    """
    Lazy function to read a file piece by piece.
    Default chunk size: 2kB.

    """
    while True:
        data = stream.read(chunkSize)
        if not data:
            break
        return data

if __name__ == "__main__":
    entries = []
    counter = 0
    while True: 
        chunk = sys.stdin.readline()
        counter+=1
        if not chunk:
            break
        try:
            entry = json.loads(chunk)
            word = entry.get("word")
            # def: Each word should be an actual word with 0 spaces
            if not re.match("^[a-zA-Z]*[-.']*[a-zA-Z]+$", word):
                continue
            sounds = []
            # clean sounds
            for s in entry.get('sounds'):
                if s.get('ipa'):
                    rejected, data = check_char(
                        {
                            'ipa':s.get('ipa'),
                            'tags':s.get('tags')
                        }
                    )
                    if rejected:
                        break
                    else:
                        sounds.append(data)
            senses = []
            for s in entry.get('senses'):
                sense = s
                if sense.get('raw_glosses'):
                    del sense['raw_glosses']
                if sense.get('categories'):
                    del sense['categories']
                senses.append(sense)
            entry.update({"sounds":sounds})
            entry.update({"senses":senses})
            entries.append(entry)
        except Exception as e:
            continue

    wikipedia_list = [l.strip() for l in open(sys.argv[1], 'r').readlines()]

    # compress shared entries whereever possible
    wiki_dict = {}
    for loaded_line in entries:
        word = loaded_line.get("word")
        if not word in wikipedia_list:
            continue
        if word in wiki_dict.keys():
            pos_list = wiki_dict[word]
            del loaded_line["word"]  # use word as key
            loaded_line_shortened = {'ety':loaded_line.get('etymology_text'), 'pos':loaded_line.get('pos')}
            pos_list.append(loaded_line_shortened)
            wiki_dict.update({word:pos_list})
        else:
            del loaded_line["word"]  # use word as key
            wiki_dict.update({word:[{'ety':loaded_line.get('etymology_text'), 'pos':loaded_line.get('pos')}]})
    print(json.dumps(wiki_dict, indent=2))

