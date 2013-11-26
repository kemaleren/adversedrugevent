import requests
import urllib2
from xml.etree import ElementTree
import re
import jellyfish
import numpy as np

# REST API of RxNav
from suds.client import Client
url = "http://rxnav.nlm.nih.gov/RxNormDBService.xml"
rxnav = Client(url)


def approx_uis(drug):
    """Returns the CUI and AUI of first approximate match in RxNav"""
    result = rxnav.service.getApproximateMatch(drug, 1, 0)
    if len(result.rxMatchInfo) == 0:
        return None, None  # TODO: return unknown
    r = result.rxMatchInfo[0]
    return r.RXCUI, r.RXAUI


def exact_uis(drug, db):
    """Returns CUI and AUI of first exact match in RxNorm"""
    cur = db.cursor()
    n = cur.execute("""select RXCUI, RXAUI from RXNCONSO where STR like "%{}%" limit 1;""".format(drug))
    if n == 0:
        cur.close()
        return None, None
    result = cur.fetchone()
    cur.close()
    rxcui = int(result[0])
    rxaui = int(result[1])
    return rxcui, rxaui


def cui_to_canonical(cui, db):
    """Build a canonical name for this RXCUI"""
    cur = db.cursor()
    n = cur.execute("""select STR from RXNCONSO where RXCUI like "{}" and TTY like "PN" limit 1;""".format(cui))
    if n == 0:
        cur.close()
        return None
    cur.close()
    return cur.fetchone()


def prefix_match(n, rxn_terms, min_length=5):
    for i in range(len(n), min_length - 1, -1):
        prefix = n[:i]
        if prefix in rxn_terms:
            return prefix
    return ""


def approx_match(name, rxn_terms, len_limit=2):
    nchars = len(name)
    rxn_terms = list(n for n in rxn_terms
                     if nchars - len_limit <= len(n) <= nchars + len_limit)
    return min((jellyfish.levenshtein_distance(name, n), n)
               for n in rxn_terms)


def word_to_bigram_vector(word):
    result = np.zeros((26 * 26,), dtype=np.int)
    for i in range(len(word) - 1):
        a = ord(word[i]) - ord('a')
        b = ord(word[i + 1]) - ord('a')
        if (0 <= a < 26 and 0 <= b < 26):
            result[a * 26 + b] += 1
    return result


def match_vectors(word, targets, target_vectors, limit=2):
    nchars = len(word)
    targets, target_vectors = zip(*list((t, v)
                                        for t, v in zip(targets, target_vectors)
                                        if nchars - limit <= len(t) <= nchars + limit))
    vec = word_to_bigram_vector(word)
    idx = min((np.sum(np.abs(vec - v)), idx)
              for idx, v in enumerate(target_vectors))[1]
    return targets[idx]


def map_drug_name(drug, db):
    """Returns a precise name for each drug."""
    # TODO: what if search term matches multiple drugs?
    pass


def get_faers_drugs(db):
    """Fetch all unique drugs from the database.

    ``db`` should be the result of MySqldb.connect().

    """
    cur = db.cursor()
    cur.execute("select distinct drugName from aers_ascii_drug")
    drugs = cur.fetchall()
    cur.close()
    return list(r[0].lower() for r in drugs)


def get_rxn_terms(db):
    cur = db.cursor()
    cur.execute("select STR from RXNCONSO")
    matches = cur.fetchall()
    cur.close()
    return set(m[0].lower() for m in matches)


PATTERN = re.compile("\/[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]\/")


def clean_name(name):
    """Some names have a suffix like `/XXXX/`. Remove them.

    Remove extra space.

    """
    match = PATTERN.search(name)
    if match is not None:
        name = name[:match.start()] + name[match.end():]
    return ' '.join(name.split())
