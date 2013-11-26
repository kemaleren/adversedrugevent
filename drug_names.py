import requests
import urllib2
from xml.etree import ElementTree
import re
import jellyfish
import numpy as np

# REST API of RxNav
URL = "http://rxnav.nlm.nih.gov/REST/"


def _get_request(url):
    """get and parse a REST request"""
    r = requests.get(url)
    if r.status_code != 200:
        raise Exception('html error. status: {}'.format(r.status_code))
    tree = ElementTree.fromstring(r.content)
    return tree


def approx_cui(drug):
    term = urllib2.quote(drug)
    url = URL + "approximateTerm?term={}&maxEntries=1".format(term)
    tree = _get_request(url)
    try:
        cand = tree.find('approximateGroup').find('candidate')
        rxcui = cand.find('rxcui').text
        return rxcui
    except:
        return None


def map_drug_name_local(drug, db):
    cur = db.cursor()
    cur.execute("""select RXCUI from RXNCONSO where STR like "%{}%" limit 1;""".format(drug))
    results = cur.fetchone()
    cur.close()
    return results is not None


def cui_to_canonical(cui, db):
    """Build a canonical name for this RXCUI"""
    pass


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
    """Returns the RxNorm precise ingredient for a drug."""
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
