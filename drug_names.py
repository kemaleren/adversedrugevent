import requests
import urllib2
from xml.etree import ElementTree

# REST API of RxNav
URL = "http://rxnav.nlm.nih.gov/REST/"


def _get_request(url):
    """get and parse a REST request"""
    r = requests.get(url)
    if r.status_code != 200:
        raise Exception('html error. status: {}'.format(r.status_code))
    tree = ElementTree.fromstring(r.content)
    return tree


def _get_tty_name(tree, tty):
    """returns the name of a term type"""
    for child in tree[0]:
        try:
            if child.find('tty').text == tty:
                return child.find('conceptProperties').find('name').text
        except:
            pass
    return None


def _name_from_rxcui(rxcui, tty):
    url = URL + "rxcui/{}/related?tty={}".format(rxcui, tty)
    tree = _get_request(url)
    return _get_tty_name(tree, tty)


def map_drug_name(drug):
    """Returns the RxNorm precise ingredient for a drug."""
    # TODO: what if search term matches multiple drugs?
    term = urllib2.quote(drug)
    url = URL + "approximateTerm?term={}&maxEntries=1".format(term)
    tree = _get_request(url)
    try:
        cand = tree.find('approximateGroup').find('candidate')
        rxcui = cand.find('rxcui').text
        # TODO: do this from RxNorm database
        return _name_from_rxcui(rxcui, "PIN")
    except:
        return None


def get_faers_drugs(db):
    """Fetch all unique drugs from the database.

    ``db`` should be the result of MySqldb.connect().

    """
    cur = db.cursor()
    cur.execute("select distinct drugName from aers_ascii_drug")
    drugs = cur.fetchall()
    return list(r[0] for r in drugs)
