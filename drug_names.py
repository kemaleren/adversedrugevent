from suds.client import Client

url = "http://rxnav.nlm.nih.gov/RxNormDBService.xml"
rxnav = Client(url)


def approx_cui(drug):
    """Returns the CUI of first approximate match in RxNav"""
    result = rxnav.service.getApproximateMatch(drug, 1, 0)
    if len(result.rxMatchInfo) == 0:
        return -1
    r = result.rxMatchInfo[0]
    return r.RXCUI


def cui_to_ingredient(cui):
    """Returns the first ingredient for this CUI"""
    result = rxnav.service.getRelatedByType(cui, ["IN"])
    if len(result) == 0:
        return -1, "UNKNOWN"
    r = result[0].rxConcept
    if len(r) == 0:
        return -1, "UNKNOWN"
    r = r[0]
    return r.RXCUI, r.STR


def map_drug_name(drug):
    """Returns the RXCUI for the approximate match, the RXCUI of the
    first ingredient, and the name of the first ingredient.

    """
    try:
        cui = approx_cui(drug)
        if cui == -1:
            return -1, -1, "UNKNOWN"
        ingred_cui, ingred = cui_to_ingredient(cui)
        return cui, ingred_cui, ingred
    except:
        return -1, -1, "UNKNOWN"


def get_faers_drugs(db):
    """Fetch all unique drugs from the database.

    ``db`` should be the result of MySqldb.connect().

    """
    cur = db.cursor()
    cur.execute("select distinct drugName from aers_ascii_drug")
    drugs = cur.fetchall()
    cur.close()
    return list(r[0].lower() for r in drugs)
