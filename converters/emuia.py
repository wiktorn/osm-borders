import logging

import requests
import tqdm

__log = logging.getLogger(__name__)


# start here:
# http://mapy.geoportal.gov.pl/wss/service/SLN/guest/sln/woj.html


def get_emuia_slo(nazwa_slo, id_slo, id_obj):
    return requests.get(
        "http://mapy.geoportal.gov.pl/wss/service/SLN/guest/sln/{}/{}/{}/pel.json".format(
            nazwa_slo,
            id_slo,
            id_obj
        )
    ).json()


def get_addresses(terc):
    r = requests.get('http://mapy.geoportal.gov.pl/wss/service/SLN/guest/sln/woj.json')
    woj = [x for x in r.json()['jednAdms'] if x['jednAdm']['wojIdTeryt'] == terc[:2]]
    if len(woj) != 1:
        raise ValueError("No wojewodztwo found for terc: {}. Objects found: {}".format(
            terc[:2],
            len(woj)
        ))

    r = get_emuia_slo("slo", woj[0]['jednAdm']['wojIIPPn'], woj[0]['jednAdm']['wojIIPId'])
    powiat = [x for x in r['jednAdms'] if x['jednAdm']['powIdTeryt'] == terc[:4]]
    if len(powiat) != 1:
        raise ValueError("No powiat found for terc: {}. Objects found: {}".format(terc[:2], len(powiat)))

    r = get_emuia_slo("gmi", powiat[0]['jednAdm']['powIIPPn'], powiat[0]['jednAdm']['powIIPId'])
    gmina = [x for x in r['jednAdms'] if x['jednAdm']['gmIdTeryt'] == terc]
    if len(gmina) != 1:
        raise ValueError("No gmina found for terc: {}. Objects found: {}".format(terc[:2], len(gmina)))

    r = get_emuia_slo("miejsc", gmina[0]['jednAdm']['gmIIPPn'], gmina[0]['jednAdm']['gmIIPId'])

    adresy = []
    __log.info("Fetching addresses for terc: %s", terc)
    for miejscowosc in tqdm.tqdm(r['miejscowosci']):
        adresy.extend(
            get_emuia_slo("adr/miejsc",
                          miejscowosc['miejscowosc']['miejscIIPPn'],
                          miejscowosc['miejscowosc']['miejscIIPId']
                          )['adresy']
        )

        for ulica in get_emuia_slo("ul",
                                   miejscowosc['miejscowosc']['miejscIIPPn'],
                                   miejscowosc['miejscowosc']['miejscIIPId']
                                   )['ulice']:
            adresy.extend(get_emuia_slo("adr/ul", ulica['ulica']['ulIIPPn'], ulica['ulica']['ulIIPId'])['adresy'])

    return adresy
