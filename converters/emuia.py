import requests
import tqdm
import logging


__log = logging.getLogger(__name__)


def get_addresses(terc):
    r = requests.get('http://mapy.geoportal.gov.pl/wss/service/SLN/guest/sln/woj.json')
    woj = [x for x in r.json()['jednAdms'] if x['jednAdm']['wojIdTeryt'] == terc[:2]]
    if len(woj) != 1:
        raise ValueError("No wojewodztwo found for terc: {}. Objects found: {}".format(terc[:2]), len(woj))

    r = requests.get('http://mapy.geoportal.gov.pl/wss/service/SLN/guest/sln/pow/{}/{}/pel.json'.format(
        woj[0]['jednAdm']['wojIIPPn'],
        woj[0]['jednAdm']['wojIIPId']
    ))
    powiat = [x for x in r.json()['jednAdms'] if x['jednAdm']['powIdTeryt'] == terc[:4]]
    if len(powiat) != 1:
        raise ValueError("No powiat found for terc: {}. Objects found: {}".format(terc[:2], len(powiat)))

    r = requests.get('http://mapy.geoportal.gov.pl/wss/service/SLN/guest/sln/gmi/{}/{}/pel.json'.format(
        powiat[0]['jednAdm']['powIIPPn'],
        powiat[0]['jednAdm']['powIIPId']
    ))
    gmina = [x for x in r.json()['jednAdms'] if x['jednAdm']['gmIdTeryt'] == terc]
    if len(gmina) != 1:
        raise ValueError("No gmina found for terc: {}. Objects found: {}".format(terc[:2], len(gmina)))


    r = requests.get('http://mapy.geoportal.gov.pl/wss/service/SLN/guest/sln/miejsc/{}/{}/pel.json'.format(
        gmina[0]['jednAdm']['gmIIPPn'],
        gmina[0]['jednAdm']['gmIIPId']
    ))

    adresy = []
    __log.info("Fetching addresses for terc: %s", terc)
    for miejscowosc in tqdm.tqdm(r.json()['miejscowosci']):
        adresy.extend(requests.get('http://mapy.geoportal.gov.pl/wss/service/SLN/guest/sln/adr/miejsc/{}/{}/pel.json'.format(
            miejscowosc['miejscowosc']['miejscIIPPn'],
            miejscowosc['miejscowosc']['miejscIIPId']
        )).json()['adresy'])

    return adresy
