import requests
import time
import pandas as pd
import json


class DMAScraper:
    def __init__(self):
        pass

    def fetch_data(self, page=1):
        url = "https://dma.mst.dk/soeg/page"

        payload = {
            "page": str(page),
            "timestamp": str(int(time.time() * 1000)),
            "searched": "true",
            "valgtSoegning": "prae_Alle husdyrbrug",
            "soegningNavn": "",
            "fritekst": "",
            "adresse": "",
            "regionsnr": "",
            "postNr": "",
            "afstandIKm": "",
            "indenforKortudsnit": "false",
            "kortudsnitTopHoejre": "POINT(915960 6408560)",
            "kortudsnitBundVenstre": "POINT(404040 6051440)",
            "virksomhedsIdentifikator": "",
            "medtagInaktive": "false",
            "kunRisikovirksomheder": "false",
            "aktiviteter": ["VL20000112", "VL20000430", "VL00000430", "VL00001000", "VL00000431", "VL00000432", "VL00000433", "VL00001001", "VL00000492", "VL00000493", "VL00000494", "VL00000495", "VL00000491", "VL00000490", "VL00000437","VL00000496", "VL00000292", "VL00000470", "VL00000497","VL00000429"],
            "kunGodkendelsespligtige": "",
            "harMCPAnlaeg": "false",
            "harLCPAnlaeg": "false",
            "omfattetAfVOC": "false",
            "myndighedNavn": "",
            "harTilsyn": "false",
            "harAfgoerelse": "false",
            "harHaandhaevelse": "false",
            "periodeFra": "",
            "periodeTil": "",
            "sortering": "NAVN",
            "sortAscending": "true",
            "visFritekst": "false",
            "visMyndighed": "false",
            "visVirksomhed": "true",
            "visBeliggenhed": "true",
            "visOffentliggoerelser": "false",
            "empty": "false"
        }
        response = requests.post(url, data=payload)
        return response.json()

    def extract_info(self, data):
        results = []
        for item in data['resultater']:
            miljoeaktoer = item['miljoeaktoer']
            results.append({
                'miljoeaktoerUrl': item['miljoeaktoerUrl'],
                'myndighedUrl': item['myndighedUrl'],
                'navn': miljoeaktoer['navn'],
                'cvr': miljoeaktoer['cvr'],
                'chr': miljoeaktoer['chr'],
                'pnr': miljoeaktoer['pnr'],
                'mstNoegle': miljoeaktoer['mstNoegle'],
                'fuldAdresse': item['fuldAdresse'],
                'bynavn': miljoeaktoer['bynavn'],
                'postNummer': miljoeaktoer['postNummer'],
                'vejnavn': miljoeaktoer['vejnavn'],
                'husNummer': miljoeaktoer['husNummer'],
                'hovedaktivitetKode': miljoeaktoer['hovedaktivitetKode'],
                'hovedaktivitetTekst': miljoeaktoer['hovedaktivitetTekst'],
                'miljoeaktoerGruppeKode': miljoeaktoer['miljoeaktoerGruppeKode'],
                'miljoeaktoerGruppeTekst': miljoeaktoer['miljoeaktoerGruppeTekst'],
                'godkendelsespligtig': miljoeaktoer['godkendelsespligtig'],
                'risikovirksomhed': miljoeaktoer['risikovirksomhed'],
                'stedfaestelse': miljoeaktoer['stedfaestelse'],
                'ansvarligMyndighed': miljoeaktoer['ansvarligeMyndigheder'][0]['navn'] if miljoeaktoer['ansvarligeMyndigheder'] else None,
                'ansvarligMyndighedKnr': miljoeaktoer['ansvarligeMyndigheder'][0]['knr'] if miljoeaktoer['ansvarligeMyndigheder'] else None
            })
        return results
    



