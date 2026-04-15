# Datenquellen

Dieses Dokument fasst die dynamischen Inhalte für Woladen in Markdown zusammen und ergänzt die Mobilithek-Sektion um den am 14. April 2026 verifizierten Stand unseres Mobilithek-Kontos.

## Ladeinfrastrukturdaten

Woladen stützt sich auf Ladeinfrastrukturdaten im Rahmen von [EU-Verordnung 2023/1804 (AFIR), Artikel 20](https://eur-lex.europa.eu/legal-content/DE/TXT/HTML/?uri=CELEX:32023R1804#art_20). Seit dem 14. April 2025 müssen Betreiber öffentlich zugänglicher Ladepunkte die statischen und dynamischen Daten kostenfrei über nationale Zugangspunkte bereitstellen.

### Deutschland: Mobilithek, Stand 14.4.2026

Der nationale Zugangspunkt für Deutschland ist die [Mobilithek](https://mobilithek.info/).

Woladen.de nutzt für Deutschland auf diese Mobilithek-Angebote:

- [chargecloud GmbH: AFIR-recharging-stat-chargecloud-json](https://mobilithek.info/offers/978597062404620288) (CC0)
- [Eco-Movement: AFIR-recharging-stat-Eco-Movement-v3 (JSON)](https://mobilithek.info/offers/954064102947180544) und [AFIR-recharging-dyn-Eco-Movement-v2 (JSON)](https://mobilithek.info/offers/955166494396665856) (CC BY 4.0)
- [EnBW AG: AFIR-recharging-stat-EnBWmobility+](https://mobilithek.info/offers/907574882292453376) (CC BY 4.0)
- [msu solutions GmbH: AFIR-recharging-stat-m8mit](https://mobilithek.info/offers/970305056590979072)
- [Qwello Deutschland GmbH: AFIR-recharging-stat-Qwello-Deutschland-GmbH](https://mobilithek.info/offers/972963216296222720) (CC0)
- [Wirelane GmbH: AFIR-recharging-stat-Wirelane](https://mobilithek.info/offers/869246425829892096) (CC0)
- [Monta ApS](https://mobilithek.info/offers/859435593654755328)
- [Stadtwerke Ulm](https://mobilithek.info/offers/854410608351543296) (CC0). 
- [ladenetz.de Ladestationsdaten - statisch](https://mobilithek.info/offers/902547569133924352) (CC0). Der zugehörige [ladenetz.de-Datensatz - dynamisch](https://mobilithek.info/offers/903240716507836416) 

Ergänzung für Woladen auf Basis des eingeloggten Mobilithek-Kontos vom 14. April 2026:

- In der Mobilithek wurden `35` Abonnements angezeigt.
- Für Woladen werden nur Abonnements mit Status `Aktiv` berücksichtigt.
- Daraus ergeben sich `25` aktive Abonnements bzw. `24` eindeutige aktive Datenangebote.

Aktive, für Woladen nutzbare Mobilithek-Angebote:

- 800 Volt Technologies GmbH: `AFIR-recharging-stat-PUMP`
- ELU Mobility: `AFIR-recharging-dyn-elu-mobility`
- EnBW mobility+ AG & Co. KG: `AFIR-recharging-dyn-EnBWmobility+`, `AFIR-recharging-stat-EnBWmobility+`
- Monta ApS: `AFIR-recharging-stat-MONTA`
- Qwello Deutschland GmbH: `AFIR-recharging-dyn-Qwello-Deutschland-GmbH`, `AFIR-recharging-stat-Qwello-Deutschland-GmbH`
- SMATRICS GmbH & Co KG: `AFIR-recharging-dyn-SMATRICS`, `AFIR-recharging-stat-SMATRICS`
- Smartlab Innovationsgesellschaft mbH: `ladenetz.de Ladestationsdaten - dynamisch` (aktiv seit 15.09.2025), `ladenetz.de Ladestationsdaten - statisch` (aktiv seit 15.09.2025)
- Tesla Germany GmbH: `AFIR-recharging-dyn-Tesla`, `AFIR-recharging-stat-Tesla`
- Wirelane GmbH: `AFIR-recharging-dyn-Wirelane`, `AFIR-recharging-stat-Wirelane`
- chargecloud GmbH: `[deprecated] AFIR-recharging-dyn-chargecloud-json`, `AFIR-recharging-dyn-chargecloud-json`, `AFIR-recharging-stat-chargecloud-json`
- eRound: `AFIR-recharging-dyn-eRound`, `AFIR-recharging-stat-eRound`
- eliso GmbH: `eliso AFIR Dynamic Data (Station & Point)`, `eliso AFIR Static Data (Station & Point)`
- vaylens GmbH: `AFIR-recharging-dyn-vaylens GmbH`, `AFIR-recharging-stat-vaylens GmbH`


### Schweiz

TODO, nicht angebunden. Sieht gut aus: https://opendata.swiss/de/dataset/ladestationen


## EU

See [List of National Access Points (NAP)](https://transport.ec.europa.eu/document/download/963c997d-efd9-40ae-a38b-5d4b935bdfcf_en?filename=its-national-access-points.pdf) 

### Österreich: E-Control

TODO, nicht angebunden. Für Österreich gibt es das zentrale Ladestellenverzeichnis der [E-Control](https://www.e-control.at/). Die Standorte und Detailinformationen werden dort von den Betreibern selbst gepflegt; E-Control weist darauf hin, dass für Richtigkeit, Vollständigkeit und Aktualität keine Haftung übernommen wird. 

### Belgien

TODO, nicht angebunden. [NAP](https://transportdata.be/en/)


### Niederlande: NDW

TODO, nicht angebunden. In den Niederlanden ist der Zugangspunkt [NDW / opendata.ndw.nu](https://opendata.ndw.nu).

### Finnland: Digitraffic

TODO, nicht angebunden. Für Finnland gibt es [Digitraffic](https://www.digitraffic.fi/en/road-traffic/afir/).

### Slowenien: NAP.si

TODO, nicht angebunden. Der nationale Zugangspunkt für Slowenien ist [NAP.si](https://nap.si/en). Laut Hocsy melden CPOs ihre Daten dort per OCPI oder REST-API.

### Griechenland: gov.gr

TODO, nicht angebunden. Für Griechenland gibt es vom [Ministerium für Infrastruktur und Verkehr](https://electrokinisi.yme.gov.gr) bereitgestellte Portal.

## Kartenmaterial

### OpenStreetMap

Das Kartenmaterial stammt von [OpenStreetMap](https://www.openstreetmap.org). Fehler können über [OpenStreetMap: Fix the map](https://www.openstreetmap.org/fixthemap) korrigiert oder gemeldet werden.
