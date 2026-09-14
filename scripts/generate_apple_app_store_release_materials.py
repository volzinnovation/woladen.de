#!/usr/bin/env python3

"""Generate localized App Store summary material for iPhone release 1.4.0."""

from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_ROOT = ROOT / "output" / "app-store" / "ios"
SCREENSHOT_ROOT = OUTPUT_ROOT / "6.9-inch"
SUMMARY_JSON = OUTPUT_ROOT / "app-store-summary-1.4.0-build-16.json"

LANGUAGES = {
    "en": ("en-US", "Smart EV stops in Europe", "Berlin", "München"),
    "de": ("de-DE", "Bessere Ladestopps in Europa", "Berlin", "München"),
    "fr": ("fr-FR", "Pauses de recharge malines", "Paris", "Marseille"),
    "nl": ("nl-NL", "Slimme laadstops in Europa", "Amsterdam", "Rotterdam"),
    "da": ("da-DK", "Smarte ladestop i Europa", "København", "Aarhus"),
    "fi": ("fi-FI", "Älykkäät latauspysähdykset", "Helsinki", "Espoo"),
    "sv": ("sv-SE", "Smarta laddstopp i Europa", "Stockholm", "Göteborg"),
    "el": ("el-GR", "Έξυπνες στάσεις φόρτισης", "Αθήνα", "Θεσσαλονίκη"),
    "lv": ("lv-LV", "Viedas uzlādes pieturas", "Rīga", "Daugavpils"),
    "lt": ("lt-LT", "Išmanios įkrovimo stotelės", "Vilnius", "Kaunas"),
    "lb": ("lb-LU", "Intelligent Luedstopp", "Luxembourg", "Esch-sur-Alzette"),
    "mt": ("mt-MT", "Waqfiet intelliġenti EV", "Valletta", "Birkirkara"),
    "nb": ("nb-NO", "Smarte ladestopp i Europa", "Oslo", "Bergen"),
    "nn": ("nn-NO", "Smarte ladestopp i Europa", "Oslo", "Bergen"),
    "pl": ("pl-PL", "Inteligentne postoje EV", "Warszawa", "Kraków"),
    "pt": ("pt-PT", "Paragens EV inteligentes", "Lisboa", "Porto"),
    "es": ("es-ES", "Paradas EV inteligentes", "Madrid", "Barcelona"),
    "cs": ("cs-CZ", "Chytré nabíjecí zastávky", "Praha", "Brno"),
    "hu": ("hu-HU", "Okos töltési megállók", "Budapest", "Debrecen"),
    "sl": ("sl-SI", "Pametni polnilni postanki", "Ljubljana", "Maribor"),
    "it": ("it-IT", "Soste EV intelligenti", "Roma", "Milano"),
    "rm": ("rm-CH", "Fermadas EV intelligentas", "Bern", "Genève"),
    "tr": ("tr-TR", "Akıllı EV şarj molaları", "Ankara", "İstanbul"),
}

FALLBACK_SUMMARY = (
    "Find available and reliable chargers with better places to wait nearby, "
    "from bakeries and restaurants to shops, playgrounds, and cafés."
)

RELEASE_NOTES = {
    "en": "Version 1.4.0 (build 16) brings refined station details, clearer availability chips, route planning improvements, and a faster end-drive countdown.",
    "de": "Version 1.4.0 (Build 16) bringt übersichtlichere Stationsdetails, klarere Verfügbarkeits-Chips, Verbesserungen bei der Routenplanung und einen schnelleren Countdown zum Fahrtende.",
    "fr": "La version 1.4.0 (build 16) améliore les détails des stations, la disponibilité, la planification d’itinéraire et le compte à rebours de fin de trajet.",
    "nl": "Versie 1.4.0 (build 16) verbetert stationdetails, beschikbaarheidschips, routeplanning en het aftellen bij het beëindigen van een rit.",
    "da": "Version 1.4.0 (build 16) forbedrer stationsdetaljer, tilgængelighed, ruteplanlægning og nedtællingen ved turens afslutning.",
    "fi": "Versio 1.4.0 (build 16) selkeyttää latausasemien tietoja, saatavuusmerkintöjä, reittisuunnittelua ja ajon lopetuksen lähtölaskentaa.",
    "sv": "Version 1.4.0 (build 16) förbättrar stationsdetaljer, tillgänglighetschips, ruttplanering och nedräkningen när körningen avslutas.",
    "el": "Η έκδοση 1.4.0 (build 16) βελτιώνει τις λεπτομέρειες σταθμών, τη διαθεσιμότητα, τον σχεδιασμό διαδρομής και την αντίστροφη μέτρηση για το τέλος της διαδρομής.",
    "lv": "Versija 1.4.0 (build 16) uzlabo staciju informāciju, pieejamības apzīmējumus, maršruta plānošanu un brauciena beigšanas atpakaļskaitīšanu.",
    "lt": "1.4.0 versija (build 16) pagerina stočių informaciją, prieinamumo žymas, maršruto planavimą ir kelionės baigimo atgalinį skaičiavimą.",
    "lb": "Versioun 1.4.0 (Build 16) verbessert d’Stationsdetailer, d’Disponibilitéit, d’Routenplanung an de Countdown beim Ofschléisse vun enger Faart.",
    "mt": "Il-verżjoni 1.4.0 (build 16) ittejjeb id-dettalji tal-istazzjonijiet, id-disponibbiltà, l-ippjanar tar-rotta u l-countdown meta tispiċċa s-sewqan.",
    "nb": "Versjon 1.4.0 (build 16) forbedrer stasjonsdetaljer, tilgjengelighet, ruteplanlegging og nedtellingen når turen avsluttes.",
    "nn": "Versjon 1.4.0 (build 16) betrar stasjonsdetaljar, tilgjenge, ruteplanlegging og nedteljinga når turen blir avslutta.",
    "pl": "Wersja 1.4.0 (build 16) poprawia szczegóły stacji, oznaczenia dostępności, planowanie trasy i odliczanie przy kończeniu przejazdu.",
    "pt": "A versão 1.4.0 (build 16) melhora os detalhes das estações, a disponibilidade, o planeamento de rotas e a contagem decrescente ao terminar a viagem.",
    "es": "La versión 1.4.0 (build 16) mejora los detalles de las estaciones, la disponibilidad, la planificación de rutas y la cuenta atrás al finalizar el viaje.",
    "cs": "Verze 1.4.0 (build 16) vylepšuje podrobnosti stanic, dostupnost, plánování tras a odpočítávání při ukončení jízdy.",
    "hu": "Az 1.4.0-s verzió (build 16) javítja az állomásadatokat, az elérhetőségi jelzéseket, az útvonaltervezést és az út befejezésének visszaszámlálását.",
    "sl": "Različica 1.4.0 (build 16) izboljšuje podrobnosti postaj, razpoložljivost, načrtovanje poti in odštevanje pri koncu vožnje.",
    "it": "La versione 1.4.0 (build 16) migliora i dettagli delle stazioni, la disponibilità, la pianificazione del percorso e il conto alla rovescia alla fine del viaggio.",
    "rm": "La versiun 1.4.0 (build 16) meglierescha ils detagls da las staziuns, la disponibladad, la planisaziun da rutas e il countdown per terminar il viadi.",
    "tr": "1.4.0 sürümü (build 16) istasyon ayrıntılarını, uygunluk göstergelerini, rota planlamasını ve sürüşü bitirme geri sayımını geliştiriyor.",
}

PROMOTIONAL_TEXTS = {
    "en": "Find pleasant and reliable charging stations across Europe. Live occupancy status and route planning included.",
    "de": "Europa-weit angenehme und zuverlässige Ladestationen finden. Live-Belegungsstatus und Routenplanung inklusive.",
    "fr": "Trouvez des stations de recharge agréables et fiables partout en Europe. État d’occupation en direct et planification d’itinéraire inclus.",
    "nl": "Vind in heel Europa prettige en betrouwbare laadstations. Live bezettingsstatus en routeplanning inbegrepen.",
    "da": "Find behagelige og pålidelige ladestationer i hele Europa. Live-belægningsstatus og ruteplanlægning inkluderet.",
    "fi": "Löydä miellyttävät ja luotettavat latausasemat eri puolilta Eurooppaa. Live-varaustilanne ja reittisuunnittelu mukana.",
    "sv": "Hitta trevliga och pålitliga laddstationer i hela Europa. Live-beläggningsstatus och ruttplanering ingår.",
    "el": "Βρείτε άνετους και αξιόπιστους σταθμούς φόρτισης σε όλη την Ευρώπη. Ζωντανή κατάσταση διαθεσιμότητας και σχεδιασμός διαδρομής.",
    "lv": "Atrodiet ērtas un uzticamas uzlādes stacijas visā Eiropā. Tiešsaistes noslodzes statuss un maršruta plānošana iekļauta.",
    "lt": "Raskite patogias ir patikimas įkrovimo stoteles visoje Europoje. Tiesioginė užimtumo būsena ir maršruto planavimas įtraukti.",
    "lb": "Fannt uechter ganz Europa agreabel an zouverlässeg Opluedstatiounen. Live-Beleeungsstatus a Routenplanung inklusiv.",
    "mt": "Sib stazzjonijiet tal-iċċarġjar komdi u affidabbli madwar l-Ewropa. Status tal-okkupanza live u ppjanar tar-rotta inklużi.",
    "nb": "Finn behagelige og pålitelige ladestasjoner i hele Europa. Live-belegningsstatus og ruteplanlegging inkludert.",
    "nn": "Finn behagelege og pålitelege ladestasjonar i heile Europa. Live-belegningsstatus og ruteplanlegging inkludert.",
    "pl": "Znajdź wygodne i niezawodne stacje ładowania w całej Europie. Status zajętości na żywo i planowanie trasy w zestawie.",
    "pt": "Encontre estações de carregamento agradáveis e fiáveis em toda a Europa. Estado de ocupação em direto e planeamento de rotas incluídos.",
    "es": "Encuentra estaciones de carga agradables y fiables en toda Europa. Estado de ocupación en directo y planificación de rutas incluidos.",
    "cs": "Najděte příjemné a spolehlivé nabíjecí stanice po celé Evropě. Živý stav obsazenosti a plánování trasy v ceně.",
    "hu": "Találjon kellemes és megbízható töltőállomásokat Európa-szerte. Élő foglaltsági állapot és útvonaltervezés mellékelve.",
    "sl": "Poiščite prijetne in zanesljive polnilne postaje po vsej Evropi. Stanje zasedenosti v živo in načrtovanje poti sta vključena.",
    "it": "Trova stazioni di ricarica piacevoli e affidabili in tutta Europa. Stato di occupazione in tempo reale e pianificazione del percorso inclusi.",
    "rm": "Chattas staziuns da chargiar agreablas e fidaivlas en l’entira Europa. Status d’occupaziun live e planisaziun da rutas inclus.",
    "tr": "Avrupa genelinde konforlu ve güvenilir şarj istasyonları bulun. Canlı doluluk durumu ve rota planlama dahil.",
}

DESCRIPTION_TEXTS = {
    "en": "woladen helps you find better charging breaks across Europe. Discover available, reliable and pleasant charging stations with cafés, bakeries, restaurants, shops, toilets, playgrounds and other places nearby. Search the map or list, filter by availability, power, connector, provider and amenities, save favorites, and plan longer journeys with stations along your route. Live status is shown where available. No account, ads or in-app purchases. Favorites stay on your device.",
    "de": "woladen hilft dir, europaweit bessere Ladepausen zu finden. Entdecke verfügbare, zuverlässige und angenehme Ladestationen mit Cafés, Bäckereien, Restaurants, Läden, Toiletten, Spielplätzen und anderen Orten in der Nähe. Suche auf Karte oder Liste, filtere nach Verfügbarkeit, Leistung, Stecker, Betreiber und Ausstattung, speichere Favoriten und plane längere Fahrten mit Stationen entlang deiner Route. Live-Status gibt es, wo verfügbar. Kein Konto, keine Werbung und keine In-App-Käufe.",
    "fr": "woladen vous aide à trouver de meilleures pauses de recharge partout en Europe. Découvrez des stations disponibles, fiables et agréables, avec cafés, boulangeries, restaurants, commerces, toilettes, aires de jeux et autres lieux à proximité. Recherchez sur la carte ou dans la liste, filtrez par disponibilité, puissance, connecteur, opérateur et équipements, enregistrez vos favoris et planifiez vos trajets avec des stations sur l’itinéraire. Le statut en direct est affiché lorsqu’il est disponible. Sans compte, publicité ni achat intégré.",
    "nl": "woladen helpt je in heel Europa betere laadpauzes te vinden. Ontdek beschikbare, betrouwbare en prettige laadstations met cafés, bakkerijen, restaurants, winkels, toiletten, speeltuinen en andere locaties in de buurt. Zoek op de kaart of in de lijst, filter op beschikbaarheid, vermogen, stekker, provider en voorzieningen, bewaar favorieten en plan langere ritten met stations langs je route. De live status wordt getoond waar die beschikbaar is. Geen account, advertenties of in-app aankopen.",
    "da": "woladen hjælper dig med at finde bedre ladepauser i hele Europa. Find tilgængelige, pålidelige og behagelige ladestationer med caféer, bagerier, restauranter, butikker, toiletter, legepladser og andre steder i nærheden. Søg på kortet eller i listen, filtrer efter tilgængelighed, effekt, stik, udbyder og faciliteter, gem favoritter, og planlæg længere ture med stationer langs ruten. Live-status vises, når den er tilgængelig. Ingen konto, reklamer eller køb i appen.",
    "fi": "woladen auttaa löytämään parempia lataustaukoja kaikkialla Euroopassa. Löydä käytettävissä olevia, luotettavia ja viihtyisiä latausasemia, joiden lähellä on kahviloita, leipomoita, ravintoloita, kauppoja, vessoja, leikkipuistoja ja muita paikkoja. Hae kartalta tai listasta, suodata saatavuuden, tehon, liittimen, palveluntarjoajan ja palveluiden mukaan, tallenna suosikit ja suunnittele matkat reitin latausasemilla. Reaaliaikainen tila näkyy, kun se on saatavilla. Ei tiliä, mainoksia tai sovelluksen sisäisiä ostoja.",
    "sv": "woladen hjälper dig att hitta bättre laddpauser i hela Europa. Upptäck tillgängliga, pålitliga och trevliga laddstationer med kaféer, bagerier, restauranger, butiker, toaletter, lekplatser och andra platser i närheten. Sök på kartan eller i listan, filtrera efter tillgänglighet, effekt, kontakt, operatör och bekvämligheter, spara favoriter och planera längre resor med stationer längs rutten. Live-status visas när den finns. Inget konto, inga annonser och inga köp i appen.",
    "el": "Το woladen σας βοηθά να βρίσκετε καλύτερες στάσεις φόρτισης σε όλη την Ευρώπη. Ανακαλύψτε διαθέσιμους, αξιόπιστους και ευχάριστους σταθμούς φόρτισης, με καφέ, φούρνους, εστιατόρια, καταστήματα, τουαλέτες, παιδικές χαρές και άλλα μέρη κοντά σας. Αναζητήστε στον χάρτη ή στη λίστα, φιλτράρετε διαθεσιμότητα, ισχύ, βύσμα, πάροχο και παροχές, αποθηκεύστε αγαπημένα και σχεδιάστε ταξίδια με σταθμούς στη διαδρομή. Η ζωντανή κατάσταση εμφανίζεται όπου είναι διαθέσιμη. Χωρίς λογαριασμό, διαφημίσεις ή αγορές εντός εφαρμογής.",
    "lv": "woladen palīdz atrast labākas uzlādes pauzes visā Eiropā. Atrodiet pieejamas, uzticamas un patīkamas uzlādes stacijas ar tuvumā esošām kafejnīcām, maiznīcām, restorāniem, veikaliem, tualetēm, rotaļu laukumiem un citām vietām. Meklējiet kartē vai sarakstā, filtrējiet pēc pieejamības, jaudas, savienotāja, operatora un ērtībām, saglabājiet izlasi un plānojiet braucienus ar stacijām maršrutā. Tiešsaistes statuss redzams, ja tas ir pieejams. Nav konta, reklāmu vai pirkumu lietotnē.",
    "lt": "woladen padeda rasti geresnes įkrovimo pertraukas visoje Europoje. Atraskite prieinamas, patikimas ir malonias įkrovimo stoteles, šalia kurių yra kavinių, kepyklų, restoranų, parduotuvių, tualetų, žaidimų aikštelių ir kitų vietų. Ieškokite žemėlapyje arba sąraše, filtruokite pagal prieinamumą, galią, jungtį, operatorių ir patogumus, išsaugokite parankinius ir planuokite keliones su stotelėmis maršrute. Tiesioginė būsena rodoma, kai prieinama. Nereikia paskyros, nėra reklamų ar pirkinių programėlėje.",
    "lb": "woladen hëlleft Iech, uechter Europa besser Opluedpausen ze fannen. Entdeckt disponibel, zouverlässeg an agreabel Opluedstatioune mat Caféen, Bäckereien, Restauranten, Geschäfter, Toiletten, Spillplazen an anere Plazen an der Géigend. Sicht op der Kaart oder an der Lëscht, filtert no Disponibilitéit, Leeschtung, Stecker, Bedreiwer an Ariichtungen, späichert Favoritten a plangt Reesen mat Opluedstatiounen op der Route. De Live-Status gëtt gewisen, wann e verfügbar ass. Kee Kont, keng Reklammen a keng Akeef an der App.",
    "mt": "woladen jgħinek issib waqfiet aħjar għall-iċċarġjar madwar l-Ewropa. Skopri stazzjonijiet disponibbli, affidabbli u komdi, b’kafetteriji, fran, ristoranti, ħwienet, toilets, postijiet tal-logħob u postijiet oħra fil-qrib. Fittex fuq il-mappa jew fil-lista, iffiltra skont id-disponibbiltà, il-qawwa, il-konnettur, il-fornitur u l-kumditajiet, żomm il-favoriti u ppjana vjaġġi bi stazzjonijiet tul ir-rotta. L-istatus live jintwera fejn ikun disponibbli. M’hemmx kont, reklami jew xiri fl-app.",
    "nb": "woladen hjelper deg med å finne bedre ladepauser i hele Europa. Oppdag tilgjengelige, pålitelige og hyggelige ladestasjoner med kafeer, bakerier, restauranter, butikker, toaletter, lekeplasser og andre steder i nærheten. Søk på kartet eller i listen, filtrer etter tilgjengelighet, effekt, kontakt, operatør og fasiliteter, lagre favoritter og planlegg turer med ladestasjoner langs ruten. Live-status vises når den er tilgjengelig. Ingen konto, reklame eller kjøp i appen.",
    "nn": "woladen hjelper deg å finne betre ladepausar i heile Europa. Oppdag tilgjengelege, pålitelege og trivelege ladestasjonar med kafear, bakeri, restaurantar, butikkar, toalett, leikeplassar og andre stader i nærleiken. Søk på kartet eller i lista, filtrer etter tilgjenge, effekt, kontakt, operatør og fasilitetar, lagre favorittar og planlegg turar med ladestasjonar langs ruta. Live-status blir vist når han er tilgjengeleg. Ingen konto, reklamar eller kjøp i appen.",
    "pl": "woladen pomaga znaleźć lepsze przerwy na ładowanie w całej Europie. Odkrywaj dostępne, niezawodne i przyjemne stacje ładowania oraz pobliskie kawiarnie, piekarnie, restauracje, sklepy, toalety, place zabaw i inne miejsca. Szukaj na mapie lub liście, filtruj według dostępności, mocy, złącza, operatora i udogodnień, zapisuj ulubione i planuj trasy ze stacjami po drodze. Status na żywo jest widoczny, gdy jest dostępny. Bez konta, reklam i zakupów w aplikacji.",
    "pt": "A woladen ajuda a encontrar melhores pausas para carregar em toda a Europa. Descubra estações disponíveis, fiáveis e agradáveis, com cafés, padarias, restaurantes, lojas, casas de banho, parques infantis e outros locais por perto. Pesquise no mapa ou na lista, filtre por disponibilidade, potência, conector, operador e comodidades, guarde favoritos e planeie viagens com estações ao longo do percurso. O estado em direto aparece quando está disponível. Sem conta, publicidade ou compras na app.",
    "es": "woladen te ayuda a encontrar mejores pausas de carga en toda Europa. Descubre estaciones disponibles, fiables y agradables, con cafeterías, panaderías, restaurantes, tiendas, baños, parques infantiles y otros lugares cerca. Busca en el mapa o en la lista, filtra por disponibilidad, potencia, conector, operador y servicios, guarda favoritos y planifica viajes con estaciones a lo largo de la ruta. El estado en directo se muestra cuando está disponible. Sin cuenta, anuncios ni compras dentro de la app.",
    "cs": "woladen vám pomůže najít lepší přestávky na nabíjení po celé Evropě. Objevte dostupné, spolehlivé a příjemné nabíjecí stanice s kavárnami, pekárnami, restauracemi, obchody, toaletami, hřišti a dalšími místy v okolí. Hledejte na mapě nebo v seznamu, filtrujte podle dostupnosti, výkonu, konektoru, provozovatele a vybavení, ukládejte oblíbené a plánujte cesty se stanicemi na trase. Živý stav se zobrazí, pokud je dostupný. Bez účtu, reklam a nákupů v aplikaci.",
    "hu": "A woladen segít jobb töltési szüneteket találni Európa-szerte. Fedezzen fel elérhető, megbízható és kellemes töltőállomásokat a közelben lévő kávézókkal, pékségekkel, éttermekkel, üzletekkel, mosdókkal, játszóterekkel és más helyekkel. Keressen a térképen vagy a listában, szűrjön elérhetőség, teljesítmény, csatlakozó, üzemeltető és szolgáltatások szerint, mentse kedvenceit, és tervezzen utakat az útvonal menti állomásokkal. Az élő állapot elérhetőség esetén látható. Nincs fiók, reklám vagy alkalmazáson belüli vásárlás.",
    "sl": "woladen vam pomaga najti boljše postanke za polnjenje po vsej Evropi. Odkrijte razpoložljive, zanesljive in prijetne polnilne postaje s kavarnami, pekarnami, restavracijami, trgovinami, stranišči, igrišči in drugimi kraji v bližini. Iščite na zemljevidu ali seznamu, filtrirajte po razpoložljivosti, moči, priključku, ponudniku in opremi, shranite priljubljene ter načrtujte poti s postajami ob poti. Stanje v živo je prikazano, kadar je na voljo. Brez računa, oglasov in nakupov v aplikaciji.",
    "it": "woladen ti aiuta a trovare pause di ricarica migliori in tutta Europa. Scopri stazioni disponibili, affidabili e piacevoli, con bar, panetterie, ristoranti, negozi, servizi igienici, parchi giochi e altri luoghi nelle vicinanze. Cerca sulla mappa o nell’elenco, filtra per disponibilità, potenza, connettore, operatore e servizi, salva i preferiti e pianifica viaggi con stazioni lungo il percorso. Lo stato in tempo reale appare quando disponibile. Nessun account, pubblicità o acquisto in-app.",
    "rm": "woladen ta gida a chattar meglras pausas da chargiar en l’entira Europa. Chattas staziuns da chargiar disponiblas, fidaivlas ed agreablas, cun cafés, pasternarias, restaurants, butias, tualettas, plazzas da gieus ed auters lieus en la vischinanza. Tschertga sin la carta u en la glista, filtra tenor disponibladad, prestaziun, conector, gestiunari e comfort, memorisescha favuritas e planisescha viadis cun staziuns lungo la ruta. Il status live cumpara sch’el è disponibel. Nagina cuntrada, naginas reclamas e nagins cumpras en l’app.",
    "tr": "woladen, Avrupa genelinde daha iyi şarj molaları bulmanıza yardımcı olur. Yakınlarda kafe, fırın, restoran, mağaza, tuvalet, oyun alanı ve başka yerler bulunan uygun, güvenilir ve konforlu şarj istasyonlarını keşfedin. Haritada veya listede arayın, doluluk, güç, bağlantı türü, işletmeci ve olanaklara göre filtreleyin, favorileri kaydedin ve rota üzerindeki istasyonlarla yolculuk planlayın. Canlı durum mevcut olduğunda gösterilir. Hesap, reklam ve uygulama içi satın alma yok.",
}

KEYWORDS_TEXTS = {
    "en": "EV,charging,charger,fast charge,route,trip,map,live status,amenities",
    "de": "E-Auto,Laden,Ladesäule,Schnellladen,Elektroauto,Route,Karte,Live-Status,Café",
    "fr": "voiture électrique,recharge,borne,charge rapide,itineraires,carte,statut,café",
    "nl": "elektrische auto,laden,laadpaal,snelladen,route,kaart,live status,café",
    "da": "elbil,opladning,ladestation,lynladning,rute,kort,live-status,café",
    "fi": "sähköauto,lataus,latausasema,pikalataus,reitti,kartta,tila,kahvila",
    "sv": "elbil,laddning,laddstation,snabbladdning,rutt,karta,live-status,kafé",
    "el": "ηλεκτρικό αυτοκίνητο,φόρτιση,σταθμός,ταχεία φόρτιση,διαδρομή,χάρτης,κατάσταση",
    "lv": "elektroauto,uzlāde,uzlādes stacija,ātrā uzlāde,maršruts,karta,status,kafejnīca",
    "lt": "elektromobilis,įkrovimas,įkrovimo stotelė,greitas įkrovimas,maršrutas,žemėlapis",
    "lb": "Elektroauto,Oplueden,Opluedstatioun,Schnellopluedung,Route,Kaart,Live-Status,Café",
    "mt": "karozza elettrika,iċċarġjar,stazzjon,ċċarġjar veloċi,rotta,mappa,status,kafetterija",
    "nb": "elbil,lading,ladestasjon,hurtiglading,rute,kart,live-status,kafé",
    "nn": "elbil,lading,ladestasjon,hurtiglading,rute,kart,live-status,kafé",
    "pl": "auto elektryczne,ładowanie,ładowarka,szybkie ładowanie,trasa,mapa,status,kawiarnia",
    "pt": "carro elétrico,carregamento,estação,carga rápida,rota,mapa,estado,café",
    "es": "coche eléctrico,carga,cargador,carga rápida,ruta,mapa,estado,cafetería",
    "cs": "elektromobil,nabíjení,nabíječka,rychlé nabíjení,trasa,mapa,stav,kavárna",
    "hu": "elektromos autó,töltés,töltőállomás,gyorstöltés,útvonal,térkép,állapot,kávézó",
    "sl": "električni avto,polnjenje,polnilnica,hitro polnjenje,pot,zemljevid,stanje,kavarna",
    "it": "auto elettrica,ricarica,colonnina,ricarica rapida,percorso,mappa,stato,caffè",
    "rm": "auto electric,chargiar,staziun,chargiar svelt,ruta,carta,status,café",
    "tr": "elektrikli araç,şarj,şarj istasyonu,hızlı şarj,rota,harita,durum,kafe",
}


def localized_summary(language: str) -> str:
    if language == "en":
        return FALLBACK_SUMMARY
    overlay_path = ROOT / "web" / "i18n" / f"{language}.json"
    try:
        overlay = json.loads(overlay_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return FALLBACK_SUMMARY
    return overlay.get("seo", {}).get("productMessage", FALLBACK_SUMMARY)


def main() -> None:
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    metadata_root = OUTPUT_ROOT / "metadata"
    metadata_root.mkdir(parents=True, exist_ok=True)
    records = []

    for language, (store_locale, subtitle, origin, destination) in LANGUAGES.items():
        summary = localized_summary(language)
        promotional_text = PROMOTIONAL_TEXTS[language]
        description = DESCRIPTION_TEXTS[language]
        keywords = KEYWORDS_TEXTS[language]
        if len(promotional_text) > 170:
            raise ValueError(f"Promotional text for {language} exceeds Apple's 170-character limit")
        if len(description) > 4000:
            raise ValueError(f"Description for {language} exceeds Apple's 4000-character limit")
        if len(keywords) > 100:
            raise ValueError(f"Keywords for {language} exceed Apple's 100-character limit")
        locale_dir = metadata_root / store_locale
        locale_dir.mkdir(parents=True, exist_ok=True)
        values = {
            "name.txt": "woladen\n",
            "subtitle.txt": f"{subtitle}\n",
            "summary.txt": f"{summary}\n",
            "promotional-text.txt": f"{promotional_text}\n",
            "description.txt": f"{description}\n",
            "keywords.txt": f"{keywords}\n",
            "route.txt": f"{origin} → {destination}\n",
            "release-notes.txt": f"{RELEASE_NOTES[language]}\n",
        }
        for filename, value in values.items():
            (locale_dir / filename).write_text(value, encoding="utf-8")
        records.append(
            {
                "language": language,
                "appStoreLocale": store_locale,
                "name": "woladen",
                "subtitle": subtitle,
                "summary": summary,
                "promotionalText": promotional_text,
                "description": description,
                "keywords": keywords,
                "releaseNotes": RELEASE_NOTES[language],
                "route": {"origin": origin, "destination": destination},
                "screenshots": [
                    str((SCREENSHOT_ROOT / language / f"0{n}-{scene}.png").relative_to(ROOT))
                    for n, scene in enumerate(
                        ("list", "detail", "map", "favorites", "route", "info", "driving"), 1
                    )
                ],
            }
        )

    SUMMARY_JSON.write_text(
        json.dumps(
            {"version": "1.4.0", "build": "16", "localizations": records},
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    print(f"Generated {len(records)} localized App Store summaries under {metadata_root}")
    print(f"Wrote {SUMMARY_JSON}")


if __name__ == "__main__":
    main()
