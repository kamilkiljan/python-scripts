from dataclasses import asdict, dataclass
from datetime import date
import re

from bs4 import BeautifulSoup
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import HTTPError

s = requests.Session()
s.mount('https://', HTTPAdapter(max_retries=3))

DEFAULT_BRANDS = ('volkswagen', 'opel', 'ford', 'audi', 'bmw', 'mercedes-benz', 'toyota', 'renault', 'skoda',
                  'honda', 'kia', 'mazda', 'peugeot')
ALL_REGIONS = ('mazowieckie', 'wielkopolskie', 'slaskie', 'dolnoslaskie', 'malopolskie', 'pomorskie', 'lodzkie',
               'kujawsko-pomorskie', 'zachodniopomorskie', 'lubuskie', 'lubelskie', 'swietokrzyskie', 'podkrapackie',
               'opolskie', 'warminsko-mazurskie', 'podlaskie')


def extract_string(soup, html_element, class_):
    element = soup.find(html_element, class_=class_)
    if element is not None and tuple(element.stripped_strings):
        return tuple(element.stripped_strings)[0]
    else:
        return ''


@dataclass
class Offer:
    offer_est_pattern = re.compile('"price_prediction_indicator":"(none|below|in|above)"')
    region_id_pattern = re.compile("var region_id='([0-9]*)';")
    subregion_id_pattern = re.compile("var subregion_id='([0-9]*)';")
    city_id_pattern = re.compile("var city_id='([0-9]*)';")
    user_date_pattern = re.compile('Sprzedający na OTOMOTO od (20[0-9]{2})')
    lat_lon_pattern = re.compile('Current+Location/(?P<lat>[0-9]{,2}\.[0-9]{,8}),(?P<lon>[0-9]{,2}\.[0-9]{,8})')
    params_mapping = {
        'Oferta od': 'seller_type',
        'Kategoria': 'category',
        'Marka pojazdu': 'brand',
        'Model pojazdu': 'model',
        'Kod Silnika': 'engine_code',
        'Wersja': 'version',
        'Generacja': 'generation',
        'Rok produkcji': 'production_year',
        'Przebieg': 'mileage',
        'Pojemność skokowa': 'engine_displacement',
        'Rodzaj paliwa': 'fuel_type',
        'Moc': 'power',
        'Skrzynia biegów': 'transmission_type',
        'Napęd': 'powertrain',
        'Typ': 'body_type',
        'Liczba drzwi': 'doors',
        'Liczba miejsc': 'seats',
        'Kolor': 'color',
        'Miesięczna rata': 'leasing_rental_price',
        'Gwarancja dealerska (w cenie)': 'dealer_guarantee_months',
        'lub do (przebieg km)': 'dealer_guarantee_kms',
        'Okres gwarancji producenta': 'producer_guarantee',
        'Kraj pochodzenia': 'country_of_origin',
        'Numer rejestracyjny pojazdu': 'registration_number',
        'Pierwsza rejestracja': 'first_registered_year',
        'Stan': 'condition',
    }
    bool_params_mapping = {
        'Akryl (niemetalizowany)': 'acrylic',
        'Metalik': 'metallic',
        'Perłowy': 'pearl',
        'Możliwość finansowania': 'available_financing',
        'Leasing': 'leasing',
        'Faktura VAT': 'vat_invoice',
        'VAT marża': 'vat_margin',
        'Zarejestrowany w Polsce': 'registered_in_poland',
        'Pierwszy właściciel': 'first_owner',
        'Bezwypadkowy': 'accident_free',
        'Uszkodzony': 'damaged',
        'Serwisowany w ASO': 'aso_serviced',
    }
    features_mapping = {
        'ABS': 'abs',
        'CD': 'cd',
        'Centralny zamek': 'central_lock',
        'Elektryczne szyby przednie': 'el_front_windows',
        'Elektrycznie ustawiane lusterka': 'el_side_mirrors',
        'Immobilizer': 'immobilizer',
        'Radio fabryczne': 'radio_factory',
        'Wspomaganie kierownicy': 'power_steering',
        'Alarm': 'alarm',
        'Alufelgi': 'alu_rims',
        'ASR (kontrola trakcji)': 'asr',
        'Asystent parkowania': 'parking_assistant',
        'Asystent pasa ruchu': 'lane_assistant',
        'Bluetooth': 'bluetooth',
        'Czujnik deszczu': 'rain_sensor',
        'Czujnik martwego pola': 'blind_field_sensor',
        'Czujnik zmierzchu': 'nightfall_sensor',
        'Czujniki parkowania przednie': 'parking_sensors_front',
        'Czujniki parkowania tylne': 'pparking_sensors_rear',
        'Dach panoramiczny': 'panoramic_roof',
        'Elektrochromatyczne lusterka boczne': 'el_chromatic_side_mirrors',
        'Elektrochromatyczne lusterko wsteczne': 'el_chromatic_rear_mirror',
        'Elektryczne szyby tylne': 'el_rear_windows',
        'Elektrycznie ustawiane fotele': 'el_adjusted_seats',
        'ESP (stabilizacja toru jazdy)': 'esp',
        'Gniazdo AUX': 'aus_socket',
        'Gniazdo SD': 'sd_socket',
        'Gniazdo USB': 'usb_socket',
        'Hak': 'hook',
        'HUD (wyświetlacz przezierny)': 'hud',
        'Isofix': 'isofix',
        'Kamera cofania': 'reverse_camera',
        'Klimatyzacja automatyczna': 'ac_auto',
        'Klimatyzacja czterostrefowa': 'ac_four_zones',
        'Klimatyzacja dwustrefowa': 'ac_two_zones',
        'Klimatyzacja manualna': 'ac_manual',
        'Komputer pokładowy': 'computer',
        'Łopatki zmiany biegów': 'transmission_paddles',
        'MP3': 'mp3',
        'Nawigacja GPS': 'gps',
        'Odtwarzacz DVD': 'dvd',
        'Ogranicznik prędkości': 'speed_limiter',
        'Ogrzewanie postojowe': 'stand_by_heating',
        'Podgrzewana przednia szyba': 'heated_windshield',
        'Podgrzewane lusterka boczne': 'heated_side_mirrors',
        'Podgrzewane przednie siedzenia': 'heated_front_seats',
        'Podgrzewane tylne siedzenia': 'heated_rear_seats',
        'Przyciemniane szyby': 'tinted_windows',
        'Radio niefabryczne': 'radio_non_factory',
        'Regulowane zawieszenie': 'adjusted_suspension',
        'Relingi dachowe': 'roof_railings',
        'System Start-Stop': 'start_stop_system',
        'Szyberdach': 'sliding_roof',
        'Światła do jazdy dziennej': 'lights_daylight',
        'Światła LED': 'lights_led',
        'Światła przeciwmgielne': 'lights_fog_lamps',
        'Światła Xenonowe': 'lights_xenon',
        'Tapicerka skórzana': 'upholstery_leather',
        'Tapicerka welurowa': 'upholstery_velour',
        'Tempomat': 'autocruise_manual',
        'Tempomat aktywny': 'autocruise_active',
        'Tuner TV': 'tv_tuner',
        'Wielofunkcyjna kierownica': 'multifunctional_steering_wheel',
        'Zmieniarka CD': 'cd_changer',
    }
    # resource data
    url: str
    url_import_date: date
    url_expired_date: date = None
    id_: str = None
    # price
    estimation: str = 'none'
    price: int = 0
    currency: str = 'PLN'
    # params
    seller_type: str = None
    category: str = None
    brand: str = None
    model: str = None
    engine_code: str = None
    version: str = None
    generation: str = None
    production_year: int = None
    mileage: int = None
    engine_displacement: int = None
    fuel_type: str = None
    power: int = None
    transmission_type: str = None
    powertrain: str = None
    body_type: str = None
    doors: int = None
    seats: int = None
    color: str = None
    acrylic: bool = None
    metallic: bool = None
    pearl: bool = None
    financing_available: bool = None
    leasing: bool = None
    leasing_rental_price: str = None
    dealer_guarantee_months: int = None
    dealer_guarantee_kms: int = None
    producer_guarantee: str = None
    vat_invoice: bool = None
    country_of_origin: str = None
    registration_number: str = None
    first_registered_year: int = None
    registered_in_poland: bool = None
    first_owner: bool = None
    accident_free: bool = None
    damaged: bool = None
    aso_serviced: bool = None
    condition: str = None
    # user data
    user_date: str = None
    user_name: str = None
    # features
    abs: bool = None
    cd: bool = None
    central_lock: bool = None
    el_front_windows: bool = None
    el_side_mirrors: bool = None
    immobilizer: bool = None
    radio_factory: bool = None
    power_steering: bool = None
    alarm: bool = None
    alu_rims: bool = None
    asr: bool = None
    parking_assistant: bool = None
    lane_assistant: bool = None
    bluetooth: bool = None
    rain_sensor: bool = None
    blind_field_sensor: bool = None
    nightfall_sensor: bool = None
    parking_sensors_front: bool = None
    pparking_sensors_rear: bool = None
    panoramic_roof: bool = None
    el_chromatic_side_mirrors: bool = None
    el_chromatic_rear_mirror: bool = None
    el_rear_windows: bool = None
    el_adjusted_seats: bool = None
    esp: bool = None
    aus_socket: bool = None
    sd_socket: bool = None
    usb_socket: bool = None
    hook: bool = None
    hud: bool = None
    isofix: bool = None
    reverse_camera: bool = None
    ac_auto: bool = None
    ac_four_zones: bool = None
    ac_two_zones: bool = None
    ac_manual: bool = None
    computer: bool = None
    transmission_paddles: bool = None
    mp3: bool = None
    gps: bool = None
    dvd: bool = None
    speed_limiter: bool = None
    stand_by_heating: bool = None
    heated_windshield: bool = None
    heated_side_mirrors: bool = None
    heated_front_seats: bool = None
    heated_rear_seats: bool = None
    tinted_windows: bool = None
    radio_non_factory: bool = None
    adjusted_suspension: bool = None
    roof_railings: bool = None
    start_stop_system: bool = None
    sliding_roof: bool = None
    lights_daylight: bool = None
    lights_led: bool = None
    lights_fog_lamps: bool = None
    lights_xenon: bool = None
    upholstery_leather: bool = None
    upholstery_velour: bool = None
    autocruise_manual: bool = None
    autocruise_active: bool = None
    tv_tuner: bool = None
    multifunctional_steering_wheel: bool = None
    cd_changer: bool = None
    # other
    description: str = None
    location: str = None
    region_id: int = None
    subregion_id: int = None
    city_id: int = None
    photos: int = None
    publish_date: date = None

    def import_offer(self):
        """Import data from the offer's URL"""
        full_url = f"https://www.otomoto.pl/oferta/{self.url}.html"
        print(full_url)
        try:
            r = s.get(full_url)
            if r.status_code != 200:
                raise Exception
            content = r.text
            soup = BeautifulSoup(content, 'lxml')

            self.publish_date = extract_string(soup, 'span', 'offer-meta__value')
            self.id_ = tuple(soup.find('div', class_='offer-meta').stripped_strings)[2]
            if re.findall(self.offer_est_pattern, content):
                self.estimation = re.findall(self.offer_est_pattern, content)[0]
            else:
                self.estimation = 'none'
            self.price = extract_string(soup, 'span', 'offer-price__number')
            self.currency = extract_string(soup, 'span', 'offer-price__currency')
            if soup.find('div', class_='offer-features__row'):
                features = tuple(soup.find('div', class_='offer-features__row').stripped_strings)
                for feature in features:
                    if feature in self.features_mapping:
                        self.__setattr__(self.features_mapping[feature], True)
            self.description = extract_string(soup, 'div', 'offer-description__description')
            self.location = extract_string(soup, 'span', 'seller-box__seller-address__label')
            self.region_id = re.findall(self.region_id_pattern, content)[0]
            self.subregion_id = re.findall(self.subregion_id_pattern, content)[0]
            self.city_id = re.findall(self.city_id_pattern, content)[0]
            if soup.find('button', class_='gallery-images-counter'):
                self.photos = int(''.join(tuple(soup.find('button', class_='gallery-images-counter').stripped_strings)).split('/')[1])
            self.user_date = re.findall(self.user_date_pattern, content)[0]
            self.user_name = extract_string(soup, 'h2', 'seller-box__seller-name')

            # Params and features parsing
            params = dict([tuple(item.stripped_strings) for item in soup.find_all('li', class_='offer-params__item')])
            for param in params:
                if param in self.params_mapping:
                    self.__setattr__(self.params_mapping[param], params[param])
                elif param in self.bool_params_mapping:
                    self.__setattr__(self.bool_params_mapping[param], True)

        except Exception as e:
            pass

    def update_offer(self):
        """Check if the offer URL still links to valid offer"""
        ...


def import_list_of_offers(offers, brands=DEFAULT_BRANDS, regions=ALL_REGIONS):
    item_url_pattern = re.compile('href="https://www.otomoto.pl/oferta/(.+?).html')

    existing_urls = set(offer.url for offer in offers)
    new_urls = set()

    for brand in brands:
        for region in regions:
            page = 1
            consecutive_errors = 0
            while consecutive_errors < 3:
                if not page % 5:
                    print(f"{brand} {region} {page}")
                try:
                    content = s.get(f"https://www.otomoto.pl/osobowe/{brand}/{region}/?page={page}").text
                    matches = set(re.findall(item_url_pattern, content))
                except HTTPError as e:
                    consecutive_errors += 1
                else:
                    if (existing_urls | new_urls) > matches:
                        consecutive_errors += 1
                    else:
                        new_urls |= matches
                        consecutive_errors = 0
                        page += 1
                        continue
    new_offers = [Offer(url=url, url_import_date=date.today()) for url in new_urls]
    offers.extend(new_offers)
    return offers

if __name__ == '__main__':
    offers = import_list_of_offers([])
    for offer in offers:
        offer.import_offer()
    df = pd.DataFrame([asdict(offer) for offer in offers])
    df.to_csv('offers.tsv', sep='\t')
    pass
