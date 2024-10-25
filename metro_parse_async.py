import asyncio
import re
from time import perf_counter
import aiohttp
import json
from bs4 import BeautifulSoup
import fake_useragent

user = fake_useragent.UserAgent().random

cookies_msk = {
    'exp_auth': '3Rd5fv0vTUylyTasdzAcqA.1',
    '_slid_server': '6717ac187b4b5620e30fe1ad',
    'pdp_abc_20': '0',
    'plp_bmpl_bage': '1',
    '_slid': '6717ac187b4b5620e30fe1ad',
    '_ga': 'GA1.1.1678156011.1729604635',
    '_ym_uid': '1729604635789710781',
    '_ym_d': '1729604635',
    '_gcl_au': '1.1.1439528156.1729604635',
    'uxs_uid': 'ae394260-907b-11ef-ab48-3fda3a6ffa7b',
    'popmechanic_sbjs_migrations': 'popmechanic_1418474375998%3D1%7C%7C%7C1471519752600%3D1%7C%7C%7C1471519752605%3D1',
    'flocktory-uuid': '1b57edf6-7a00-44c9-91ed-8bdbf8b1d184-6',
    'is18Confirmed': 'false',
    '_slsession': 'F3151517-986D-4580-A512-4C875F3AFFCD',
    '_ym_visorc': 'b',
    'mp_recom_selected_item': '{"promo_url":"https://online.metro-cc.ru/category/zamorozhennye-produkty/morozhenoe?in_stock=1&page=7","article":110662}',
    'mp_selected_item': '{"promo_url":"https://online.metro-cc.ru/category/zamorozhennye-produkty/morozhenoe"}',
    '_slfreq': '633ff97b9a3f3b9e90027740%3A633ffa4c90db8d5cf00d7810%3A1729680520%3B64a81e68255733f276099da5%3A64abaf645c1afe216b0a0d38%3A1729680520',
    '_ym_isad': '1',
    'mindboxDeviceUUID': '71596804-0e08-418a-815a-a1d75225dea5',
    'directCrm-session': '%7B%22deviceGuid%22%3A%2271596804-0e08-418a-815a-a1d75225dea5%22%7D',
    'metroStoreId': '10',
    'pickupStore': '10',
    'coords': '55.875753%2637.447279',
    'mp_5e1c29b29aeb315968bbfeb763b8f699_mixpanel': '%7B%22distinct_id%22%3A%20%22%24device%3A192b47848bba55-0e1bb84d0d'
                                                    '6486-367b7637-144000-192b47848bba55%22%2C%22%24device_id%22%3A%20%'
                                                    '22192b47848bba55-0e1bb84d0d6486-367b7637-144000-192b47848bba55%22%'
                                                    '2C%22%24search_engine%22%3A%20%22google%22%2C%22%24initial_referre'
                                                    'r%22%3A%20%22https%3A%2F%2Fwww.google.com%2F%22%2C%22%24initial_re'
                                                    'ferring_domain%22%3A%20%22www.google.com%22%2C%22__mps%22%3A%20%7B'
                                                    '%7D%2C%22__mpso%22%3A%20%7B%22%24initial_referrer%22%3A%20%22https'
                                                    '%3A%2F%2Fwww.google.com%2F%22%2C%22%24initial_referring_domain%22%'
                                                    '3A%20%22www.google.com%22%7D%2C%22__mpus%22%3A%20%7B%7D%2C%22__mpa'
                                                    '%22%3A%20%7B%7D%2C%22__mpu%22%3A%20%7B%7D%2C%22__mpr%22%3A%20%5B%5'
                                                    'D%2C%22__mpap%22%3A%20%5B%5D%7D',
    'mp_88875cfb7a649ab6e6e310368f37a563_mixpanel': '%7B%22distinct_id%22%3A%20%22%24device%3A192b4784911aab-0481cd3d20'
                                                    'f429-367b7637-144000-192b4784911aab%22%2C%22%24device_id%22%3A%20%'
                                                    '22192b4784911aab-0481cd3d20f429-367b7637-144000-192b4784911aab%22%'
                                                    '2C%22%24search_engine%22%3A%20%22google%22%2C%22%24initial_referre'
                                                    'r%22%3A%20%22https%3A%2F%2Fwww.google.com%2F%22%2C%22%24initial_re'
                                                    'ferring_domain%22%3A%20%22www.google.com%22%2C%22__mps%22%3A%20%7B'
                                                    '%7D%2C%22__mpso%22%3A%20%7B%22%24initial_referrer%22%3A%20%22https'
                                                    '%3A%2F%2Fwww.google.com%2F%22%2C%22%24initial_referring_domain%22%'
                                                    '3A%20%22www.google.com%22%7D%2C%22__mpus%22%3A%20%7B%7D%2C%22__mpa'
                                                    '%22%3A%20%7B%7D%2C%22__mpu%22%3A%20%7B%7D%2C%22__mpr%22%3A%20%5B%5'
                                                    'D%2C%22__mpap%22%3A%20%5B%5D%7D',
    '_ga_VHKD93V3FV': 'GS1.1.1729665909.2.1.1729677202.0.0.0',
}

headers_msk = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,applica'
              'tion/signed-exchange;v=b3;q=0.7',
    'accept-language': 'ru,en-US;q=0.9,en;q=0.8',
    'cache-control': 'max-age=0',
    'if-none-match': '"1fb4ae-DIqYHViIkIu+7e7sz7G+O/afcS4"',
    'priority': 'u=0, i',
    'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "YaBrowser";v="24.7", "Yowser";v="2.5"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': user
}

cookies_spb = {
    '_slid_server': '6717ac187b4b5620e30fe1ad',
    'pdp_abc_20': '0',
    'plp_bmpl_bage': '1',
    '_slid': '6717ac187b4b5620e30fe1ad',
    '_ga': 'GA1.1.1678156011.1729604635',
    '_ym_uid': '1729604635789710781',
    '_ym_d': '1729604635',
    '_gcl_au': '1.1.1439528156.1729604635',
    'uxs_uid': 'ae394260-907b-11ef-ab48-3fda3a6ffa7b',
    'popmechanic_sbjs_migrations': 'popmechanic_1418474375998%3D1%7C%7C%7C1471519752600%3D1%7C%7C%7C1471519752605%3D1',
    'flocktory-uuid': '1b57edf6-7a00-44c9-91ed-8bdbf8b1d184-6',
    'exp_auth': '3Rd5fv0vTUylyTasdzAcqA.1',
    '_ym_isad': '1',
    'is18Confirmed': 'true',
    '_slsession': '35F53B1B-3FE7-48C5-99CD-5B912DA66F19',
    '_slfreq': '633ff97b9a3f3b9e90027740%3A633ffa4c90db8d5cf00d7810%3A1729877657%3B64a81e68255733f276099da5%3A64abaf645c1afe216b0a0d38%3A1729877657',
    '_ym_visorc': 'b',
    'metroStoreId': '15',
    'pickupStore': '15',
    'coords': '60.002202%2630.26868',
    'mp_5e1c29b29aeb315968bbfeb763b8f699_mixpanel': '%7B%22distinct_id%22%3A%20%22%24device%3A192b47848bba55-0e1bb84d0d6486-367b7637-144000-192b47848bba55%22%2C%22%24device_id%22%3A%20%22192b47848bba55-0e1bb84d0d6486-367b7637-144000-192b47848bba55%22%2C%22%24search_engine%22%3A%20%22google%22%2C%22%24initial_referrer%22%3A%20%22https%3A%2F%2Fwww.google.com%2F%22%2C%22%24initial_referring_domain%22%3A%20%22www.google.com%22%2C%22__mps%22%3A%20%7B%7D%2C%22__mpso%22%3A%20%7B%22%24initial_referrer%22%3A%20%22https%3A%2F%2Fwww.google.com%2F%22%2C%22%24initial_referring_domain%22%3A%20%22www.google.com%22%7D%2C%22__mpus%22%3A%20%7B%7D%2C%22__mpa%22%3A%20%7B%7D%2C%22__mpu%22%3A%20%7B%7D%2C%22__mpr%22%3A%20%5B%5D%2C%22__mpap%22%3A%20%5B%5D%7D',
    'mp_88875cfb7a649ab6e6e310368f37a563_mixpanel': '%7B%22distinct_id%22%3A%20%22%24device%3A192b4784911aab-0481cd3d20f429-367b7637-144000-192b4784911aab%22%2C%22%24device_id%22%3A%20%22192b4784911aab-0481cd3d20f429-367b7637-144000-192b4784911aab%22%2C%22%24search_engine%22%3A%20%22google%22%2C%22%24initial_referrer%22%3A%20%22https%3A%2F%2Fwww.google.com%2F%22%2C%22%24initial_referring_domain%22%3A%20%22www.google.com%22%2C%22__mps%22%3A%20%7B%7D%2C%22__mpso%22%3A%20%7B%22%24initial_referrer%22%3A%20%22https%3A%2F%2Fwww.google.com%2F%22%2C%22%24initial_referring_domain%22%3A%20%22www.google.com%22%7D%2C%22__mpus%22%3A%20%7B%7D%2C%22__mpa%22%3A%20%7B%7D%2C%22__mpu%22%3A%20%7B%7D%2C%22__mpr%22%3A%20%5B%5D%2C%22__mpap%22%3A%20%5B%5D%7D',
    'mindboxDeviceUUID': '71596804-0e08-418a-815a-a1d75225dea5',
    'directCrm-session': '%7B%22deviceGuid%22%3A%2271596804-0e08-418a-815a-a1d75225dea5%22%7D',
    '_ga_VHKD93V3FV': 'GS1.1.1729870457.8.1.1729873241.0.0.0',
}

headers_spb = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'ru,en-US;q=0.9,en;q=0.8',
    'cache-control': 'max-age=0',
    # 'cookie': '_slid_server=6717ac187b4b5620e30fe1ad; pdp_abc_20=0; plp_bmpl_bage=1; _slid=6717ac187b4b5620e30fe1ad; _ga=GA1.1.1678156011.1729604635; _ym_uid=1729604635789710781; _ym_d=1729604635; _gcl_au=1.1.1439528156.1729604635; uxs_uid=ae394260-907b-11ef-ab48-3fda3a6ffa7b; popmechanic_sbjs_migrations=popmechanic_1418474375998%3D1%7C%7C%7C1471519752600%3D1%7C%7C%7C1471519752605%3D1; flocktory-uuid=1b57edf6-7a00-44c9-91ed-8bdbf8b1d184-6; exp_auth=3Rd5fv0vTUylyTasdzAcqA.1; _ym_isad=1; is18Confirmed=true; _slsession=35F53B1B-3FE7-48C5-99CD-5B912DA66F19; _slfreq=633ff97b9a3f3b9e90027740%3A633ffa4c90db8d5cf00d7810%3A1729877657%3B64a81e68255733f276099da5%3A64abaf645c1afe216b0a0d38%3A1729877657; _ym_visorc=b; metroStoreId=15; pickupStore=15; coords=60.002202%2630.26868; mp_5e1c29b29aeb315968bbfeb763b8f699_mixpanel=%7B%22distinct_id%22%3A%20%22%24device%3A192b47848bba55-0e1bb84d0d6486-367b7637-144000-192b47848bba55%22%2C%22%24device_id%22%3A%20%22192b47848bba55-0e1bb84d0d6486-367b7637-144000-192b47848bba55%22%2C%22%24search_engine%22%3A%20%22google%22%2C%22%24initial_referrer%22%3A%20%22https%3A%2F%2Fwww.google.com%2F%22%2C%22%24initial_referring_domain%22%3A%20%22www.google.com%22%2C%22__mps%22%3A%20%7B%7D%2C%22__mpso%22%3A%20%7B%22%24initial_referrer%22%3A%20%22https%3A%2F%2Fwww.google.com%2F%22%2C%22%24initial_referring_domain%22%3A%20%22www.google.com%22%7D%2C%22__mpus%22%3A%20%7B%7D%2C%22__mpa%22%3A%20%7B%7D%2C%22__mpu%22%3A%20%7B%7D%2C%22__mpr%22%3A%20%5B%5D%2C%22__mpap%22%3A%20%5B%5D%7D; mp_88875cfb7a649ab6e6e310368f37a563_mixpanel=%7B%22distinct_id%22%3A%20%22%24device%3A192b4784911aab-0481cd3d20f429-367b7637-144000-192b4784911aab%22%2C%22%24device_id%22%3A%20%22192b4784911aab-0481cd3d20f429-367b7637-144000-192b4784911aab%22%2C%22%24search_engine%22%3A%20%22google%22%2C%22%24initial_referrer%22%3A%20%22https%3A%2F%2Fwww.google.com%2F%22%2C%22%24initial_referring_domain%22%3A%20%22www.google.com%22%2C%22__mps%22%3A%20%7B%7D%2C%22__mpso%22%3A%20%7B%22%24initial_referrer%22%3A%20%22https%3A%2F%2Fwww.google.com%2F%22%2C%22%24initial_referring_domain%22%3A%20%22www.google.com%22%7D%2C%22__mpus%22%3A%20%7B%7D%2C%22__mpa%22%3A%20%7B%7D%2C%22__mpu%22%3A%20%7B%7D%2C%22__mpr%22%3A%20%5B%5D%2C%22__mpap%22%3A%20%5B%5D%7D; mindboxDeviceUUID=71596804-0e08-418a-815a-a1d75225dea5; directCrm-session=%7B%22deviceGuid%22%3A%2271596804-0e08-418a-815a-a1d75225dea5%22%7D; _ga_VHKD93V3FV=GS1.1.1729870457.8.1.1729873241.0.0.0',
    'if-none-match': '"1fe1a9-sB4LAqbs4E5WevmEbMrV9ZVwCrM"',
    'priority': 'u=0, i',
    'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "YaBrowser";v="24.10", "Yowser";v="2.5"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': user,
}

params = {
    'in_stock': '1',
    'page': '1'
}

counter = 1

metro_dn = 'https://online.metro-cc.ru'
category = 'zamorozhennye-produkty/morozhenoe'


async def get_response(session, lk, pm, cs, hs):
    async with session.get(lk, params=pm, cookies=cs, headers=hs) as response:
        return await response.text()


async def process_element(session, el, pm, cs, hs, results):
    global counter
    if el.get('id'):
        id_el = el.get('id')
        name = (el.find('div', class_='product-card__content')
                .find('div', class_='catalog-2-level-product-card__middle')
                .find('a').get('title'))
        el_link = metro_dn + ((el.find('div', class_='product-card__content')
                               .find('div', class_='catalog-2-level-product-card__middle'))
                              .find('a').get('href'))
        el_price = str(el.find('div', class_='product-card__content')
                       .find('div', class_='catalog-2-level-product-card__prices-rating')
                       .find('div', class_='product-unit-prices__actual-wrapper')
                       .find('span', class_='product-price__sum-rubles').text)
        try:
            el_price += f"{el.find('div', class_='product-card__content')
                  .find('div', class_='catalog-2-level-product-card__prices-rating')
                  .find('div', class_='product-unit-prices__actual-wrapper')
                  .find('span', class_='product-price__sum-penny').text}"
        except:
            pass
        try:
            regular_price = str(el.find('div', class_='product-card__content')
                                .find('div', class_='catalog-2-level-product-card__prices-rating')
                                .find('div', class_='product-unit-prices__old-wrapper')
                                .find('span', class_='product-price__sum-rubles').text)
            try:
                regular_price += f"{el.find('div', class_='product-card__content')
                                .find('div', class_='catalog-2-level-product-card__prices-rating')
                                .find('div', class_='product-unit-prices__old-wrapper')
                                .find('span', class_='product-price__sum-penny').text}"
            except:
                pass
        except:
            regular_price = el_price

        brand = await get_brand(session, el_link, pm, cs, hs)
        el_price = re.sub(r'[^\d.]', '', el_price)
        regular_price = re.sub(r'[^\d.]', '', regular_price)
        print(f"{counter}) ID: {id_el}, Name: {name}, Brand: {brand}, Price: {el_price}, Old price: {regular_price}, Link: {el_link}")
        counter += 1
        results.append({
            "ID": id_el,
            "Name": name,
            "Brand": brand,
            "Price": el_price,
            "Old price": regular_price,
            "Link": el_link
        })


async def get_data(session, sp, pm, cs, hs, results):
    elements = sp.find('div', id='products-inner').find_all()
    tasks = [process_element(session, el, pm, cs, hs, results) for el in elements]
    await asyncio.gather(*tasks)


async def get_brand(session, lk, pm, cs, hs):
    rs = await get_response(session, lk, pm, cs, hs)
    soup = BeautifulSoup(rs, 'lxml')
    brand = (soup.find('ul', class_='product-attributes__list style--product-page-short-list')
             .find_all('li')[4].find('a', class_='product-attributes__list-item-link reset-link active-blue-text').text)
    return brand.strip()


async def get_last_page(session, lk, pm, cs, hs):
    res = await get_response(session, lk, pm, cs, hs)
    soup = BeautifulSoup(res, 'lxml')
    last_page = soup.find('ul', class_='catalog-paginate v-pagination').find_all('li')[-2].text
    print(f"Количество страниц: {last_page}")
    return last_page


async def main(city):
    results = []
    if city == 'msk':
        async with aiohttp.ClientSession() as session:
            link = f'https://online.metro-cc.ru/category/{category}'
            last_page = await get_last_page(session, link, params, cookies_msk, headers_msk)
            page = 1
            for i in range(int(last_page)):
                response = await get_response(session, link, params, cookies_msk, headers_msk)
                soup = BeautifulSoup(response, 'lxml')
                await get_data(session, soup, params, cookies_msk, headers_msk, results)
                page += 1
                params['page'] = str(page)
        with open('results.json', 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=4)
    if city == 'spb':
        async with aiohttp.ClientSession() as session:
            link = f'https://online.metro-cc.ru/category/{category}'
            last_page = await get_last_page(session, link, params, cookies_spb, headers_spb)
            page = 1
            for i in range(int(last_page)):
                response = await get_response(session, link, params, cookies_spb, headers_spb)
                soup = BeautifulSoup(response, 'lxml')
                await get_data(session, soup, params, cookies_spb, headers_spb, results)
                page += 1
                params['page'] = str(page)
        with open('results.json', 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=4)


if __name__ == '__main__':
    start = perf_counter()
    # Чтобы получить данные для Москвы необходимо передать в функцию main параметр 'msk'
    # Для Санкт-Петербурга 'spb'
    asyncio.run(main('spb'))
    print(f"time: {perf_counter() - start}")
    # time: 82.87765650000074
