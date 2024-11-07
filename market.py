import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """Функция возвращает список продуктов из Яндекс Маркета.

    Args:
        page (str): Токен страницы для постраничного доступа.
        campaign_id (str): Идентификатор кампании в Яндекс Маркете.
        access_token (str): Токен доступа для авторизации в API.

    Returns:
        dict: Результаты запроса, содержащие список товаров.

    Raises:
        requests.exceptions.HTTPError: Если запрос возвращает код ошибки HTTP.
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """Функция обновляет остатки товаров в Яндекс Маркете.

    Args:
        stocks (list): Список артикулов товаров с обновленными остатками.
        campaign_id (str): Идентификатор кампании в Яндекс Маркете.
        access_token (str): Токен доступа для авторизации в API.

    Returns:
        dict: Результаты запроса об обновлении остатков.

    Raises:
        requests.exceptions.HTTPError: Если запрос возвращает код ошибки HTTP.
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    """Функция обновляет цены товаров в Яндекс Маркете.

    Args:
        prices (list): Список объектов с обновленными ценами товаров.
        campaign_id (str): Идентификатор кампании в Яндекс Маркете.
        access_token (str): Токен доступа для авторизации в API.

    Returns:
        dict: Результаты запроса об обновлении цен.

    Raises:
        requests.exceptions.HTTPError: Если запрос возвращает код ошибки HTTP.
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """Функция возвращает артикулы товаров Яндекс Маркета.

    Args:
        campaign_id (str): Идентификатор кампании в Яндекс Маркете.
        market_token (str): Токен доступа к API Яндекс Маркета.

    Returns:
        list: Список артикулов (shopSku) товаров, найденных в Яндекс Маркете.

    Raises:
        ValueError: Если не удается получить список товаров из API.
    """
    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """Функция возвращает список остатков товаров на складе.

    Args:
        watch_remnants (list): Список остатков часов с их кодами
        и количествами.
        offer_ids (list): Список артикулов товаров для проверки на наличие.
        warehouse_id (str): Идентификатор склада, на котором хранится товар.

    Returns:
        list: Форматированный список остатков для обновления на складе.

    Raises:
        ValueError: Если `watch_remnants` не является списком
            или `offer_ids` не является списком.
        TypeError: Если в `watch_remnants` содержатся элементы,
            которые не являются словарями.
    """
    stocks = list()
    date = str(datetime.datetime.utcnow().replace(
        microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Функция влзвращает список цен на товары.

    Args:
        watch_remnants (list): Список остатков часов с их кодами и ценами.
        offer_ids (list): Список артикулов товаров для проверки на наличие.

    Returns:
        prices(list): Форматированный список цен для обновления на складе.

    Raises:
        ValueError: price_conversion отработала с ошибкой.
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "id": str(watch.get("Код")),
                # "feed": {"id": 0},
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    # "discountBase": 0,
                    "currencyId": "RUR",
                    # "vat": 0,
                },
                # "marketSku": 0,
                # "shopSku": "string",
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    """Функция загружает цены для оставшихся часов.

    Args:
        watch_remnants (list): Список остатков часов в виде словарей.
        client_id (str): Идентификатор клиента.
        seller_token (str): Токен продавца для аутентификации.

    Returns:
        prices(list): Список цен, который был загружен.

    Raises:
        Exception: Если функции get_offer_ids, create_prices
        или update_price вызывают исключения.

    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    """Функция загружает остатки для оставшихся часов

    Args:
        watch_remnants (list): Список остатков часов в виде словарей.
        client_id (str): Идентификатор клиента.
        seller_token (str): Токен продавца для аутентификации.

    Returns:
        tuple: 
            - list: Остатки, у которых количество больше 0.
            - list: Полный список всех остатков.

    Raises:
        Exception: Если функции get_offer_ids, create_stocks
        или update_stocks вызывают исключения.
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        # FBS
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        # Обновить остатки FBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        # Поменять цены FBS
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        # DBS
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        # Обновить остатки DBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        # Поменять цены DBS
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
