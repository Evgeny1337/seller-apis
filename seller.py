import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """Функция возвращает список товаров магазина озон

    Args:
        last_id (str):
            Идентификатор последнего товара для получения последующих.
        client_id (str):
            Идентификатор клиент.
        seller_token (str):
            Токен продавца для аутентификации.

    Returns:
        list: список товаров, с ограничением в 1000 единиц

    Raises:
        HTTPError: Если запрос к API не был успешным.

    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """Функция возвращает артикулы товаров магазина Озон.

    Args:
        client_id (str): Идентификатор клиента.
        seller_token (str): Токен продавца для аутентификации.

    Returns:
        list: Список артикулов товаров.

    Raises:
        HTTPError: Если запрос к API не был успешным.
    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """Функция вохвращет обновленные цены товаров
    Args:
        prices (list): Список товаров с новыми ценами.
        client_id (str): Идентификатор клиента.
        seller_token (str): Токен продавца для аутентификации.

    Returns:
        dict: Ответ от API после обновления цен.

    Raises:
        HTTPError: Если запрос к API не был успешным.
    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """Функция обновляет остатки товаров

    Args:
        stocks (list): Список остатков товаров с артикулом и количеством.
        client_id (str): Идентификатор клиента.
        seller_token (str): Токен продавца для аутентификации.

    Returns:
        dict: Ответ от API после обновления остатков.

    Raises:
        HTTPError: Если запрос к API не был успешным.
    """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """Функция возвращет из файла остатков с сайта Casio список товаров.

    Returns:
        list: Список остатков часов в виде словарей.

    Raises:
        HTTPError: Если запрос к сайту не был успешным.
        FileNotFoundError: Если файл со списком остатков не найден.
        Exception: Если возникла ошибка при извлечении или чтении файла.

    """
    # Скачать остатки с сайта
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """Функция возврщает обновленный список с товарами

    Args:
        watch_remnants (list):
            Список остатков часов в виде словарей.
        offer_ids (list):
            Список артикулов товаров.
    Returns:
        list: Список остатков для обновления с артикулом и количеством.
    Raises:
        ValueError: Если `watch_remnants` не является списком
            или `offer_ids` не является списком.
        TypeError: Если в `watch_remnants` содержатся элементы,
            которые не являются словарями.
    """
    # Уберем то, что не загружено в seller
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Функция обновления стоимости

    Args:
        watch_remnants (list): Данные об остатках товаров с сайта CASIO
        offer_ids (list): Данные о товарах с OZON

    Returns:
        list: Список товаров с обновленными ценами

    Raises:
        ValueError: Если `watch_remnants` не является списком
            или `offer_ids` не является списком.
        TypeError: Если в `watch_remnants` содержатся элементы,
            которые не являются словарями.

    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """Функция конвертации стоимости

    Данная функция удаляет символы, которые не влияют на числовое значение.

    Args:
        price (str): Стоимость товара

    Returns:
        str: Обновленная стоимость товара

    Raises:
        ValueError: Если входное значение `price` не является строкой
        или пустой строкой.
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Функция разделяет список lst на части по n элементов.

    Args:
        lst (list): Список, который нужно разделить.
        n (int): Количество элементов в каждом подсписке.

    Yields:
        list: Подсписки длины n из исходного списка.

    Raises:
        ValueError: Если n меньше или равно 0.

    """
    for i in range(0, len(lst), n):
        yield lst[i: i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
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
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """Функция загружает остатки для оставшихся часов.

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
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
