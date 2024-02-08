import requests
import datetime
import pandas as pd
import json
import time
import sys
import logging
import threading
import queue
from functools import wraps
from collections import defaultdict


class InitialWBDataLoader:
    def __init__(self, wb_statistic_and_price_token=None, wb_seller_analytics=None):
        self.loger = logging.getLogger(self.__class__.__name__)
        self.loger.setLevel(logging.DEBUG)

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        stream_hunter = logging.StreamHandler(sys.stdout)
        stream_hunter.setLevel(logging.DEBUG)
        stream_hunter.setFormatter(formatter)
        self.loger.addHandler(stream_hunter)

        file_hunter = logging.FileHandler('Logbook')
        file_hunter.setLevel(logging.DEBUG)
        file_hunter.setFormatter(formatter)
        self.loger.addHandler(file_hunter)

        self.wb_statistic_and_price_token = wb_statistic_and_price_token
        self.wb_seller_analytics = wb_seller_analytics

    @staticmethod
    def __handle_exceptions(max_attempts=5):
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                attempt = {'RequestException': 0, 'ValueError': 0, 'Exception': 0}
                network_issues_attempts = 0
                while max(attempt.values()) < max_attempts:
                    try:
                        response = func(*args, **kwargs)
                        return response
                    except (ConnectionError, requests.exceptions.Timeout):
                        network_issues_attempts += 1
                        if network_issues_attempts >= max_attempts:
                            raise Exception("Превышено количество попыток из-за проблем с сетью. "
                                            "Пожалуйста, проверьте ваше соединение.")
                        args[0].loger.exception(f"Проблемы с сетью. Попытка {network_issues_attempts}"
                                                f" из {max_attempts}.")
                        print(1)
                    except requests.RequestException as e:
                        attempt['RequestException'] += 1
                        print(2)
                        args[0].loger.exception(f'Ошибка при выполнении запроса: {e}')
                    except ValueError:
                        attempt['ValueError'] += 1
                        logging.exception("Неверный запрос, проверьте передаваемые данные.")
                        print(3)
                    except Exception as e:
                        attempt['Exception'] += 1
                        args[0].loger.exception(f'Неизвестная ошибка: {e}')
                        print(4)
                    if attempt != max_attempts:
                        args[0].loger.info(
                            f"time.sleep увеличен до {45 + 15 * max(attempt.values())}")
                        time.sleep(60 + 15 * max(attempt.values()))
                if max(attempt.values()) == max_attempts:
                    raise Exception(f"Превышено максимальное количество попыток из-за ошибок {attempt}")
            return wrapper
        return decorator

    @__handle_exceptions(max_attempts=3)
    def get_supplier_data(self, date_from):
        url = 'https://statistics-api.wildberries.ru/api/v1/supplier/incomes'
        headers = {'Authorization': self.wb_statistic_and_price_token}
        params = {'dateFrom': date_from}
        response = requests.get(url=url, headers=headers, params=params)
        report = json.loads(response.text)
        df = pd.DataFrame(report)
        return df

    @__handle_exceptions(max_attempts=3)
    def get_stock_data(self, date_from):
        url = 'https://statistics-api.wildberries.ru/api/v1/supplier/stocks'
        headers = {'Authorization': self.wb_statistic_and_price_token}
        params = {'dateFrom': date_from}
        response = requests.get(url=url, headers=headers, params=params)
        report = json.loads(response.text)
        df = pd.DataFrame(report)
        return df

    @__handle_exceptions(max_attempts=3)
    def get_orders_data(self, date_from):
        url = 'https://statistics-api.wildberries.ru/api/v1/supplier/orders'
        headers = {'Authorization': self.wb_statistic_and_price_token}
        params = {'dateFrom': date_from}
        response = requests.get(url=url, headers=headers, params=params)
        report = json.loads(response.text)
        df = pd.DataFrame(report)
        return df

    @__handle_exceptions(max_attempts=3)
    def get_sales_data(self, date_from):
        url = 'https://statistics-api.wildberries.ru/api/v1/supplier/sales'
        headers = {'Authorization': self.wb_statistic_and_price_token}
        params = {'dateFrom': date_from}
        response = requests.get(url=url, headers=headers, params=params)
        report = json.loads(response.text)
        df = pd.DataFrame(report)
        return df

    @__handle_exceptions(max_attempts=3)
    def get_detail_by_period_data(self, date_from, date_to, limit, rrdid):
        df = pd.DataFrame()
        number_request = 1
        while True:
            url = 'https://statistics-api.wildberries.ru/api/v1/supplier/reportDetailByPeriod'
            headers = {'Authorization': self.wb_statistic_and_price_token}
            params = {
                'dateFrom': date_from,
                'limit': limit,
                'dateTo': date_to,
                'rrdid': rrdid
            }
            response = requests.get(url=url, headers=headers, params=params)
            report = json.loads(response.text)
            df = pd.concat([df, pd.DataFrame(report)])
            if df.shape[0] < 100000 * number_request:
                break
            else:
                rrdid = df['rrd_id'].iloc[-1]
            number_request += 1
            time.sleep(60)
        return df

    @__handle_exceptions(max_attempts=5)
    def __create_report_paid_storage(self, remainder, diviser, date_from, date_to):
        url = 'https://statistics-api.wildberries.ru/api/v1/delayed-gen/tasks/create'
        headers = {
            'Content-Type': 'application/json',
            'Authorization': self.wb_statistic_and_price_token
        }
        json_data = {
            "type": "paid_storage",
            "params": {
                "dateFrom": date_from,
                "dateTo": date_to,
                'diviser': diviser,
                "remainder": remainder
            }
        }
        response = requests.post(url=url, headers=headers, json=json_data)
        if response.ok:
            return response.text
        else:
            response.raise_for_status()

    @__handle_exceptions(max_attempts=5)
    def __check_status_create_paid_storage(self, tasks_id):
        tasks_id = json.loads(tasks_id)
        url = 'https://statistics-api.wildberries.ru/api/v1/delayed-gen/tasks'
        headers = {
            'Content-Type': 'application/json',
            'Authorization': self.wb_statistic_and_price_token
        }
        json_data = {
            "ids": [
                tasks_id['data']['taskId']
            ]
        }

        response = requests.get(url=url, headers=headers, json=json_data)
        if response.status_code == 200:
            if json.loads(response.text)['data']['tasks'][0]['status'] == 'error':
                return 'increase_diviser'
            if json.loads(response.text)['data']['tasks'][0]['status'] == 'done':
                return response.text
            else:
                raise Exception(f"Неизвестный статус {json.loads(response.text)['data']['tasks'][0]['status']}")
        else:
            response.raise_for_status()

    @__handle_exceptions(max_attempts=5)
    def __load_paid_storage(self, tasks):
        tasks = json.loads(tasks)
        url = 'https://statistics-api.wildberries.ru/api/v1/delayed-gen/tasks/download'
        headers = {
            'Content-Type': 'application/json',
            'Authorization': self.wb_statistic_and_price_token
        }
        json_data = {
            "id": tasks['data']['tasks'][0]['id']
        }
        response = requests.get(
            url=url,
            headers=headers,
            json=json_data
        )
        if response.status_code == 200:
            report = json.loads(response.text)
            return report
        else:
            response.raise_for_status()

    @staticmethod
    def __set_download_package_information(date_from_str, date_to_str):
        date_from = datetime.datetime.strptime(date_from_str, '%Y-%m-%d')
        date_to = datetime.datetime.strptime(date_to_str, '%Y-%m-%d')

        current_date = date_from
        dates_list = [date_from]
        days_diff_list = []

        while current_date <= date_to:
            next_date_7_days = current_date + datetime.timedelta(days=7)
            if next_date_7_days <= date_to:
                days_diff_list.append((next_date_7_days - current_date).days)
                dates_list.append(next_date_7_days)
                current_date = next_date_7_days
                next_date_1_day = current_date + datetime.timedelta(days=1)
                if next_date_1_day <= date_to:
                    dates_list.append(next_date_1_day)
                    current_date = next_date_1_day
            else:
                days_diff_list.append((date_to - current_date).days)
                dates_list.append(date_to)
                break
        if dates_list[-1] != date_to:
            days_diff_list.append((date_to - dates_list[-1]).days)
            dates_list.append(date_to)
        dates_list = [str(day.date()) for day in dates_list]
        days_diff_list = [diff - 1 if diff > 2 else diff if diff != 0 else 1 for diff in days_diff_list]
        return {'download_package_counts': days_diff_list, 'download_time_points': dates_list}

    def get_report_paid_storage(self, date_from: str, date_to: str):
        def aggregate_join_data(join_report):
            while True:
                report = data_queue.get()
                if report is None:
                    break
                sorted_data = [{'date': d['date'], 'nmId': d['nmId'], 'warehousePrice': d['warehousePrice']}
                               for d in report]
                aggregate_data = defaultdict(int)
                for element in sorted_data:
                    date_element, id_element, price_element = element.values()
                    aggregate_data[(date_element, id_element)] += price_element
                aggregate_data = dict(aggregate_data)
                aggregate_data = [(k[0], k[1], v) for k, v in aggregate_data.items()]
                join_report += aggregate_data
            return join_report

        def load_data(download_package_counts: list, download_time_points: list):
            counter_of_downloaded_report_parts = 0
            while counter_of_downloaded_report_parts < len(download_package_counts):
                reports = []
                diviser = download_package_counts[counter_of_downloaded_report_parts]
                remainder = 0
                dateFrom = download_time_points[counter_of_downloaded_report_parts * 2]
                dateTo = download_time_points[counter_of_downloaded_report_parts * 2 + 1]
                while remainder < diviser:
                    self.loger.info(f'Отчет разделен на: {diviser} частей. Часть отчета {remainder + 1}')
                    tasks_id = self.__create_report_paid_storage(remainder=remainder, diviser=diviser,
                                                                 date_from=dateFrom, date_to=dateTo)
                    # time.sleep(20)
                    tasks = self.__check_status_create_paid_storage(tasks_id=tasks_id)
                    if tasks == 'increase_diviser':
                        diviser += 1
                        remainder = 0
                        self.loger.info(f'Разделение отчета увеличено. Сейчас {diviser} части')
                        continue
                    # time.sleep(3)
                    new_report = self.__load_paid_storage(tasks=tasks)
                    # time.sleep(3)
                    reports += new_report
                    remainder += 1
                self.loger.info(f'Недельный отчет за хранение {date_from} - {date_to} загружен')
                counter_of_downloaded_report_parts += 1
                data_queue.put(reports)
            data_queue.put(None)

        data_queue = queue.SimpleQueue()
        package_download_counts, time_points = self.__set_download_package_information(
            date_from, date_to).values()
        join_report = []
        loader_thread = threading.Thread(target=load_data, args=(package_download_counts, time_points))
        aggregator_thread = threading.Thread(target=aggregate_join_data, args=(join_report, ))

        loader_thread.start()
        aggregator_thread.start()

        loader_thread.join()
        aggregator_thread.join()
        df_reports = pd.DataFrame(join_report, columns=['Дата', 'Артикул WB', 'Хранение, руб'])
        logging.info(f'Отчет за хранение c {date_from} по {date_to} загружен')
        return df_reports

    def get_price(self, quantity: int):
        url = 'https://suppliers-api.wildberries.ru/public/api/v1/info'
        headers = {'Authorization': self.wb_statistic_and_price_token}
        params = {'quantity': quantity}
        response = requests.get(url=url, headers=headers, params=params)
        report = json.loads(response.text)
        df = pd.DataFrame(report)
        return df

    def select_wb_report(self, source, date_from=None, date_to=None, limit=100000, rrdid=None,
                         quantity=None):
        if source == 'suppliers':
            return self.get_supplier_data(date_from)
        if source == 'stock':
            return self.get_stock_data(date_from)
        elif source == 'orders':
            return self.get_orders_data(date_from)
        elif source == 'sales':
            d_from = date_from if date_from is not None else 100000
            return self.get_sales_data(d_from)
        elif source == 'detailByPeriod':
            # Код ниже вероятно не нужен
            if (date_from or date_to) is not None:
                lim = limit if limit is not None else 100000
                rd = rrdid if rrdid is not None else 0
                return self.get_detail_by_period_data(date_from, date_to, lim, rd)
            else:
                raise ValueError(f'Проверьте параметры date_from: {date_from}, date_to: {date_to}')
        elif source == 'paidStorage':
            return self.get_report_paid_storage(date_from, date_to)
        elif source == 'priceInfo':
            return self.get_price(quantity)
        else:
            raise ValueError(f'Ошибка в определении параметра: {source}')


class InitialOZONDataLoader:
    def __init__(self, ozon_token):
        self.ozon_token = ozon_token

    def select_ozon_report(self):
        pass
    pass


class InitialOurDataLoader:
    def select_our_report(self):
        pass
    pass


class InitialDataLoader(InitialWBDataLoader, InitialOZONDataLoader, InitialOurDataLoader):
    def __init__(self, wb_statistic_and_price_token=None, wb_seller_analytics_token=None):
        super().__init__(wb_statistic_and_price_token, wb_seller_analytics_token)
        self.wb_statistic_and_price_token = wb_statistic_and_price_token
        self.wb_seller_analytics_token = wb_seller_analytics_token

    def select_data_source(self, data_provider, **kwargs):
        if data_provider == 'wb':
            return self.select_wb_report(**kwargs)
        elif data_provider == 'ozon':
            return self.select_ozon_report()
        elif data_provider == 'our_report':
            return self.select_our_report()
        else:
            raise ValueError(f"Unsupported data source: {data_provider}")


with open(r"C:\Users\vyacheslav\Desktop\Работа\apiKeys\api_keys.txt", 'r') as f:
    key = f.read()

t = json.loads(key)['wb_key']

loader = InitialDataLoader(wb_statistic_and_price_token=t)

data = loader.select_data_source('wb', source='paidStorage', date_from='2023-12-22', date_to='2024-01-28')
data.to_excel('data.xlsx', index=False)
# data = data.T
# data.to_excel('Хранение.xlsx', index=False)
