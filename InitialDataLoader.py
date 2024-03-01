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
import googleapiclient
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow


class InitialWBDataLoader:
    def __init__(self, wb_statistic_and_price_token=None, wb_seller_analytics=None):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.DEBUG)

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        stream_hunter = logging.StreamHandler(sys.stdout)
        stream_hunter.setLevel(logging.DEBUG)
        stream_hunter.setFormatter(formatter)
        self.logger.addHandler(stream_hunter)

        file_hunter = logging.FileHandler('Logbook.txt')
        file_hunter.setLevel(logging.DEBUG)
        file_hunter.setFormatter(formatter)
        self.logger.addHandler(file_hunter)

        self.wb_statistic_and_price_token = wb_statistic_and_price_token
        self.wb_seller_analytics = wb_seller_analytics

    @staticmethod
    def _handle_exceptions(max_attempts=5):
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                attempt = {'RequestException': 0, 'ValueError': 0, 'Other exception': 0}
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
                        args[0].logger.exception(f"Проблемы с сетью. Попытка {network_issues_attempts}"
                                                 f" из {max_attempts}.")
                    except requests.RequestException as e:
                        attempt['RequestException'] += 1
                        args[0].logger.exception(f'Ошибка при выполнении запроса: {e}')
                    except ValueError:
                        attempt['ValueError'] += 1
                        logging.exception("Неверный запрос, проверьте передаваемые данные.")
                    except Exception as e:
                        attempt['Other exception'] += 1
                        args[0].logger.exception(f'Неизвестная ошибка: {e}')
                    if attempt != max_attempts:
                        args[0].logger.info(
                            f"time.sleep увеличен до {45 + 15 * max(attempt.values())}")
                        time.sleep(60 + 15 * max(attempt.values()))
                if max(attempt.values()) == max_attempts:
                    raise Exception(f"Превышено максимальное количество попыток из-за ошибок {attempt}")

            return wrapper

        return decorator

    @_handle_exceptions(max_attempts=3)
    def get_supplier_data(self, date_from):
        url = 'https://statistics-api.wildberries.ru/api/v1/supplier/incomes'
        headers = {'Authorization': self.wb_statistic_and_price_token}
        params = {'dateFrom': date_from}
        response = requests.get(url=url, headers=headers, params=params)
        if response.ok:
            report = json.loads(response.text)
            df = pd.DataFrame(report)
            return df
        else:
            response.raise_for_status()

    @_handle_exceptions(max_attempts=3)
    def get_stock_data(self, date_from):
        url = 'https://statistics-api.wildberries.ru/api/v1/supplier/stocks'
        headers = {'Authorization': self.wb_statistic_and_price_token}
        params = {'dateFrom': date_from}
        response = requests.get(url=url, headers=headers, params=params)
        if response.ok:
            report = json.loads(response.text)
            df = pd.DataFrame(report)
            return df
        else:
            response.raise_for_status()

    @_handle_exceptions(max_attempts=3)
    def get_orders_data(self, date_from):
        url = 'https://statistics-api.wildberries.ru/api/v1/supplier/orders'
        headers = {'Authorization': self.wb_statistic_and_price_token}
        params = {'dateFrom': date_from}
        response = requests.get(url=url, headers=headers, params=params)
        if response.ok:
            report = json.loads(response.text)
            df = pd.DataFrame(report)
            return df
        else:
            response.raise_for_status()

    @_handle_exceptions(max_attempts=3)
    def get_sales_data(self, date_from):
        url = 'https://statistics-api.wildberries.ru/api/v1/supplier/sales'
        headers = {'Authorization': self.wb_statistic_and_price_token}
        params = {'dateFrom': date_from}
        response = requests.get(url=url, headers=headers, params=params)
        if response.ok:
            report = json.loads(response.text)
            df = pd.DataFrame(report)
            return df
        else:
            response.raise_for_status()

    @_handle_exceptions(max_attempts=3)
    def __load_detail_by_period_data(self, date_from, date_to, limit, rrdid):
        url = 'https://statistics-api.wildberries.ru/api/v1/supplier/reportDetailByPeriod'
        headers = {'Authorization': self.wb_statistic_and_price_token}
        params = {
            'dateFrom': date_from,
            'limit': limit,
            'dateTo': date_to,
            'rrdid': rrdid
        }
        response = requests.get(url=url, headers=headers, params=params)
        if response.ok:
            report = json.loads(response.text)
            return report
        else:
            response.raise_for_status()

    def get_detail_by_period_data(self, date_from, date_to, limit, rrdid):
        if limit is None:
            limit = 100000
        if rrdid is None:
            rrdid = 0
        reports = []
        path_report = 0
        while True:
            new_report = self.__load_detail_by_period_data(date_from, date_to, limit, rrdid)
            reports += new_report
            if len(new_report) < 100000:
                break
            else:
                rrdid = new_report[-1]['rrd_id']
            self.logger.info(f'Часть отчета {path_report + 1}')
            path_report += 1
        df = pd.DataFrame(reports)
        return df

    @_handle_exceptions(max_attempts=5)
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

    @_handle_exceptions(max_attempts=5)
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

    @_handle_exceptions(max_attempts=5)
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
            test_data = current_date + datetime.timedelta(days=8)
            if test_data <= date_to:
                current_date += datetime.timedelta(days=7)
                days_diff_list.append(7)
                dates_list.append(current_date)
                current_date += datetime.timedelta(days=1)
                dates_list.append(current_date)
            else:
                dates_list.append(date_to)
                days_diff_list.append((date_to - current_date).days)
                break
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
                    self.logger.info(f'Отчет разделен на: {diviser} частей. Часть отчета {remainder + 1}')
                    tasks_id = self.__create_report_paid_storage(remainder=remainder, diviser=diviser,
                                                                 date_from=dateFrom, date_to=dateTo)
                    time.sleep(20)
                    tasks = self.__check_status_create_paid_storage(tasks_id=tasks_id)
                    if tasks == 'increase_diviser':
                        diviser += 1
                        remainder = 0
                        self.logger.info(f'Разделение отчета увеличено. Сейчас {diviser} части')
                        continue
                    time.sleep(3)
                    new_report = self.__load_paid_storage(tasks=tasks)
                    time.sleep(3)
                    reports += new_report
                    remainder += 1
                self.logger.info(f'Недельный отчет за хранение {dateFrom} - {dateTo} загружен')
                counter_of_downloaded_report_parts += 1
                data_queue.put(reports)
            data_queue.put(None)

        data_queue = queue.SimpleQueue()
        package_download_counts, time_points = self.__set_download_package_information(date_from, date_to).values()
        join_report = []
        loader_thread = threading.Thread(target=load_data, args=(package_download_counts, time_points))
        aggregator_thread = threading.Thread(target=aggregate_join_data, args=(join_report,))

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

    def select_wb_report(self, source, date_from=None, date_to=None, limit=None, rrdid=None,
                         quantity=None):
        if source == 'suppliers':
            return self.get_supplier_data(date_from)
        if source == 'stock':
            return self.get_stock_data(date_from)
        elif source == 'orders':
            return self.get_orders_data(date_from)
        elif source == 'sales':
            return self.get_sales_data(date_from)
        elif source == 'detailByPeriod':
            return self.get_detail_by_period_data(date_from, date_to, limit, rrdid)
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
    def __init__(self, configurator_downloading_our_reports, data_source_folder_id='10ZDgPLizSw-M7hZm3QkE7oTe-HKRNRdU'):
        self.scope = ["https://www.googleapis.com/auth/drive.metadata.readonly",
                      "https://www.googleapis.com/auth/spreadsheets.readonly"]
        self.data_source_folder_id = data_source_folder_id
        self.configurator_downloading_our_reports = configurator_downloading_our_reports
        with open(self.configurator_downloading_our_reports, 'r') as config_file:
            content = config_file.read()
            self.config = json.loads(content)

    @staticmethod
    def _handle_exceptions(max_attempts=1):
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                attempts = 0
                try:
                    return func(*args, **kwargs)
                except googleapiclient.errors.HttpError as err:
                    attempts += 1
                    print(f'Ошибка {err}')
                if attempts == max_attempts:
                    raise Exception('Много ошибочек')
            return wrapper
        return decorator

    @_handle_exceptions()
    def authenticate_in_google_services(self):
        creds = None
        if 'token' in self.config.keys():
            creds = self.config["token"]
            creds = Credentials.from_authorized_user_info(creds)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_config(self.config["credentials"], self.scope)
                creds = flow.run_local_server(port=0)
            self.config['token'] = json.loads(creds.to_json())
            with open("configuratorDownloadOurReports.json", "w") as conf_file:
                json.dump(self.config, conf_file)

    @_handle_exceptions()
    def get_list_our_report(self):
        creds = Credentials.from_authorized_user_info(self.config['token'], self.scope)
        service = build("drive", "v3", credentials=creds)
        results = service.files().list(
            q=f"'{self.data_source_folder_id}' in parents",
            fields="nextPageToken, files(id, name)"
        ).execute()
        items = results.get("files", [])
        return items

    @_handle_exceptions()
    def download_report(self, sample_spreadsheet_id, sample_range_name=None):
        if sample_range_name is None:
            sample_range_name = "TDSheet!A1:AAA"
        creds = Credentials.from_authorized_user_info(self.config['token'], self.scope)
        service = build("sheets", "v4", credentials=creds)

        sheet = service.spreadsheets()
        result = (
            sheet.values()
            .get(spreadsheetId=sample_spreadsheet_id, range=sample_range_name)
            .execute()
        )
        values = result.get("values", [])
        if not values:
            raise Exception('Какая то фигня')
        return pd.DataFrame(values[1:], columns=values[0])

    @staticmethod
    def get_list_selected_report(all_our_reports, type_of_report):
        def parse_report_date(report_name):
            try:
                _, date_str = report_name.split('_')
                return datetime.datetime.strptime(date_str, '%Y.%m.%d')
            except ValueError:
                raise Exception(f'Invalid date format in report name: {report_name}')

        report_types = {
            'stockReport': 'OurStock',
            'priceReport': 'OurPrice',
            'collectionsReport': 'OurCollections',
            'mappingReport': 'OurMappingFile'
        }
        flag_type = report_types.get(type_of_report)
        if not flag_type:
            raise Exception('Unrecognized type report')

        selected_reports = [
            {
                'id': report['id'],
                'date_type': parse_report_date(report['name']),
                'name_report': report['name']
            }
            for report in all_our_reports if report['name'].startswith(flag_type)
        ]
        selected_reports.sort(key=lambda x: x['date_type'])
        return selected_reports

    def select_our_report(self, source, sample_range_name=None, report_date=None):
        if isinstance(report_date, str):
            report_date = datetime.datetime.strptime(report_date, '%Y-%m-%d')
        selected_reports = self.get_list_selected_report(self.get_list_our_report(), type_of_report=source)
        date_selected_reports = [report['date_type'] for report in selected_reports]
        if (report_date not in date_selected_reports) and (report_date is not None):
            raise Exception(f'Отчета {source} с датой {report_date} не существует.\n'
                            f'Возможные даты отчета {date_selected_reports}')
        if report_date is None:
            id_report_to_download = selected_reports[-1]['id']
            return self.download_report(id_report_to_download, sample_range_name)
        else:
            for report in selected_reports:
                if report['date_type'] == report_date:
                    id_report_to_download = report['id']
                    return self.download_report(id_report_to_download, sample_range_name)


class InitialDataLoader:
    def __init__(self, wb_statistic_and_price_token=None, wb_seller_analytics_token=None,
                 configurator_downloading_our_reports=None, ozon_token=None):
        self.ourDataLoader = InitialOurDataLoader(configurator_downloading_our_reports)
        self.wbDataLoader = InitialWBDataLoader(wb_statistic_and_price_token, wb_seller_analytics_token)
        self.ozonDataLoader = InitialOZONDataLoader(ozon_token)

    def select_data_source(self, data_provider, **kwargs):
        if data_provider == 'wb':
            return self.wbDataLoader.select_wb_report(**kwargs)
        elif data_provider == 'ozon':
            return self.ozonDataLoader.select_ozon_report()
        elif data_provider == 'our_report':
            return self.ourDataLoader.select_our_report(**kwargs)
        else:
            raise ValueError(f"Unsupported data source: {data_provider}")


with open(r"C:\Users\vyacheslav\Desktop\Работа\apiKeys\api_keys.txt", 'r') as f:
    key = f.read()

# t = json.loads(key)['wb_key']
#
# loader = InitialDataLoader(wb_statistic_and_price_token=t)
#
# data = loader.select_data_source('wb', source='paidStorage', date_from='2023-11-27', date_to='2023-11-03')
# data.to_excel('data.xlsx', index=False)
# data = data.T
# data.to_excel('Хранение 27-03 ноября.xlsx', index=False)
loader = InitialDataLoader(configurator_downloading_our_reports='configuratorDownloadOurReports.json')
data = loader.select_data_source('our_report', source='mappingReport')
# data = loader.authenticate_in_google_services()
print(data)

# loader = InitialOurDataLoader(configurator_downloading_our_reports='configuratorDownloadOurReports.json')
# data = loader.get_list_our_report()
# print(data)
