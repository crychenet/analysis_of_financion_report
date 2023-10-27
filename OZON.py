import sys
import pandas as pd
import numpy as np
from copy import deepcopy
from datetime import datetime

print(datetime.now())
df = pd.read_excel("C:/Users/vyacheslav/Downloads/Финансовый_отчет_по_OZON_1_15_октября.xlsx")
df_cost_of_goods = pd.read_excel("C:/Users/vyacheslav/Desktop/Работа/2023.10.09 Отчет по скидкам_111023.xlsx",
                                 skiprows=2)


def get_number_of_sales(column):
    number_of_sales = df[(df['Артикул'] == column) & (
            df['За продажу или возврат до вычета комиссий и услуг'] > 0)]['Количество'].sum()
    return number_of_sales


def get_number_of_returns(column):
    number_of_returns = df[
        (df['Артикул'] == column) & (df['За продажу или возврат до вычета комиссий и услуг'] < 0)]['Количество'].sum()
    return number_of_returns


def find_article(columns):
    first_way = columns[:-3]
    second_way = columns[:-6]

    test = df_cost_of_goods[df_cost_of_goods['Артикул 1С'] == columns]
    if not test.empty:
        return test['Артикул 1С'].iloc[0]

    test = df_cost_of_goods[df_cost_of_goods['Артикул 1С'] == first_way]
    if not test.empty:
        return test['Артикул 1С'].iloc[0]

    test = df_cost_of_goods[df_cost_of_goods['Артикул 1С'] == second_way]
    if not test.empty:
        return test['Артикул 1С'].iloc[0]
    return 'Артикул не найден'


acquiring_df = df[df['Тип начисления'] == 'Оплата эквайринга'][['Артикул', 'Итого']]
acquiring_df = acquiring_df.groupby('Артикул').sum()
acquiring_df.reset_index(drop=False, inplace=True)
acquiring_df.rename(columns={'Итого': 'Оплата экваринга'}, inplace=True)
article_report = deepcopy(df)
article_report.drop(
    columns=['SKU', 'Итого', 'Количество', 'Сборка заказа', 'Название товара или услуги', 'Склад отгрузки',
             'Дата принятия заказа в обработку или оказания услуги', 'Тип начисления',
             'Номер отправления или идентификатор услуги', 'Дата начисления',
             'Обработка отправления (Drop-off/Pick-up) (разбивается по товарам пропорционально количеству в отправлении)',
             'Магистраль', 'Обратная магистраль', 'Индекс локализации', 'Ставка комиссии'], inplace=True)
article_report = article_report.groupby('Артикул').sum()
article_report['Итоговая логистика'] = article_report[
                                           'Последняя миля (разбивается по товарам пропорционально доле цены товара в сумме отправления)'] + \
                                       article_report['Обработка возврата'] + article_report[
                                           'Обработка отмененного или невостребованного товара (разбивается по товарам в отправлении в одинаковой пропорции)'] + \
                                       article_report['Обработка невыкупленного товара'] + article_report['Логистика'] + \
                                       article_report['Обратная логистика']
article_report.reset_index(drop=False, inplace=True)

article_report['Кол-во продаж'] = article_report['Артикул'].apply(get_number_of_sales)
article_report['Кол-во возвратов'] = article_report['Артикул'].apply(get_number_of_returns)
article_report = pd.merge(article_report, acquiring_df,
                                      left_on='Артикул', right_on='Артикул', how='left')

article_report['Артикул 1С'] = article_report['Артикул'].apply(find_article)
article_report.drop(columns=['Артикул'], inplace=True)
article_report = article_report.groupby('Артикул 1С').sum()

article_report = pd.merge(
    article_report,
    df_cost_of_goods[['Артикул 1С', 'Настоящий СЕБЕК', 'КОЛЛЕКЦИЯ!!!', 'текущая РРЦ']],
    left_on='Артикул 1С', right_on='Артикул 1С', how='left'
)
article_report['Выручка в РРЦ'] = (article_report['Кол-во продаж'] -
                                   article_report['Кол-во возвратов']) * article_report['текущая РРЦ']
article_report['Итоговая логистика'] = (article_report[
                                            'Последняя миля (разбивается по товарам пропорционально доле цены товара в сумме отправления)'] +
                                        article_report['Обработка возврата'] +
                                        article_report[
                                            'Обработка отмененного или невостребованного товара (разбивается по товарам в отправлении в одинаковой пропорции)'] +
                                        article_report['Обработка невыкупленного товара'] +
                                        article_report['Логистика'] +
                                        article_report['Обратная логистика'])
article_report['Итоговая себестоимость'] = article_report['Настоящий СЕБЕК'] * (article_report['Кол-во продаж'] -
                                                                                article_report['Кол-во возвратов'])

article_report['Скидка от РРЦ'] = article_report['За продажу или возврат до вычета комиссий и услуг'] / article_report[
    'Выручка в РРЦ']

article_report['Прибыль'] = (article_report['За продажу или возврат до вычета комиссий и услуг'] -
                             article_report['Итоговая себестоимость'] +
                             article_report['Итоговая логистика'] +
                             article_report['Оплата экваринга'] +
                             article_report['Комиссия за продажу'])
article_report = article_report.replace([0], np.NaN)
article_report['Рентабельность'] = article_report['Прибыль'] / article_report['Итоговая себестоимость']
article_report.to_excel('article_report.xlsx')
