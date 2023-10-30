import sys
import pandas as pd
import numpy as np
from copy import deepcopy
from datetime import datetime

print(datetime.now())
df_ = pd.read_excel("C:/Users/vyacheslav/Downloads/Финансовый_отчет_по_OZON_1_15_октября.xlsx")
df_cost_of_goods_ = pd.read_excel("C:/Users/vyacheslav/Desktop/Работа/2023.10.09 Отчет по скидкам_111023.xlsx",
                                  skiprows=2)


def get_article_report(df, df_cost_of_goods):
    def get_number_of_sales(column):
        number_of_sales = df[(df['Артикул'] == column) & (
                df['За продажу или возврат до вычета комиссий и услуг'] > 0)]['Количество'].sum()
        return number_of_sales

    def get_number_of_returns(column):
        number_of_returns = df[
            (df['Артикул'] == column) & (df['За продажу или возврат до вычета комиссий и услуг'] < 0)][
            'Количество'].sum()
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
                                           article_report['Обработка невыкупленного товара'] + article_report[
                                               'Логистика'] + \
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

    article_report['Скидка от РРЦ'] = (article_report['За продажу или возврат до вычета комиссий и услуг'] / \
                                       article_report['Выручка в РРЦ'])

    article_report['Прибыль'] = (article_report['За продажу или возврат до вычета комиссий и услуг'] -
                                 article_report['Итоговая себестоимость'] +
                                 article_report['Итоговая логистика'] +
                                 article_report['Оплата экваринга'] +
                                 article_report['Комиссия за продажу'])
    article_report = article_report.replace([0], np.NaN)
    article_report['Рентабельность'] = article_report['Прибыль'] / article_report['Итоговая себестоимость']

    article_report.rename(columns={'За продажу или возврат до вычета комиссий и услуг': 'Итоговая выручка',
                                   'Настоящий СЕБЕК': 'Себек',
                                   'КОЛЛЕКЦИЯ!!!': 'Коллекция',
                                   'текущая РРЦ': 'РРЦ',
                                   'Кол-во продаж': 'Продажи, шт',
                                   'Кол-во возвратов': 'Возвраты, шт'}, inplace=True)
    article_report = article_report.iloc[:, [0, 14, 10, 11, 13, 15, 16, 3, 4, 5, 6, 7, 8, 9, 2, 12, 17, 1, 18, 19, 20]]
    return article_report


def get_data_grouped_by_season(article_df):
    article_df = article_df[['Продажи, шт', 'Возвраты, шт', 'Итоговая себестоимость', 'Итоговая выручка',
                             'Итоговая логистика', 'Комиссия за продажу', 'Оплата экваринга', 'Коллекция']]
    data_grouped_by_season = article_df.groupby('Коллекция').sum()
    data_grouped_by_season['ЧП'] = (data_grouped_by_season['Итоговая выручка'] -
                                    data_grouped_by_season['Итоговая себестоимость'] +
                                    data_grouped_by_season['Итоговая логистика'] +
                                    data_grouped_by_season['Комиссия за продажу'] +
                                    data_grouped_by_season['Оплата экваринга'])
    data_grouped_by_season = data_grouped_by_season.replace([0], np.NaN)
    data_grouped_by_season['Рентабельность'] = (data_grouped_by_season['ЧП'] /
                                                data_grouped_by_season['Итоговая себестоимость'])
    return data_grouped_by_season


def get_report(initial_df, article_df):
    list_various_services = ['Приобретение отзывов на платформе', 'Услуги продвижения товаров',
                             'Доставка возвратов до склада продавца силами Ozon', 'Инвентаризация взаиморасчетов',
                             'Услуга по бронированию места и персонала для поставки с неполным составом',
                             'Услуга по обработке опознанных излишков', 'Утилизация',
                             'Обработка неопознанных излишков с приемки', 'Подписка Premium Plus']
    sum_various_services = 0
    for service in list_various_services:
        sum_various_services += np.sum(initial_df[initial_df['Тип начисления'] == service]['Итого'])
    sum_various_services = -(sum_various_services + np.sum(article_df['Оплата экваринга']))
    actual_at_recommended_market_prices = np.sum(article_df['Выручка в РРЦ'])
    actual_at_revenue_prices = np.sum(initial_df['За продажу или возврат до вычета комиссий и услуг'])
    OZON_commission = -np.sum(article_df['Комиссия за продажу'])
    logistic_costs = -np.sum(article_df['Итоговая логистика'])
    costs_of_fines = 0  # Пока что нет такого поля
    storage_costs = 0  # Пока что нет такого поля
    summarize_costs_within_OZON = (OZON_commission + logistic_costs + costs_of_fines +
                                   storage_costs + sum_various_services)
    cost_price = np.sum(article_df['Итоговая себестоимость'])
    report = {
        'Выручка в РРЦ': actual_at_recommended_market_prices,
        'Скидка от РРЦ до ФЦ': 1 - (actual_at_revenue_prices / actual_at_recommended_market_prices),
        'Выручка в ФЦ': actual_at_revenue_prices,
        'Суммарные затраты внутри OZON': summarize_costs_within_OZON,
        "'- Затраты на комиссию OZON": OZON_commission,
        "'- Затраты на логистику": logistic_costs,
        "'- Затраты на штрафы": costs_of_fines,
        "'- Стоимость хранения": storage_costs,
        "'- Прочие удержания": sum_various_services,
        'Выручка за минусом затрат внутри OZON': actual_at_revenue_prices - summarize_costs_within_OZON,
        'Суммарная себестоимость': cost_price,
        'Прибыль': actual_at_revenue_prices - summarize_costs_within_OZON - cost_price,
        'Рентабельность (общая)': (actual_at_revenue_prices - summarize_costs_within_OZON - cost_price) / cost_price
    }
    report_df = pd.DataFrame.from_dict(report, orient='index', columns=['Показатели'])
    return report_df


df_article_report = get_article_report(df_, df_cost_of_goods_)
print(f'Сформировали отчет по артикулам: {datetime.now()}')
df_seasonal_analysis = get_data_grouped_by_season(df_article_report)
print(f'Сформировали отчет по сезонам: {datetime.now()}')
df_report = get_report(initial_df=df_, article_df=df_article_report)
print(f'Сформировали итоговый отчет: {datetime.now()}')


with pd.ExcelWriter('OZON 1-15 октября.xlsx', engine='xlsxwriter') as writer:
    df_article_report.to_excel(writer, sheet_name='Отчет по артикулам', index=False)
    df_seasonal_analysis.to_excel(writer, sheet_name='Отчет по сезонам')
    df_report.to_excel(writer, sheet_name='Итоговый отчет')

print(f'Сохранили файл: {datetime.now()}')
