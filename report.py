import pandas as pd
import numpy as np


# sales_df = pd.read_excel('sales_item_summary.xlsx')
# return_df = pd.read_excel('return_item_summary.xlsx')
# initial_df = pd.read_excel('initialData.xlsx')
def get_report(item_report_df, initial_df):
    sales_filter = ['Продажа', 'Сторно возвратов', 'Компенсация подмененного товара', 'Частичная компенсация брака',
                    'Корректная продажа']
    return_filter = ['Возврат', 'Корректный возврат', 'Сторно продаж']

    total_sales_in_actual_price = np.sum(initial_df[initial_df['Обоснование для оплаты'].isin(
        sales_filter)]['Цена розничная с учетом согласованной скидки'])
    total_returns_in_actual_price = np.sum(initial_df[initial_df['Обоснование для оплаты'].isin(
        return_filter)]['Цена розничная с учетом согласованной скидки'])

    total_sales_in_first_price = np.sum(initial_df[initial_df['Обоснование для оплаты'].isin(
        sales_filter)]['Цена розничная'])
    total_returns_in_first_price = np.sum(initial_df[initial_df['Обоснование для оплаты'].isin(
        return_filter)]['Цена розничная'])

    WB_price_sale = np.sum(initial_df[initial_df['Обоснование для оплаты'].isin(
        sales_filter)]['Вайлдберриз реализовал Товар (Пр)'])
    WB_price_return = np.sum(initial_df[initial_df['Обоснование для оплаты'].isin(
        return_filter)]['Вайлдберриз реализовал Товар (Пр)'])

    cost_price_of_sales = np.sum(item_report_df['Себестоимость всех продаж'])
    cost_price_of_returns = np.sum(item_report_df['Себестоимость всех возвратов'])

    WB_commission_on_sales = np.sum(item_report_df['Размер кВВ всех продаж, руб'])
    WB_commission_on_returns = np.sum(item_report_df['Размер кВВ всех возвратов, руб'])

    logistic_costs = np.sum(item_report_df['Логистика, руб'])
    costs_of_fines = np.sum(item_report_df['Штраф, руб'])

    total_sales_in_RRP = np.sum(item_report_df['РРЦ'] * item_report_df['Продажи, шт'])
    total_returns_in_RRP = np.sum(item_report_df['РРЦ'] * item_report_df['Возвраты, шт'])

    report = {
        'Выручка в ПЦ': total_sales_in_first_price - total_returns_in_first_price,
        'Скидка от ПЦ до РРЦ': 1 - ((total_sales_in_RRP - total_returns_in_RRP) /
                                      (total_sales_in_first_price - total_returns_in_first_price)),
        'СПП': 1 - ((WB_price_sale - WB_price_return) /
                                                     (total_sales_in_actual_price - total_returns_in_actual_price)),
        'Выручка в РРЦ': total_sales_in_RRP - total_returns_in_RRP,
        'Скидка от РРЦ до ФЦ': 1 - ((total_sales_in_actual_price - total_returns_in_actual_price) /
                                      (total_sales_in_RRP - total_returns_in_RRP)),
        'Выручка в ФЦ': total_sales_in_actual_price - total_returns_in_actual_price,
        'Суммарные затраты внутри WB': '=B9+B10+B11+B12',
        "'- Затраты на комиссию WB": WB_commission_on_sales - WB_commission_on_returns,
        "'- Затраты на логистику": logistic_costs,
        "'- Затраты на штрафы": costs_of_fines,
        "'- Стоимость хранения": 'Вписать с WB',
        "'- Прочие удержания": 'Вписать с WB',
        'Выручка за минусом затрат внутри WB': '=B7-B8',
        'Суммарная себестоимость': cost_price_of_sales - cost_price_of_returns,
        'Прибыль': '=B14-B15',
        'Рентабельность (общая)': '=B16/B15'
    }
    report_df = pd.DataFrame.from_dict(report, orient='index', columns=['Показатели'])
    return report_df
