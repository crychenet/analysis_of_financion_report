import pandas as pd
import numpy as np


# sales_df = pd.read_excel('sales_item_summary.xlsx')
# return_df = pd.read_excel('return_item_summary.xlsx')
# initial_df = pd.read_excel('initialData.xlsx')
def get_report(item_report_df, initial_df):
    total_sales_in_actual_price = np.sum(initial_df[
                                             (initial_df['Обоснование для оплаты'] == 'Продажа') |
                                             (initial_df['Обоснование для оплаты'] == 'Сторно возвратов') |
                                             (initial_df['Обоснование для оплаты'] == 'Компенсация подмененного товара') |
                                             (initial_df['Обоснование для оплаты'] == 'Частичная компенсация брака') |
                                             (initial_df['Обоснование для оплаты'] == 'Корректная продажа')
                                             ]['Цена розничная с учетом согласованной скидки'])
    total_returns_in_actual_price = np.sum(initial_df[
                                               (initial_df['Обоснование для оплаты'] == 'Возврат') |
                                               (initial_df['Обоснование для оплаты'] == 'Корректный возврат') |
                                               (initial_df['Обоснование для оплаты'] == 'Сторно продаж')][
                                               'Цена розничная с учетом согласованной скидки'])

    total_sales_in_first_price = np.sum(initial_df[
                                            (initial_df['Обоснование для оплаты'] == 'Продажа') |
                                            (initial_df['Обоснование для оплаты'] == 'Сторно возвратов') |
                                            (initial_df['Обоснование для оплаты'] == 'Компенсация подмененного товара') |
                                            (initial_df['Обоснование для оплаты'] == 'Частичная компенсация брака') |
                                            (initial_df['Обоснование для оплаты'] == 'Корректная продажа')
                                            ]['Цена розничная'])
    total_returns_in_first_price = np.sum(initial_df[
                                              (initial_df['Обоснование для оплаты'] == 'Возврат') |
                                              (initial_df['Обоснование для оплаты'] == 'Корректный возврат') |
                                              (initial_df['Обоснование для оплаты'] == 'Сторно продаж')][
                                              'Цена розничная'])

    WB_price_sale = np.sum(initial_df[(initial_df['Обоснование для оплаты'] == 'Продажа') |
                                      (initial_df['Обоснование для оплаты'] == 'Сторно возвратов') |
                                      (initial_df['Обоснование для оплаты'] == 'Компенсация подмененного товара') |
                                      (initial_df['Обоснование для оплаты'] == 'Частичная компенсация брака') |
                                      (initial_df['Обоснование для оплаты'] == 'Корректная продажа')
                                      ]['Вайлдберриз реализовал Товар (Пр)'])
    WB_price_return = np.sum(
        initial_df[(initial_df['Обоснование для оплаты'] == 'Возврат') |
                   (initial_df['Обоснование для оплаты'] == 'Корректный возврат') |
                   (initial_df['Обоснование для оплаты'] == 'Сторно продаж')]['Вайлдберриз реализовал Товар (Пр)'])

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
        'Скидка на ПЦ (до РРЦ)': 1 - ((total_sales_in_RRP - total_returns_in_RRP) /
                                      (total_sales_in_first_price - total_returns_in_first_price)),
        'Выручка в РРЦ': total_sales_in_RRP - total_returns_in_RRP,
        'Скидка на РРЦ (до ФЦ)': 1 - ((total_sales_in_actual_price - total_returns_in_actual_price) /
                                      (total_sales_in_RRP - total_returns_in_RRP)),
        'Выручка в ФЦ': total_sales_in_actual_price - total_returns_in_actual_price,
        'Суммарные затраты внутри WB': '=B8+B9+B10+B11',
        "'- Затраты на комиссию WB": WB_commission_on_sales - WB_commission_on_returns,
        "'- Затраты на логистику": logistic_costs,
        "'- Затраты на штрафы": costs_of_fines,
        "'- Стоимость хранения": 'Вписать с WB',
        "'- Прочие удержания": 'Вписать с WB',
        'Выручка за минусом затрат внутри WB': '=B6-B7',
        'Суммарная себестоимость': cost_price_of_sales - cost_price_of_returns,
        'ЧП': '=B13-B14',
        'Рентабельность (без расходов внутри WB)': ((total_sales_in_actual_price - total_returns_in_actual_price)
                                                    - (cost_price_of_sales - cost_price_of_returns)) /
                                                   (cost_price_of_sales - cost_price_of_returns),
        'Рентабельность (общая)': '=(B15)/B13',
        'Софинансирование WB': ((total_sales_in_actual_price - total_returns_in_actual_price) -
                                (WB_price_sale - WB_price_return)),
        'Скидка WB с помощью софинансирования': 1 - ((WB_price_sale - WB_price_return) /
                                                     (total_sales_in_actual_price - total_returns_in_actual_price))
    }
    report_df = pd.DataFrame.from_dict(report, orient='index', columns=['Показатели'])
    # report_df.to_excel('report.xlsx', index=False)
    return report_df
