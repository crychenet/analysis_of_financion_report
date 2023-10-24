import pandas as pd
import numpy as np


# df = pd.read_excel("C:/Users/vyacheslav/Desktop/Работа/детализированный финансовый отчет 16-22 октября.xlsx")
# df_cost_of_goods = pd.read_excel("C:/Users/vyacheslav/Desktop/Работа/2023.10.09 Отчет по скидкам_111023.xlsx",
#                                  skiprows=2)


def get_article_report(df, df_cost_of_goods):
    def get_fact_price_sales(column):
        fact_price = df[(df['Артикул поставщика'] == column) & (
                (df['Обоснование для оплаты'] == 'Продажа') |
                (df['Обоснование для оплаты'] == 'Сторно возвратов') |
                (df['Обоснование для оплаты'] == 'Компенсация подмененного товара') |
                (df['Обоснование для оплаты'] == 'Частичная компенсация брака') |
                (df['Обоснование для оплаты'] == 'Корректная продажа')
        )]['Сумма фактических продаж'].sum()
        return fact_price

    def get_fact_price_returns(column):
        fact_price = df[(df['Артикул поставщика'] == column) & ((df['Обоснование для оплаты'] == 'Возврат') |
                                                                (df['Обоснование для оплаты'] == 'Сторно продаж') |
                                                                (df['Обоснование для оплаты'] == 'Корректный возврат'))][
            'Сумма фактических продаж'].sum()
        return fact_price

    def get_number_of_sales(column):
        number_of_sales = df[(df['Артикул поставщика'] == column) & (
                (df['Обоснование для оплаты'] == 'Продажа') |
                (df['Обоснование для оплаты'] == "Корректная продажа") |
                (df['Обоснование для оплаты'] == 'Сторно возвратов'))]['Кол-во'].sum()
        return number_of_sales

    def get_number_of_returns(column):
        number_of_returns = df[
            (df['Артикул поставщика'] == column) & ((df['Обоснование для оплаты'] == 'Возврат') |
                                                    (df['Обоснование для оплаты'] == 'Корректный возврат') |
                                                    (df['Обоснование для оплаты'] == 'Сторно продаж'))]['Кол-во'].sum()
        return number_of_returns

    def get_commission_WB_sales(column):
        commission_WB = df[(df['Артикул поставщика'] == column) & (
                (df['Обоснование для оплаты'] == 'Продажа') |
                (df['Обоснование для оплаты'] == 'Сторно возвратов') |
                (df['Обоснование для оплаты'] == 'Компенсация подмененного товара') |
                (df['Обоснование для оплаты'] == 'Частичная компенсация брака') |
                (df['Обоснование для оплаты'] == 'Корректная продажа')
        )]['Размер кВВ, руб'].sum()
        return commission_WB

    def get_commission_WB_returns(column):
        commission_WB = df[(df['Артикул поставщика'] == column) & ((df['Обоснование для оплаты'] == 'Возврат') |
                                                                   (df['Обоснование для оплаты'] == 'Корректный возврат') |
                                                                   (df['Обоснование для оплаты'] == 'Сторно продаж'))][
            'Размер кВВ, руб'].sum()
        return commission_WB

    def get_logistic_costs(column):
        commission_WB = df[(df['Тип документа'] == 'Продажа') & (df['Обоснование для оплаты'] == 'Логистика') &
                           (df['Артикул поставщика'] == column)]['Услуги по доставке товара покупателю'].sum()
        return commission_WB

    def get_fine(column):
        fine = df[(df['Артикул поставщика'] == column) & (df['Обоснование для оплаты'] == 'Штрафы') &
                  (df['Тип документа'] == 'Продажа')]['Общая сумма штрафов'].sum()
        return fine

    def get_hyperlink(column):
        hyperlink = "https://www.wildberries.ru/catalog/" + str(column) + "/detail.aspx?targetUrl=SP"
        return hyperlink

    df['Цена РРЦ'] = 0.66 * df['Цена розничная']
    df['Сумма продаж в РРЦ'] = df['Цена РРЦ'] * df['Кол-во']
    df['Сумма фактических продаж'] = df['Цена розничная с учетом согласованной скидки'] * df['Кол-во']
    df['Скидка от РРЦ'] = df['Цена розничная с учетом согласованной скидки'] / df['Цена РРЦ']
    df['Скидка от РРЦ'] = df['Скидка от РРЦ'].replace([np.inf, -np.inf, np.nan], 0)
    df['Размер кВВ, руб'] = df['Размер кВВ, %'] * df['Сумма фактических продаж']

    unique_sales_item = df['Артикул поставщика'].unique()
    article_report = pd.DataFrame(unique_sales_item)
    article_report = article_report.rename(columns={0: 'Артикул поставщика'})

    article_report = pd.merge(
        article_report, df[['Код номенклатуры', 'Артикул поставщика']].drop_duplicates(
            subset='Код номенклатуры'), on='Артикул поставщика', how='left'
    )
    article_report = pd.merge(
        article_report,
        df_cost_of_goods[['Артикул продавца', 'Настоящий СЕБЕК', 'КОЛЛЕКЦИЯ!!!', 'текущая РРЦ']],
        left_on='Артикул поставщика', right_on='Артикул продавца', how='left'
    )
    article_report['Выручка с продаж, руб'] = article_report['Артикул поставщика'].apply(get_fact_price_sales)
    article_report['Продажи, шт'] = article_report['Артикул поставщика'].apply(get_number_of_sales)
    article_report['Себестоимость всех продаж'] = (
            article_report['Продажи, шт'] * article_report['Настоящий СЕБЕК'])
    article_report['Размер кВВ всех продаж, руб'] = article_report['Артикул поставщика'].apply(get_commission_WB_sales)
    article_report['Логистика, руб'] = article_report['Артикул поставщика'].apply(get_logistic_costs)
    article_report['Штраф, руб'] = article_report['Артикул поставщика'].apply(get_fine)
    article_report['Ссылка WB'] = article_report['Код номенклатуры'].apply(get_hyperlink)
    #
    article_report['Возвраты, шт'] = article_report['Артикул поставщика'].apply(get_number_of_returns)
    article_report['Возврат выручки, руб'] = article_report['Артикул поставщика'].apply(get_fact_price_returns)
    article_report['Себестоимость всех возвратов'] = (
            article_report['Возвраты, шт'] * article_report['Настоящий СЕБЕК'])
    article_report['Размер кВВ всех возвратов, руб'] = article_report['Артикул поставщика'].apply(
        get_commission_WB_returns)
    ##
    article_report['Итоговая выручка'] = (article_report['Выручка с продаж, руб'] -
                                          article_report['Возврат выручки, руб'])
    article_report['Итоговая себестоимость'] = (article_report['Себестоимость всех продаж'] -
                                                article_report['Себестоимость всех возвратов'])
    article_report['Итоговый размер кВВ'] = (article_report['Размер кВВ всех продаж, руб'] -
                                             article_report['Размер кВВ всех возвратов, руб'])
    ##
    article_report['ЧП (без хранения)'] = (
            article_report['Итоговая выручка'] - (article_report['Итоговая себестоимость'] +
                                                  article_report['Итоговый размер кВВ'] +
                                                  article_report['Логистика, руб'] +
                                                  article_report['Штраф, руб']))
    article_report = article_report.replace([0], np.NaN)
    article_report['Рентабельность'] = (article_report['ЧП (без хранения)'] /
                                        article_report['Итоговая себестоимость'])
    article_report['Совокупная скидка к РРЦ'] = 1 - (
            article_report['Итоговая выручка'] / (article_report['текущая РРЦ'] * (
            article_report['Продажи, шт'] - article_report['Возвраты, шт'])))

    article_report.drop(columns=['Артикул поставщика'], inplace=True)
    article_report.rename(columns={"Настоящий СЕБЕК": "Cебек", "КОЛЛЕКЦИЯ!!!": "Коллекция",
                                   'Код номенклатуры': 'артикул WB', 'текущая РРЦ': 'РРЦ'}, inplace=True)

    article_report = article_report.iloc[:, [0, 1, 3, 11, 2, 4, 6, 5, 7, 8, 9, 10, 12, 13, 14, 15, 16,
                                             17, 18, 19, 20, 21]]
    return article_report
