import numpy as np
import pandas as pd


class ArticleReport:
    def __init__(self, df, df_cost_of_goods):
        self.df = df
        self.df_cost_of_goods = df_cost_of_goods
        self.sales_filter = ['Продажа', 'Сторно возвратов', 'Компенсация подмененного товара',
                             'Частичная компенсация брака', 'Корректная продажа']
        self.return_filter = ['Возврат', 'Корректный возврат', 'Сторно продаж']

    def get_fact_price_sales(self, column):
        fact_price = np.sum(self.df[(self.df['Артикул поставщика'] == column) & (
            self.df['Обоснование для оплаты'].isin(self.sales_filter))]['Сумма фактических продаж'])
        return fact_price

    def get_fact_price_returns(self, column):
        fact_price = np.sum(self.df[(self.df['Артикул поставщика'] == column) & (
            self.df['Обоснование для оплаты'].isin(self.return_filter))]['Сумма фактических продаж'])
        return fact_price

    def get_number_of_sales(self, column):
        number_of_sales = self.df[(self.df['Артикул поставщика'] == column) & (
                    (self.df['Обоснование для оплаты'] == 'Продажа') | (
                        self.df['Обоснование для оплаты'] == "Корректная продажа") | (
                                self.df['Обоснование для оплаты'] == 'Сторно возвратов'))]['Кол-во'].sum()
        return number_of_sales

    def get_number_of_returns(self, column):
        number_of_returns = self.df[(self.df['Артикул поставщика'] == column) & (
                    (self.df['Обоснование для оплаты'] == 'Возврат') | (
                        self.df['Обоснование для оплаты'] == 'Корректный возврат') | (
                                self.df['Обоснование для оплаты'] == 'Сторно продаж'))]['Кол-во'].sum()
        return number_of_returns

    def get_commission_WB_sales(self, column):
        commission_WB = np.sum(self.df[(self.df['Артикул поставщика'] == column) & (
            self.df['Обоснование для оплаты'].isin(self.sales_filter))]['Размер кВВ, руб'])
        return commission_WB

    def get_commission_WB_returns(self, column):
        commission_WB = np.sum(self.df[(self.df['Артикул поставщика'] == column) & (
            self.df['Обоснование для оплаты'].isin(self.return_filter))]['Размер кВВ, руб'])
        return commission_WB

    def get_logistic_costs(self, column):
        commission_WB = np.sum(self.df[(self.df['Тип документа'] == 'Продажа') & (
                    self.df['Обоснование для оплаты'] == 'Логистика') & (self.df['Артикул поставщика'] == column)][
                                   'Услуги по доставке товара покупателю'])
        return commission_WB

    def get_fine(self, column):
        fine = np.sum(self.df[(self.df['Артикул поставщика'] == column) & (
                    self.df['Обоснование для оплаты'] == 'Штрафы') & (self.df['Тип документа'] == 'Продажа')][
                          'Общая сумма штрафов'])
        return fine

    def get_hyperlink(self, column):
        hyperlink = "https://www.wildberries.ru/catalog/" + str(column) + "/detail.aspx?targetUrl=SP"
        return hyperlink

    def generate_article_report(self):
        self.df['Цена РРЦ'] = 0.66 * self.df['Цена розничная']
        self.df['Сумма продаж в РРЦ'] = self.df['Цена РРЦ'] * self.df['Кол-во']
        self.df['Сумма фактических продаж'] = self.df['Цена розничная с учетом согласованной скидки'] * self.df[
            'Кол-во']
        self.df['Скидка от РРЦ'] = self.df['Цена розничная с учетом согласованной скидки'] / self.df['Цена РРЦ']
        self.df['Скидка от РРЦ'] = self.df['Скидка от РРЦ'].replace([np.inf, -np.inf, np.nan], 0)
        self.df['Размер кВВ, руб'] = self.df['Размер кВВ, %'] * self.df['Сумма фактических продаж']

        unique_article = self.df['Артикул поставщика'].unique()
        article_report = pd.DataFrame(unique_article)
        article_report = article_report.rename(columns={0: 'Артикул поставщика'})

        article_report = pd.merge(article_report, self.df[['Код номенклатуры', 'Артикул поставщика']].drop_duplicates(
            subset='Код номенклатуры'), on='Артикул поставщика', how='left')

        article_report = pd.merge(article_report, self.df_cost_of_goods[
            ['Артикул продавца', 'Настоящий СЕБЕК', 'КОЛЛЕКЦИЯ!!!',
             'текущая РРЦ']], left_on='Артикул поставщика', right_on='Артикул продавца', how='left')

        article_report['Выручка с продаж, руб'] = article_report['Артикул поставщика'].apply(self.get_fact_price_sales)
        article_report['Продажи, шт'] = article_report['Артикул поставщика'].apply(self.get_number_of_sales)
        article_report['Себестоимость всех продаж'] = article_report['Продажи, шт'] * article_report['Настоящий СЕБЕК']
        article_report['Размер кВВ всех продаж, руб'] = article_report['Артикул поставщика'].apply(
            self.get_commission_WB_sales)
        article_report['Логистика, руб'] = article_report['Артикул поставщика'].apply(self.get_logistic_costs)
        article_report['Штраф, руб'] = article_report['Артикул поставщика'].apply(self.get_fine)
        article_report['Ссылка WB'] = article_report['Код номенклатуры'].apply(self.get_hyperlink)

        article_report['Возвраты, шт'] = article_report['Артикул поставщика'].apply(self.get_number_of_returns)
        article_report['Возврат выручки, руб'] = article_report['Артикул поставщика'].apply(self.get_fact_price_returns)
        article_report['Себестоимость всех возвратов'] = article_report['Возвраты, шт'] * article_report[
            'Настоящий СЕБЕК']
        article_report['Размер кВВ всех возвратов, руб'] = article_report['Артикул поставщика'].apply(
            self.get_commission_WB_returns)

        article_report['Итоговая выручка'] = (
                    article_report['Выручка с продаж, руб'] - article_report['Возврат выручки, руб'])
        article_report['Итоговая себестоимость'] = (
                    article_report['Себестоимость всех продаж'] - article_report['Себестоимость всех возвратов'])
        article_report['Итоговый размер кВВ'] = (
                    article_report['Размер кВВ всех продаж, руб'] - article_report['Размер кВВ всех возвратов, руб'])

        article_report['ЧП (без хранения)'] = (article_report['Итоговая выручка'] - (
                    article_report['Итоговая себестоимость'] + article_report['Итоговый размер кВВ'] + article_report[
                'Логистика, руб'] + article_report['Штраф, руб']))
        article_report = article_report.replace([0], np.NaN)
        article_report['Рентабельность'] = (
                    article_report['ЧП (без хранения)'] / article_report['Итоговая себестоимость'])
        article_report['Совокупная скидка к РРЦ'] = 1 - (article_report['Итоговая выручка'] / (
                    article_report['РРЦ'] * (article_report['Продажи, шт'] - article_report['Возвраты, шт'])))

        article_report.drop(columns=['Артикул поставщика'], inplace=True)
        article_report.rename(
            columns={"Настоящий СЕБЕК": "Cебек", "КОЛЛЕКЦИЯ!!!": "Коллекция", 'Код номенклатуры': 'артикул WB',
                     'текущая РРЦ': 'РРЦ'}, inplace=True)

        article_report = article_report.iloc[:,
                         [0, 1, 3, 11, 2, 4, 6, 5, 7, 8, 9, 10, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21]]

        return article_report


# Пример использования:
if __name__ == "__main__":
    df_ = pd.read_excel("C:/Users/vyacheslav/Desktop/Работа/детализированный финансовый отчет 16-22 октября.xlsx")
    df_cost_of_goods_ = pd.read_excel("C:/Users/vyacheslav/Desktop/Работа/2023.10.09 Отчет по скидкам_111023.xlsx",
                                     skiprows=2)
    report_generator = ArticleReport(df_, df_cost_of_goods_)
    report = report_generator.generate_article_report()
    print(report)

