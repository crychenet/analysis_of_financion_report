import pandas as pd
import numpy as np


class FinancialReport:
    def __init__(self, df, cost_of_goods_data, sales_filter, returns_filter):
        # Инициализация класса с данными о продажах и стоимости товаров
        self.unique_article = None
        self.df = df
        self.cost_of_goods_data = cost_of_goods_data
        self.sales_filter = sales_filter
        self.returns_filter = returns_filter

    def preprocess_data(self):
        self.df['Цена РРЦ'] = 0.66 * self.df['Цена розничная']
        self.df['Сумма продаж в РРЦ'] = self.df['Цена РРЦ'] * self.df['Кол-во']
        self.df['Сумма фактических продаж'] = self.df['Цена розничная с учетом согласованной скидки'] * self.df['Кол-во']
        self.df['Скидка от РРЦ'] = self.df['Цена розничная с учетом согласованной скидки'] / self.df['Цена РРЦ']
        self.df['Скидка от РРЦ'] = self.df['Скидка от РРЦ'].replace([np.inf, -np.inf, np.nan], 0)
        self.df['Размер кВВ, руб'] = self.df['Размер кВВ, %'] * self.df['Сумма фактических продаж']
        return self


        # Метод для вычисления суммы фактических продаж по заданному столбцу

    def get_fact_returns_information(self, column):
        pass
        # Метод для вычисления суммы фактических возвратов по заданному столбцу

    # Другие методы для вычисления показателей

    def generate_article_report(self, df, df_cost_of_goods):
        self.unique_article = df['Артикул поставщика'].unique()
        article_report = pd.DataFrame(self.unique_article)
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

        pass
        # Метод для формирования отчета по артикулам

    def generate_data_grouped_by_season(self):
        pass
        # Метод для формирования отчета, сгруппированного по сезонам

    def generate_report(self):
        pass
        # Метод для создания общего отчета



