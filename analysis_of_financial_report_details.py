import pandas as pd
import article_report_WB
import report
import seasonal_analysis
from datetime import datetime


start_time = datetime.now()
print(start_time)
df = pd.read_excel("C:/Users/vyacheslav/Desktop/Работа/детализированный финансовый отчет 16-22 октября.xlsx")
df_cost_of_goods = pd.read_excel("C:/Users/vyacheslav/Desktop/Работа/2023.10.09 Отчет по скидкам_111023.xlsx",
                                 skiprows=2)

df_product_sales = article_report_WB.get_article_report(df=df, df_cost_of_goods=df_cost_of_goods)
print(f'Сформировали отчет по артикулам: {datetime.now()}')
df_report = report.get_report(item_report_df=df_product_sales, initial_df=df)
print(f'Сформировали итоговый отчет: {datetime.now()}')
df_seasonal_analysis = seasonal_analysis.get_data_grouped_by_season(season_df=df_product_sales)
print(f'Сформировали отчет по сезонам: {datetime.now()}')

with pd.ExcelWriter('Финансовый отчет 16-22 октября_test.xlsx', engine='xlsxwriter') as writer:
    df_product_sales.to_excel(writer, sheet_name='Отчет по артикулам', index=False)
    df_seasonal_analysis.to_excel(writer, sheet_name='Отчет по сезонам')
    df_report.to_excel(writer, sheet_name='Итоговый отчет')
print(f'Сохранили файл: {datetime.now()}')
