import pandas as pd
import test
import report
import seasonal_analysis
from datetime import datetime


start_time = datetime.now()
print(start_time)
# df = pd.read_excel("C:/Users/vyacheslav/Desktop/Работа/детализированный финансовый отчет 16-22 октября.xlsx")
df_cost_of_goods = pd.read_excel("C:/Users/vyacheslav/Desktop/2023.10.31 Отчет по скидкам_021123.xlsx",
                                 skiprows=1)
# df1 = pd.read_excel(r"C:\Users\vyacheslav\Desktop\48376703.xlsx")
# df2 = pd.read_excel(r"C:\Users\vyacheslav\Desktop\48376702.xlsx")

df = pd.read_excel(r"C:\Users\vyacheslav\PycharmProjects\salesControl\Внутредневные финансовые отчеты 28.08 - 26.11.xlsx")

df_product_sales = get_article_report.get_article_report(df=df, df_cost_of_goods=df_cost_of_goods)
print(f'Сформировали отчет по артикулам: {datetime.now()}')
df_report = report.get_report(item_report_df=df_product_sales, initial_df=df)
print(f'Сформировали итоговый отчет: {datetime.now()}')
df_seasonal_analysis = seasonal_analysis.get_data_grouped_by_season(season_df=df_product_sales)
print(f'Сформировали отчет по сезонам: {datetime.now()}')

with pd.ExcelWriter('WB Финансовый отчет 28.8 - 26.11 ноября.xlsx', engine='xlsxwriter') as writer:
    df_product_sales.to_excel(writer, sheet_name='Отчет по артикулам', index=False)
    df_seasonal_analysis.to_excel(writer, sheet_name='Отчет по сезонам')
    df_report.to_excel(writer, sheet_name='Итоговый отчет')
print(f'Сохранили файл: {datetime.now()}')
