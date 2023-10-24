import numpy as np
import pandas as pd

# season_df1 = pd.read_excel('sales_item_summary.xlsx')


def get_data_grouped_by_season(season_df):
    season_df = season_df[['Продажи, шт', 'Возвраты, шт', 'Итоговая себестоимость', 'Итоговая выручка', 'Штраф, руб',
                           'Логистика, руб', 'Итоговый размер кВВ', 'Коллекция']]
    data_grouped_by_season = season_df.groupby('Коллекция').sum()
    data_grouped_by_season['ЧП'] = data_grouped_by_season['Итоговая выручка'] - (
        data_grouped_by_season['Итоговая себестоимость'] + data_grouped_by_season['Штраф, руб'] +
        data_grouped_by_season['Логистика, руб'] + data_grouped_by_season['Итоговый размер кВВ']
    )
    # data_grouped_by_season['Рентабельность'] = np.where(
    #     data_grouped_by_season['Себестоимость всех продаж'] != 0,
    #     data_grouped_by_season['ЧП'] / data_grouped_by_season['Себестоимость всех продаж'], y=0)
    return data_grouped_by_season


# test = get_data_grouped_by_season(season_df=season_df1)
# print(test['Себестоимость всех продаж'])
