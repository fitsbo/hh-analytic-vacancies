import warnings
from datetime import datetime

import pandas as pd

# уберём предупреждения от Pandas
warnings.simplefilter(action="ignore", category=Warning)

# парсим дату
dateparse = lambda x: datetime.strptime(x, "%d.%m.%Y")

# загрузим исходные данные - средние ЗП по регионам, список регионов для исправления названий
na_values = ["NaN"]
df_salary = pd.read_csv(
    "./dict/region_avg_salary.csv",
    parse_dates=["Месяц"],
    date_parser=dateparse,
    na_values=na_values,
    encoding="cp1251",
    delimiter=";",
)
fix_regions = pd.read_csv(
    "./dict/fix_regions.csv", delimiter=";", encoding="cp1251", index_col=["region_hh"]
).to_dict("index")

# проверим какие колонки нуждаются в переименовании и переименуем их
for col in df_salary.columns:
    if col in fix_regions.keys():
        print(fix_regions[col]["region_dict"])
        df_salary.rename(columns={col: fix_regions[col]["region_dict"]}, inplace=True)

# проверим что некорректных колонок не осталось
for col in df_salary.columns:
    if col in fix_regions.keys():
        print(fix_regions[col]["region_dict"])

# сохраним датафрейм с переименованными колонками
df_salary.to_csv("./dict/region_avg_salary_renamed.csv", encoding="utf-8")

### Предсказание средних зарплат по регионам методом Month-Over-Month Growth

# датафрейм, куда будем записывать данные о средней ЗП по регионам
df_predicted = pd.DataFrame()

# список регионов из датафрейма средней ЗП
regions = [col for col in df_salary.columns if col not in ["Месяц"]]

# основной цикл обработки датафрейма с ЗП
for region in regions:
    df_region = df_salary[["Месяц", region]].loc[df_salary["Месяц"].dt.year >= 2022]
    df_region["region_name"] = region
    df_region.rename(columns={"Месяц": "month", region: "salary"}, inplace=True)

    # Прирост ЗП относительно предыдущего месяца
    df_region["growth"] = round(
        df_region["salary"].pct_change(periods=1, fill_method=None), 4
    )

    # Преобразуем колонку Месяц в формат даты
    df_region["month"] = pd.to_datetime(df_region["month"])
    # Устанавливаем колонку Месяц как индекс датафрейма
    df_region.set_index("month", inplace=True)
    # Сортируем индекс датафрейма, на случай если он не был отсортирован
    df_region.sort_index(inplace=True)

    # Итерируемся по строкам датафрейма
    for i in range(len(df_region)):
        # Текуший месяц и год
        current_month = df_region.index[i].month
        current_year = df_region.index[i].year

        previous_month = df_region.index[i].month - 1
        previous_year = df_region.index[i].year - 1

        # Пропускаем строки, где зарплата уже известна
        if pd.notnull(df_region["salary"].iloc[i]):
            continue

        # Находим индекс предыдущего месяца
        previous_month_idx = i - 1
        if previous_month_idx <= 0:
            # Если это январь, у нас нет предыдущих данных в текущем году
            continue

        # Зарплата предыдущего месяца
        previous_month_salary = df_region["salary"].iloc[previous_month_idx]

        # Прирост за этот месяц в прошлом году
        growth_last_year = df_region["growth"][
            (df_region.index.month == current_month)
            & (df_region.index.year == previous_year)
        ]
        if not growth_last_year.empty:
            predicted_salary = round(
                previous_month_salary
                + (previous_month_salary * growth_last_year.values[0]),
                1,
            )
            df_region["salary"].iloc[i] = predicted_salary

    df_predicted = pd.concat([df_predicted, df_region])
    df_predicted.drop("growth", axis=1, inplace=True)

# сохраним датафрейм с переименованными колонками
df_predicted.to_csv("./dict/region_avg_salary_renamed_predicted.csv", encoding="utf-8")
