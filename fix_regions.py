import pandas as pd

from helpers import df_files

DICT_REGIONS = pd.read_csv(
    ".\\dict\\fix_regions.csv",
    index_col=["region_hh"],
    encoding="cp1251",
    delimiter=";",
).to_dict("index")


def fix_region(region):
    if region in DICT_REGIONS.keys():
        return DICT_REGIONS[region]["region_dict"]
    else:
        return region


vacancies_df = pd.read_csv("vacancies.csv")
vacancies_df["region_name"] = vacancies_df["region_name"].apply(fix_region)
