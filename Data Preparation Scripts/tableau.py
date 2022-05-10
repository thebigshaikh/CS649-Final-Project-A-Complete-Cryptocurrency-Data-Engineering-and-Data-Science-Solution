import os
import pandas as pd
from datetime import datetime
import pathlib


def join_csv(tableau_path, main_df):
    for filename in os.listdir(path_of_the_directory):
        f = os.path.join(path_of_the_directory, filename)
        if os.path.isfile(f) and filename != ".DS_Store"\
                and filename != "Gemini_1INCHUSD_1h.csv":
            df1 = pd.read_csv(f, skiprows=2,header=None)
            df = pd.concat([main_df, df1])
            main_df = df

    path = str(tableau_path) + "/" + "all_coin_data" + ".csv"

    df.columns = ["unix", "date", "symbol", "open", "high", "low", "close", "volume-coin", "volume-usd"]
    df.to_csv(path, index=False, encoding='utf-8')
    return df


def create_daily_files():
    for filename in os.listdir(path_of_the_directory):
        f = os.path.join(path_of_the_directory, filename)
        if os.path.isfile(f) and filename != ".DS_Store":
            df = pd.read_csv(f, skiprows=1)
            path = str(output_path) + "/" + filename[7:len(filename) - 4] + "-" + ".csv"
            df.to_csv(path, index=False, encoding='utf-8')


if __name__ == "__main__":
    # dates = ["2022-05-03", "2022-05-02", "2022-05-01", "2022-04-30", "2022-04-29"]
    # dates = ["2022-05-03", "2022-05-02", "2022-05-01", "2022-04-30", "2022-04-29"]

    # 2022 - 05 - 03
    path_of_the_directory = '../Data'
    output_path = "../Processed_Data/Tableau/"
    tableau_path = "../Processed_Data/Tableau/Main/"
    file_name = ""
    path_all = output_path + "All"
    path_all = pathlib.Path(path_all)
    path_all.mkdir(parents=True, exist_ok=True)
    # create_daily_files()
    mdf = pd.read_csv("../Data/Gemini_1INCHUSD_1h.csv", skiprows=2,
                      header=None)
    # print(mdf)
    join_csv(tableau_path, mdf)
