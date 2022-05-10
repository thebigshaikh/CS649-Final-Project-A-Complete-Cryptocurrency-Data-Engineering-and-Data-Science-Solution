import os
import pandas as pd
from datetime import datetime
import pathlib


def create_daily_files():
    for filename in os.listdir(path_of_the_directory):
        f = os.path.join(path_of_the_directory,filename)
        if os.path.isfile(f) and filename != ".DS_Store":
            df = pd.read_csv(f, skiprows=1)
            df['just_date'] = pd.to_datetime(df['date']).dt.date
            for d in dates:
                d = datetime.strptime(d, '%Y-%m-%d').date()
                df_filtered = df[df['just_date'] == d]

                path_name = output_path+filename[7:len(filename)-7]
                path = pathlib.Path(path_name)
                path.mkdir(parents=True, exist_ok=True)

                path_date = pathlib.Path(output_path +str(d))
                path_date.mkdir(parents=True, exist_ok=True)

                by_coin = str(path)+ "/" + filename[7:len(filename)-4]+"-"+str(d)+".csv"
                all = str(path_all) + "/" + filename[7:len(filename)-4]+"-"+str(d)+".csv"
                by_date = str(path_date) + "/" + filename[7:len(filename)-4]+"-"+str(d)+".csv"


                df_filtered.to_csv(by_coin ,index=False,  encoding='utf-8')
                df_filtered.to_csv(all, index=False,  encoding='utf-8')
                df_filtered.to_csv(by_date, index=False,  encoding='utf-8')



if __name__ == "__main__":
    dates = ["2022-05-03", "2022-05-02", "2022-05-01", "2022-04-30", "2022-04-29"]
    # dates = ["2022-05-03", "2022-05-02", "2022-05-01", "2022-04-30", "2022-04-29"]

    # 2022 - 05 - 03
    path_of_the_directory = '../Data/MinuteData'
    output_path = "../Processed_Data/MinuteData/"
    file_name = ""
    path_all = output_path + "All"
    path_all = pathlib.Path(path_all)
    path_all.mkdir(parents=True, exist_ok=True)
    create_daily_files()

