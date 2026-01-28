import commonpractice
import datetime as DT
import matplotlib.pyplot as plt
import multiprocessing as mp
import os
import pandas as pd
from functools import partial

def main():
    pool = mp.Pool(16)
    #_____Files definition________
    RawDataDir = 'D:\\QPS-Data\\Projects\\BENOA\\Export\\Day1\\Denoise'
    os.chdir(RawDataDir)
    Files = os.listdir(RawDataDir)
    #Directory to save profiles:
    ProfileDir = 'D:\\QPS-Data\\Projects\\BENOA\\Export\\Day1\\Profile'

    print('#Phase 1 - Create profile from rawdata')

    #Start loop through the file list:
    for File in Files:
        if File.endswith(".txt"):
            print('Start - '+File+str(DT.datetime.now().strftime("%d/%m/%Y %H:%M:%S")))
            #Read points file into Pandas Data Frame 'Df':
            df = pd.read_csv(File, header = 0, sep = ",", low_memory = False)
            #Define column names:
            df.columns = ["Date", "Time", "E", "N", "Z", "P", "Bm", "Z_Cor"]

            #???

            #List of correction values for each beam:
            Av = []
            #Remove rows with empty data in df:
            df = df.dropna()
            #Get beam number from df:
            Beams = df.Bm.unique()
            #Start loop over the beams
            Beampopulate = []
            for I in Beams:
                Beampopulate.append(I)
            func = partial(commonpractice.aggregate, df)
            populateaggregate = pool.map(func, Beampopulate)
            Av = populateaggregate
            #print(populateaggregate)
            pool.close
            pool.join

            #???
            #print(populateaggregate)
            #print(Av)
            #print(Beams)
            #print(len(Av))
            #print(len(Beams))
            #Define new data frame for depth values aggregated and averaged by beams:
            DF = pd.DataFrame()

            #Create column 'Av' with depth values:
            DF["Av"] = Av
            #Create column 'Av' with beam numbers:
            DF["Beam"] = Beams
            #Calculate column of corrections:
            DF["Cor"] = -(DF["Av"] - DF["Av"].mean())
            #Calculate corrected depths:
            DF["Av_Cor"] = DF["Av"] + DF["Cor"]
            #Sorting data by beam numbers:
            DF = DF.sort_values(by="Beam")

            #???

            #Plotting Data
            ##plt.plot(DF["Beam"], DF["Av_Cor"])
            ##plt.plot(DF["Beam"], DF["Av"])
            #Invert axis (optional):
            #plt.gca().invert_yaxis()
            ##Title = File
            ##plt.title(Title)
            ##plt.show()

            #Write processes profiles:
            DF.to_csv(ProfileDir+"\Profile_"+File, index=False, float_format="%.3f")
            print('Finish - '+File+str(DT.datetime.now().strftime("%d/%m/%Y %H:%M:%S")))

    print('#Phase 2 - Create correction from profile')

    os.chdir(ProfileDir)
    Files2 = os.listdir(ProfileDir)
    CorrectionDir = 'D:\\QPS-Data\\Projects\\BENOA\\Export\\Day1\\Correction'
    #Create list of profiles:
    DF_list = []

    #Start loop through the file list:
    for File2 in Files2:
        if File2.endswith(".txt"):
            print('Start - '+File2+str(DT.datetime.now().strftime("%d/%m/%Y %H:%M:%S")))
            #Read profile file into Pandas Data Frame:
            df2 = pd.read_csv(File2)
            #print(df2.Bm)
            #Add Data Frame to list:
            DF_list.append(df2)
            ##plt.plot(df2["Beam"], df2["Cor"], label=File2)
            #Loop over files completed, all profiles added to list of data frames

            #Plotting data of error profiles:
            ##plt.legend(bbox_to_anchor=(1.05, 1), loc=2)
            ##plt.gca().invert_yaxis()
            ##plt.show()

            #???

            #Start loop for aggregating list's data into one data frame
            i = 0
            df2 = DF_list[0]
            for item in DF_list:
                if i != 0:
                    df2 = df2.append(DF_list[i])
                i = i + 1

            #???

            #Get beam number from df2:
            Beams = df2.Beam.unique()
            #Create list for averaged values by beam:
            Av = []
            #Start loop for averaging data from different profiles by beam:
            for I in Beams:
                Av.append(df2.Av[df2.Beam == I].mean())
            #Loop completed

            #???

            #Create Data Frame for correction calculations:
            df_mean = pd.DataFrame()
            #Add column with averaged errors:
            df_mean["Av"] = Av
            #Add column with beam numbers:
            df_mean["Beam"] = Beams
            #Calculation of corrections:
            df_mean["Cor"] = -(df_mean["Av"] - df_mean["Av"].mean())
            #Calculation of corrected depths (use for check):
            df_mean["Av_Cor"] = df_mean["Av"] + df_mean["Cor"]
            #Sorting of data by beam numbers:
            df_mean = df_mean.sort_values(by="Beam")

            # #???

            # #Plotting data:
            # plt.plot(df_mean["Beam"], df_mean["Cor"])
            # #Plotting of corrections:
            # plt.legend()
            # plt.show()

            #Write profile with corrections into text file:
            df_mean.to_csv(CorrectionDir+"\Correction_"+File2, index=False, float_format="%.3f")
            print('Finish - '+File2+str(DT.datetime.now().strftime("%d/%m/%Y %H:%M:%S")))

            # #???

    print('#Phase 3 - Apply correction to rawdata')

    #os.chdir(RawDataDir)
    #Files2 = os.listdir(RawDataDir)
    FinalDir = 'D:\\QPS-Data\\Projects\\BENOA\\Export\\Day1\\Final'
    #Create list of profiles:
    #DF_list = []

    for File in Files:
        if File.endswith(".txt"):
            print('Start - '+File+str(DT.datetime.now().strftime("%d/%m/%Y %H:%M:%S")))
            #Read rawdata and correction file into Pandas Data Frame:
            df3 = pd.read_csv(RawDataDir+"\\"+File)
            Cor = pd.read_csv(CorrectionDir+"\Correction_Profile_"+File)
            #Define column names:
            df3.columns = ["Date", "Time", "E", "N", "Z", "P", "Beam", "Z_Cor"]
            #Merging Data Frame with the corrections on Beam Number:
            df3 = df3.merge(Cor[["Beam","Cor"]])
            #Calculating the corrected depth:
            df3["Corr_Depth"] = df3.Z + df3.Cor
            #Write corrected data into text file:
            df3[["Date", "Time", "E", "N", "Corr_Depth"]].to_csv(FinalDir+"\Final_"+File, index = False, float_format = "%.3f")
            print('Finish - '+File+str(DT.datetime.now().strftime("%d/%m/%Y %H:%M:%S")))

if __name__ == '__main__':
	main()