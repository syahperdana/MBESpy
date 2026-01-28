import commonpractice
import datetime as DT
##import matplotlib.pyplot as plt
import multiprocessing as mp
import os
import pandas as pd
from functools import partial

pd.options.mode.chained_assignment = None

def main():
    pool = mp.Pool(16)
    #_____Files definition________
    RawDataDir = 'D:\\QPS-Data\\Projects\\KAPUAS\\Export\\Raw'
    DenoiseDataDir = 'D:\\QPS-Data\\Projects\\KAPUAS\\Export\\Denoise'
    os.chdir(RawDataDir)
    Files = os.listdir(RawDataDir)

    #Start the loop through the file list:
    for File in Files:
        if File.endswith(".txt"):
            print('Start - '+File+' '+str(DT.datetime.now().strftime("%d/%m/%Y %H:%M:%S")))
            
            #Read the current file:
            df = pd.read_csv(File, header = None, sep = " ")
            df.columns = ["Date", "Time", "E", "N", "Z", "P", "Bm"] #Define the column names
            ##df.drop(["Nb", "Skip"], axis = 1, inplace = True) #Drop unwanted columns
            Pings = df.P.unique() #Define unique ping list
            df_Out = pd.DataFrame(columns=["Date", "Time", "E", "N", "Z", "Z_Reg"]) #Create Data Frame for final data
            df_Clean = pd.DataFrame(columns=["Date", "Time", "E", "N", "Z", "P", "Bm"]) #Create Data Frame for despiked data
            print('Start averaging - '+File+' '+str(DT.datetime.now().strftime("%d/%m/%Y %H:%M:%S")))
            
            #Start cleaning loop through all pings (might take some times):
            Pingpopulate = []
            for P in Pings:
                Pingpopulate.append(P)
            print('Cleaning progress: '+'{0:.2f}'.format((float(P)/len(Pings))*100)+'%')
            func = partial(commonpractice.cleaning, df)
            populateclean = pool.map(func, Pingpopulate)
            df_Clean = pd.concat(populateclean, ignore_index = True)
            pool.close
            pool.join
            ##plt.plot(Slice_one.Z) #Raw data
            ##plt.plot(Slice_one_Cor.Z) #Data to process
            ##plt.show()
            
            #Add cleaned data to the created data frame:
            ##df_Clean.to_csv(RawDataDir+"\Clean_"+File, index=False, float_format="%.3f")
            
            #Start correction loop through all pings (time can be used as an option):
            Pingpopulate2 = []
            for P in Pings:
                Pingpopulate2.append(P)
            print('Correction progress: '+'{0:.2f}'.format((float(P)/len(Pings))*100)+'%')
            #Select pings around current ping number, set (df_Clean.P<P+600)&(df_Clean.P>P-600)
            #Conditions to adjust window size, this data set will be used as reference to define correction for current ping:
            func2 = partial(commonpractice.correcting, df_Clean)
            populatecorr = pool.map(func2, Pingpopulate2)
            df_Out = pd.concat(populatecorr, ignore_index = True)
            pool.close
            pool.join
            
            #Add corrected data to the created data frame:        
            df_Out.to_csv(DenoiseDataDir+"\Denoise_"+File, index=False, float_format="%.3f")
            print('Finish - '+File+' '+str(DT.datetime.now().strftime("%d/%m/%Y %H:%M:%S")))

if __name__ == '__main__':
	main()
