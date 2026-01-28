import datetime as DT
import pandas as pd
from sklearn.linear_model import LinearRegression

def aggregate(a, b):
    #Note - use only one option for beam aggregation

    #Calculation of average depth values for the current beam (Option):
    #Av.append(df.Z[df.Bm == I].mean())

    #Calculation of median depth values for the current beam (Option);
    #Av.append(df.Z[df.Bm == I].median())
    #Aggregate_one = a.Z_Cor[a.Bm == b].median()

    #Calculation of median depth values for the current beam with Gate Filtering (Option):
    #Av.append(df.Z[(df.Z<-7.3) & (df.Z>-8.5)][df.Bm == I].median())
    #Aggregate_one = a.Z_Cor[(a.Z_Cor<21.0) & (a.Z_Cor>19.0)][a.Bm == b].median()

    #Calculation of average depth values for the current beam with Gate Filtering (Default):
    #Av.append(df.Z[(df.Z<-7.3) & (df.Z>-8.5)][df.Bm == b].mean())
    Aggregate_one = a.Z_Cor[(a.Z_Cor<21.0) & (a.Z_Cor>19.0)][a.Bm == b].mean()
    #End loop over the beams;
    return Aggregate_one

def cleaning(a, b):
    #All beams for current ping with ability to remove some specific beams
    #Use the df.Bm>0 and df.Bm<{Beam Witdh} conditions to specify beams to use:
    Slice_one = a[(a.P==b)&(a.Bm>0)&(a.Bm<251)].copy()
    #Create linear regression model based on the Slice_one data set:
    model = LinearRegression().fit(Slice_one.Bm.values.reshape((-1,1)), Slice_one.Z.values)
    #Predict linear regression Z values based on the model defined above:
    Slice_one["Z_1"] = model.predict(Slice_one.Bm.values.reshape((-1,1)))
    #Calculate absolute difference between predicted Z and raw Z values:
    Slice_one["dZ"] = abs(Slice_one.Z_1 - Slice_one.Z)
    #Create data set by keeping the Z values differ from the model less than 0.25 meter
    #Please use this condition (Slice_one.dZ<1) to set up cleaning tolerance
    Slice_one_Cor = Slice_one[(Slice_one.dZ<1)]
    #Drop unwanted columns:
    Slice_one_Cor.drop(["Z_1", "dZ"], axis = 1, inplace = True)
    return Slice_one_Cor

def correcting(a, b):
    #Select pings around current ping number, set (df_Clean.P<P+200)&(df_Clean.P>P-200)
    #Conditions to adjust window size, this data set will be used as reference to define correction for current ping:
    Slice_df = a[(a.P<b+1250)&(a.P>b-1250)&(a.Bm>5)&(a.Bm<246)]
    #Select data for current ping:
    Slice_one = a[(a.P==b)&(a.Bm>5)&(a.Bm<246)].copy()
    #Linear regression model from current ping data set:
    model = LinearRegression().fit(Slice_one.Bm.values.reshape((-1,1)), Slice_one.Z.values)
    #Linear regression model from all pings around current set (depends on the window size)
    model1 = LinearRegression().fit(Slice_df.Bm.values.reshape((-1,1)), Slice_df.Z.values)
    #Predict Z values for each beam from current ping model:
    Slice_one["Z_T"] = model.predict(Slice_one.Bm.values.reshape((-1,1)))
    #Predict Z values for each beam from stack of ping model:
    Slice_one["Z_TT"] = model1.predict(Slice_one.Bm.values.reshape((-1,1)))
    #Calculate correction as difference between the Big and Current model:
    Slice_one["Z_Cor"] = Slice_one.Z + Slice_one.Z_TT - Slice_one.Z_T
    Slice_one.drop(["Z_T", "Z_TT"], axis = 1, inplace = True)
    #Plotting the results:
    ##plt.plot(Slice_one.Z)
    ##plt.plot(Slice_one.Z_T)
    ##plt.plot(Slice_one.Z_TT)
    ##plt.show()
    return Slice_one