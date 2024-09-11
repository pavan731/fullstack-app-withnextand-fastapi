import pandas as pd

def truck_vehicle_population(data,pvpm):
    Vehicle_pop=data.pivot_table(index='AGE', columns='VDB APPLIcATION', aggfunc='size', fill_value=0)
    Vehicle_pop.fillna(0,inplace=True)
    Vehicle_pop = Vehicle_pop.reindex(columns=pvpm.columns)
    Vehicle_pop=Vehicle_pop.astype(int)
    return Vehicle_pop

def Segmentwise(Vehicle_pop,pvpm):
    Segmentwise_Potential=Vehicle_pop.mul(pvpm).fillna(0)
    Segmentwise_Potential=Segmentwise_Potential.div(10**6)
    Segmentwise_Potential=Segmentwise_Potential.reindex(columns=pvpm.columns)
    return Segmentwise_Potential


def utilization(data):
    Utilzation_per_month=data.groupby(['VDB APPLIcATION',"Month"])["Utilization %"].mean().reset_index()
    Utilzation_per_month=pd.pivot_table(Utilzation_per_month,values="Utilization %",index='VDB APPLIcATION',columns="Month")
    Utilzation_per_month.fillna(0,inplace=True)
    return Utilzation_per_month

def Gross_sale(retail):
    gross_sale=retail.groupby(["Month Name","Year"])["Gross Sale in MINR"].sum().reset_index()
    return gross_sale


def parts_penetration(retail,pvpm,data,running):
        
    Vehicle_pop=truck_vehicle_population(data,pvpm)
    Segmentwise_Potential=Segmentwise(Vehicle_pop,pvpm)
    gross_sale=Gross_sale(retail)
    Utilzation_per_month=utilization(data)
   
    c=data.groupby(['VDB APPLIcATION',"Month"])["Utilization %"].count().reset_index()
    c=pd.pivot_table(c,values="Utilization %",index='VDB APPLIcATION',columns="Month")
    c.fillna(0,inplace=True)
    multi_util_count=Utilzation_per_month.mul(c)


    df=pd.DataFrame(columns=['Application','Vehicle_Population','Running_hrs','Pot_Utilisation/Day','Utilisation%','ACT_Pot_Utilisation/Day','Ideal_Potential','Utilisation_Factor','Actual pototential calc'])
    df['Application'] = ['Mining OB/Mineral','Coal Transport','Road Construction','Irrigation','Quarry','On-Road LH','On Road']
    df.set_index("Application",inplace=True)
    df.fillna(0,inplace=True)
    
    #for all mining adding the vehicle count
    df.loc['Mining OB/Mineral','Vehicle_Population']=Vehicle_pop[[i for i in Vehicle_pop.columns if "Mining" in i]].sum().sum()
    
    #for all index 
    ind=list(set(Vehicle_pop.columns) &  set(df.index))
    df.loc[ind,'Vehicle_Population']=Vehicle_pop.loc[:,ind].sum()
    
    #running hrs
    df.loc[running.index,"Running_hrs"]=running.loc[:,"Running hours"]
    
    #Pot Utilization/day
    df["Pot_Utilisation/Day"]=df['Running_hrs']/12/30
    
    #Utilisation%
    df.loc['Mining OB/Mineral','Utilisation%']=multi_util_count.loc[[i for i in multi_util_count.index if "Mining" in i]].sum().sum()/c.loc[[i for i in c.index if "Mining" in i]].sum().sum()
    
    
    ind=list(set(multi_util_count.index) &  set(df.index))
   
    mul=multi_util_count.loc[ind]/c.loc[ind]
    df.loc[ind,'Utilisation%']=mul.values
    
   
    df['ACT_Pot_Utilisation/Day']=(df['Utilisation%']*24)/100
    df.loc['Mining OB/Mineral','Ideal_Potential']=Segmentwise_Potential[[i for i in Segmentwise_Potential.columns if "Mining" in i]].sum().sum()*len(Utilzation_per_month.columns)
    
 
    ind=list(set(Segmentwise_Potential.columns) &  set(df.index))
    df.loc[ind,'Ideal_Potential']=Segmentwise_Potential.loc[:,ind].sum()*len(Utilzation_per_month.columns)
    

    df["Utilisation_Factor"]=df['ACT_Pot_Utilisation/Day']/df["Pot_Utilisation/Day"]
    
    df['Actual pot calc']=df["Utilisation_Factor"]*df['Ideal_Potential']
    
    sum_gross=gross_sale["Gross Sale in MINR"].sum()
    
    df=df.reset_index()
    df=df.round(2)
    df.fillna(0,inplace=True)
   
    return df,round(df['Actual pot calc'].sum(),2),round(sum_gross,2),(sum_gross/df['Actual pot calc'].sum())*100

