import pandas as pd 
import glob #finds files and directories with specific patterns  
import os 

clean_bronze(): 
  files = glob.glob("data/bronze/*.parquet") #search fot the data file in the bronze folder 
                                             #if the ingestion process runs more than once a day, we are able to find all the otput files 
  dfs = [pd.read_parquet(f) for f in files]  #each file read turns into a df -> dfs =[df1, df2, df3]
  df = pd.concat(dfs, ignore_index=True) #concatenates all dfs, unifying all of them in one table 

  #cleaning
  df.drop_duplicates(subset=["dt"], inplace=True) #removes duplicate lines considering dt, so we have only one line for each timestamp 
  df["dt"] = pd.todatetime(df["dt"] #in pandas, it's very important that timestamps are converted into the right types, alowing: 
                                    #groups by day, filters by period, correct order 
  df["weather"] = df["weather"].str.tile() #sets a writing pattern to the column, allowing better analysis and visualization

  #consolidation 
  os.makedirs("data/silver/", exist_ok=True)
  df.to_parquet("data/silver/weather_clean.parquet", index=False)

  print("Silver layer successfully generated.) 

if __name__ = "__main__":
  clean_bronze()
