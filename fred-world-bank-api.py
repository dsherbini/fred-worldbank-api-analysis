# PPHA 30537: Python Programming for Public Policy
# Spring 2023
# HW7: APIs with Pandas Data Reader
# Author: Danya Sherbini

##################

# Question 1: Explore the data APIs available from Pandas DataReader.  Pick
# any two countries, and then 
#   a) Find two sets of data for each that cover these places for overlapping 
#      time periods.  There should be exactly four dataframes and four individual
#      downloads (e.g. do not use one line to download two datasets).
#      - At least one should be from the World Bank, and at least one should
#        not be from the World Bank.
#      - At least one should have a frequency that does not match the others,
#        e.g. annual, quarterly, monthly.

import pandas as pd
import datetime

import pandas_datareader.data as web 
from pandas_datareader import wb 

# US GDP from FRED
start_fred = datetime.date(year=2010, month=1,  day=1) 
end_fred = datetime.date(year=2019, month=12, day=31) 
series_gdp = 'GDP' 
source = 'fred'

df_us_gdp = web.DataReader(series_gdp, source, start_fred, end_fred)

# US CO2 emissions from world bank
indicator_co2 = 'EN.ATM.CO2E.KT'
country1 = 'US'
start_wb = 2010
end_wb = 2019

df_us_co2 = wb.download(indicator = indicator_co2, 
                 country = country1, 
                 start= start_wb, end= end_wb)


# China GDP/CO2 emissions from world bank
indicator_gdp = 'NY.GDP.MKTP.CD'
country2 = 'CHN'

df_ch_gdp = wb.download(indicator = indicator_gdp, 
                 country = country2, 
                 start= start_wb, end= end_wb)

df_ch_co2 = wb.download(indicator = indicator_co2, 
                 country = country2, 
                 start= start_wb, end= end_wb)

# removing scientific notation
pd.set_option('display.float_format', lambda x: '%.2f' % x)


#   b) Adjust the data so that all four are at the same frequency (you'll have
#      to look this up), then merge them together into one long (tidy) format
#      dataframe.

# making US GDP annual
df_us_gdp = df_us_gdp.resample('Y').mean().reset_index()

# data cleaning for merge
df_us_gdp['DATE'] = df_us_gdp['DATE'].dt.year
df_us_gdp['country'] = 'United States'

df_us_co2.reset_index(inplace=True)
df_ch_co2.reset_index(inplace=True)
df_ch_gdp.reset_index(inplace=True)

df_us_gdp['DATE'] = df_us_gdp['DATE'].astype(int)
df_us_co2['year'] = df_us_co2['year'].astype(int)
df_ch_gdp['year'] = df_ch_gdp['year'].astype(int)

# merge US data
df_us_long = df_us_co2.merge(df_us_gdp, left_on= ['country', 'year'], right_on=['country', 'DATE'], how = 'left')
df_us_long = df_us_long.drop(labels = 'DATE', axis = 1)
df_us_long.rename(columns = {'EN.ATM.CO2E.KT':'co2', 'GDP':'gdp'}, inplace = True)

# merge china data
df_ch_long = df_ch_co2.merge(df_ch_gdp, on= ['country', 'year'], how = 'left')
df_ch_long.rename(columns = {'EN.ATM.CO2E.KT':'co2', 'NY.GDP.MKTP.CD':'gdp'}, inplace = True)

# merge all data
df_full = pd.concat([df_us_long, df_ch_long])


#   c) Finally, go back and change your earlier code so that the
#      countries and dates are set in variables at the top of the file.  Your
#      final result for parts a and b should allow you to (hypothetically) 
#      modify these values easily so that your code would download the data
#      and merge for different countries and dates.
#      - You do not have to leave your code from any previous way you did it
#        in the file. If you did it this way from the start, congrats!
#      - You do not have to account for the validity of all the possible 
#        countries and dates, e.g. if you downloaded the US and Canada for 
#        1990-2000, you can ignore the fact that maybe this data for some
#        other two countries aren't available at these dates.



#   d) Clean up any column names and values so that the data is consistent
#      and clear, e.g. don't leave some columns named in all caps and others
#      in all lower-case, or a column of mixed strings and integers.  Write 
#      the dataframe you've created out to a file named q1.csv, and commit
#      it to your repo.

# for data frame cleaning, see part b above

import os

path = '/Users/danya/Documents/GitHub/personal github/homework-7-dsherbini'
os.chdir(path)

df_full.to_csv('/Users/danya/Documents/GitHub/personal github/homework-7-dsherbini/q1.csv') 
