"""
    The module will be used to download every information from a stock, including :
    1) OHLC from CAFEF
    2) Vndirect API event
    3) Finance information from the Vietstock
    4) Event List from the Vndirect API

"""
DATA_PATH = r'C:\Users\ThanhDat\Desktop\secrete stock\Data' 

import numpy as np
import pandas as pd
from datetime import datetime
import time
from bs4 import BeautifulSoup
from pandas.core.window.rolling import _Rolling_and_Expanding
from pandas.io import html
from pandas.io.formats.format import TableFormatter
import requests
from selenium import webdriver
from selenium.webdriver.common import by
from selenium.webdriver.firefox.options import Options
import matplotlib.pyplot as plt
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

HEADERS = {'content-type': 'application/x-www-form-urlencoded', 'User-Agent': 'Mozilla'}
URL_CAFE = "http://s.cafef.vn/Lich-su-giao-dich-"

CHROME_DRIVER_PATH = r'C:\Users\ThanhDat\Desktop\worldquant 2021\chromedriver.exe' # p/s : be careful with the version of the driver



# the 3 functions are used to normalize from the three exchanges

def normalizeHOSE(df):
    df['refprice'] = df['close'] - df['change_value']
    df['vwap'] = df['value_match'] / df['volume_match']
    df['open'] = df['open'] *1000
    df['close'] = df['close'] *1000
    df['high'] = df['high'] *1000
    df['low'] = df['low'] *1000
    df['date'] =  pd.to_datetime(df['date'], format='%d/%m/%Y')
    df['adjust'] = df['adjust'] *1000
    df['refprice'] = df['refprice'] *1000
    df['change_value'] = df['change_value'] *1000
    df['date'] =  df['date'].apply(normalizeDate)

    df.set_index('date', inplace = True)
    return df.iloc[::-1]
def normalizeHNX(df):
    df['vwap'] = df['value_match'] / df['volume_match']
    df['open'] = df['open'] *1000
    df['close'] = df['close'] *1000
    df['high'] = df['high'] *1000
    df['low'] = df['low'] *1000
    df['date'] =  pd.to_datetime(df['date'], format='%d/%m/%Y')
    df['adjust'] = df['adjust'] *1000
    df['refprice'] = df['refprice'] *1000
    df['change_value'] = df['change_value'] *1000
    df['date'] =  df['date'].apply(normalizeDate)

    df.set_index('date', inplace = True)
    return df.iloc[::-1]

def normalizeUPCOM(df):
    df['open'] = df['open']*1000

    df['close'] = df['close']*1000
    df['high'] = df['high']*1000
    df['low'] = df['low']*1000
    df['adjust'] = df['adjust']*1000
    df['refprice'] = df['refprice']*1000
    df['change_value'] = df['change_value']*1000
    df['vwap'] = df['vwap']*1000
    df['date'] =  pd.to_datetime(df['date'], format='%d/%m/%Y')
    df['date'] =  df['date'].apply(normalizeDate)
    df.set_index('date', inplace = True)
    return df.iloc[::-1]

def normalizeDate(datetimeObject):
    
    return datetime.strftime(datetimeObject, '%Y%m%d') # covert the string datetime object to an integer that should be used for handling the datetime 

def checkListed(aString):
    if aString.find('delisted') == 0:
        return -1
    elif aString.find('listed') == 0:
        return 1
    return 0
def getEventData(symbol):
    url = 'https://api-finfo.vndirect.com.vn/events?symbols=' + symbol
    HEADERS = {'content-type': 'application/x-www-form-urlencoded', 'User-Agent': 'Mozilla'}

    res = requests.get(url, headers = HEADERS, verify=False)
    data = res.json()['data']   
    df = pd.DataFrame(data)
    df1 = df.iloc[::-1]
    df1 = df1[df1.group == 'stockAlert']
    df1['checked'] = df1.type.apply(checkListed)
    df1 = df1[(df1.checked == 1) | (df1.checked == -1)]

    df1 = df1[['type','disclosuredDate','effectiveDate','content']].copy()
    df1.columns = ['type','disclosureDate','effectiveDate','content']
    df1.reset_index(inplace = True)
    df1.drop(columns=['index'], inplace = True)
    #df.to_csv('./Worldquant Vietnam Data/event/raw/' + symbol +'.csv')
    return df1
def getEventDataVersion2(symbol):
    url = 'https://finfo-api.vndirect.com.vn/v4/events?sort=disclosureDate&q=locale:VN~code:' + symbol + '&size=9999'
    HEADERS = {'content-type': 'application/x-www-form-urlencoded', 'User-Agent': 'Mozilla'}

    res = requests.get(url, headers = HEADERS, verify=False)
    data = res.json()['data']   
    df2 = pd.DataFrame(data)
    df2 = df2[df2.group == 'stockAlert']
    df2['checked'] = df2.type.apply(checkListed)
    df2 = df2[(df2.checked == 1) | (df2.checked == -1)]
    df2 = df2[['type','disclosureDate','effectiveDate', 'note']].copy()
    df2.columns = ['type','disclosureDate','effectiveDate','content']
    df2.reset_index(inplace = True)
    df2.drop(columns=['index'], inplace = True)
    return df2
def summarizeListedDelisted(symbol):

    df1 = None
    df2 = None
    try:
        df1 = getEventData(symbol)
    except Exception as e:
        print(e)
        df1 = -1
    try:
        df2 = getEventDataVersion2(symbol)
    except Exception as e:
        print(e)
        df2 = -1
    

    #df1 = df1[df1.group == 'stockAlert']
    #df2 = df2[df2.group == 'stockAlert']
    #df1['checked'] = df1.type.apply(checkListed)
    #df2['checked'] = df2.type.apply(checkListed)
    #df1 = df1[(df1.checked == 1) | (df1.checked == -1)]
    #df2 = df2[(df2.checked == 1) | (df2.checked == -1)]
#
    #df1 = df1[['type','disclosuredDate','effectiveDate','content']].copy()
    #df2 = df2[['type','disclosureDate','effectiveDate', 'note']].copy()
#
    #df1.columns = ['type','disclosureDate','effectiveDate','content']
    #df2.columns = ['type','disclosureDate','effectiveDate','content']
#
    ##df1.set_index('disclosureDate', inplace = True)
    ##df2.set_index('disclosureDate', inplace = True)
    #df1.reset_index(inplace = True)
    #df1.drop(columns=['index'], inplace = True)
    #df2.reset_index(inplace = True)
    #df2.drop(columns=['index'], inplace = True)
    exist_effectiveDate = []
    count = 0
    ##print(df1)
    ##print(df2)
    summarize_df = pd.DataFrame(columns=['type','disclosureDate','effectiveDate','content'])
    
    #summarize_df.set_index('disclosureDate', inplace=True)
    if isinstance(df1, pd.DataFrame):
        for ind in df1.index.values:
            
            if  (df1.effectiveDate.loc[ind] not in exist_effectiveDate):
                summarize_df.loc[count] =  df1.loc[ind].copy()
                exist_effectiveDate.append(df1.effectiveDate.loc[ind])
                count += 1
    if isinstance(df2, pd.DataFrame):
        for ind in df2.index.values:
        
            if  (df2.effectiveDate.loc[ind] not in exist_effectiveDate):
                summarize_df.loc[count] =  df2.loc[ind].copy()
                exist_effectiveDate.append(df2.effectiveDate.loc[ind])
                count += 1
    summarize_df.set_index('disclosureDate', inplace=True)
    summarize_df.sort_index(inplace=True, ascending=True)
    summarize_df.to_csv(DATA_PATH + '/event/listed_event/' + symbol +'.csv')
    print(summarize_df)
# extract the information from the format of the string 
# the next two functions are used for extracting info from CAFEF

def requestCafefData(symbol , page, start , end):
    form_data = {'ctl00$ContentPlaceHolder1$scriptmanager':'ctl00$ContentPlaceHolder1$ctl03$panelAjax|ctl00$ContentPlaceHolder1$ctl03$pager2',
                        'ctl00$ContentPlaceHolder1$ctl03$txtKeyword':symbol,
                        'ctl00$ContentPlaceHolder1$ctl03$dpkTradeDate1$txtDatePicker':start,
                        'ctl00$ContentPlaceHolder1$ctl03$dpkTradeDate2$txtDatePicker':end,
                        '__EVENTTARGET':'ctl00$ContentPlaceHolder1$ctl03$pager2',
                        '__EVENTARGUMENT':page,
                        '__ASYNCPOST':'true'}
    url = URL_CAFE+ symbol+"-1.chn"
    r = requests.post(url, data = form_data, headers = HEADERS, verify=False)

    #print(r.content)

    soup = BeautifulSoup(r.content, 'html.parser')
    # print(soup)
    table = soup.find('table')
    
    # print(table)
    values = pd.read_html(str(table))[0]

    return values
def extractMoneyChange(valueString):
    
    value_change, percent_change, redudant = valueString.split(' ')

    return float(value_change) # return the money change between days

def extractPercentChange(valueString):
    value_change, percent_change, redudant= valueString.split(' ')

    percent_change = percent_change[1:]

    return float(percent_change)

class StockOHLCUpdater():
    def __init__(self, stock, start, end):
        self.symbol = stock
        self.start = start
class StockLoader():
    def __init__(self, stock, floor, start, end):
        self.symbol = stock # init parameters used for requesting the API
        self.start = start
        self.end = end
        self.floor = floor
    def download(self):
        
        df = pd.DataFrame()
        df.index.name = 'date'
        current_page = 1
        first_time = 1
        for i in  range(1,300):
            
            print('Start download the page',current_page , 'in the cycle',i)

            current_df = None
            page_exist = True
            try:
                start_time =  datetime.now()
                current_df, page_exist = self.downloadPage(i)
                end_time = datetime.now()
                
                print('The request process takes ', end_time - start_time)
            except Exception as e:
                print(e) # print the error why the api is blocked
                print('the request has been blocked for a while')
            time.sleep(1)
            if not page_exist:
                break
            
            if type(current_df) != None:
                if first_time == 1:
                    df = current_df
                    first_time = 0
                    current_page += 1

                    continue
                df = df.append(current_df)
                current_page += 1

                
                
        list_columns = df.columns.tolist()
        df[list_columns] = df[list_columns].astype('float')
        df.reset_index(inplace=True)
        df = normalizeByExchange(self.symbol, df) # after write the dataframe to the csv file we will normalize immediately
        df.to_csv(DATA_PATH + '/ohlc/' + self.symbol + '.csv')

        return pd.read_csv(DATA_PATH + '/ohlc/' + self.symbol + '.csv')

    def downloadRandom(self, page):
        form_data = {'ctl00$ContentPlaceHolder1$scriptmanager':'ctl00$ContentPlaceHolder1$ctl03$panelAjax|ctl00$ContentPlaceHolder1$ctl03$pager2',
                        'ctl00$ContentPlaceHolder1$ctl03$txtKeyword':self.symbol,
                        'ctl00$ContentPlaceHolder1$ctl03$dpkTradeDate1$txtDatePicker':self.start,
                        'ctl00$ContentPlaceHolder1$ctl03$dpkTradeDate2$txtDatePicker':self.end,
                        '__EVENTTARGET':'ctl00$ContentPlaceHolder1$ctl03$pager2',
                        '__EVENTARGUMENT':page,
                        '__ASYNCPOST':'true'}
        url = URL_CAFE+ self.symbol+"-1.chn"
        r = requests.post(url, data = form_data, headers = HEADERS, verify=False)

        #print(r.content)

        soup = BeautifulSoup(r.content, 'html.parser')
        # print(soup)
        table = soup.find('table')
        
        # print(table)
        values = pd.read_html(str(table))[0]

        return values
    def downloadPage(self, page):
        if self.floor == 'HOSE':
            return self.downloadPageByAPIHOSE(page)
        elif self.floor == 'HNX':
            return self.downloadPageByAPIHNX(page)
        else:
            return self.downloadPageByAPIUPCOM(page)
    def downloadPageByAPIHOSE(self, page):
        form_data = {'ctl00$ContentPlaceHolder1$scriptmanager':'ctl00$ContentPlaceHolder1$ctl03$panelAjax|ctl00$ContentPlaceHolder1$ctl03$pager2',
                        'ctl00$ContentPlaceHolder1$ctl03$txtKeyword':self.symbol,
                        'ctl00$ContentPlaceHolder1$ctl03$dpkTradeDate1$txtDatePicker':self.start,
                        'ctl00$ContentPlaceHolder1$ctl03$dpkTradeDate2$txtDatePicker':self.end,
                        '__EVENTTARGET':'ctl00$ContentPlaceHolder1$ctl03$pager2',
                        '__EVENTARGUMENT':page,
                        '__ASYNCPOST':'true'}
        url = URL_CAFE+ self.symbol+"-1.chn"
        r = requests.post(url, data = form_data, headers = HEADERS, verify=False)

        #print(r.content)

        soup = BeautifulSoup(r.content, 'html.parser')
        # print(soup)
        table = soup.find('table')
        
        # print(table)
        values = pd.read_html(str(table))[0]
        

        values.drop(index=[0, 1],inplace=True)
        values[4] = values[3].copy()
        values = values[[0,1,2,3,4,5,6,7,8,9,10,11]]
        values[3] = values[3].apply(extractMoneyChange)
        values[4] = values[4].apply(extractPercentChange)

        #values.drop(index=[0, 1],inplace=True)
        values.columns =  ['date', 'adjust','close','change_value','change_percent','volume_match','value_match',
                            'volume_settlement','value_settlement','open','high','low']
        
        sort_dataset = values[['date','open','high','low', 'close','adjust','change_value','change_percent','volume_match','value_match',
                            'volume_settlement','value_settlement']]

        page_exist = True
        if len(values.index) == 0:
            page_exist = False # check whether it is the last page or not

        sort_dataset.set_index('date',inplace=True)
        return sort_dataset, page_exist
    def downloadPageByAPIHNX(self, page):
        form_data = {'ctl00$ContentPlaceHolder1$scriptmanager':'ctl00$ContentPlaceHolder1$ctl03$panelAjax|ctl00$ContentPlaceHolder1$ctl03$pager2',
                        'ctl00$ContentPlaceHolder1$ctl03$txtKeyword':self.symbol,
                        'ctl00$ContentPlaceHolder1$ctl03$dpkTradeDate1$txtDatePicker':self.start,
                        'ctl00$ContentPlaceHolder1$ctl03$dpkTradeDate2$txtDatePicker':self.end,
                        '__EVENTTARGET':'ctl00$ContentPlaceHolder1$ctl03$pager2',
                        '__EVENTARGUMENT':page,
                        '__ASYNCPOST':'true'}
        url = URL_CAFE+ self.symbol+"-1.chn"
        r = requests.post(url, data = form_data, headers = HEADERS, verify=False)

        #print(r.content)

        soup = BeautifulSoup(r.content, 'html.parser')
        # print(soup)
        table = soup.find('table')
        
        # print(table)
        values = pd.read_html(str(table))[0]
        

        values.drop(index=[0, 1],inplace=True)
        values[4] = values[3].copy()
        values = values[[0,1,2,3,4,5,6,7,8,9,10,11,12]]
        values[3] = values[3].apply(extractMoneyChange)
        values[4] = values[4].apply(extractPercentChange)

        #values.drop(index=[0, 1],inplace=True)
        values.columns =  ['date', 'adjust','close','change_value','change_percent','volume_match','value_match',
                            'volume_settlement','value_settlement','refprice','open','high','low']
        
        sort_dataset = values[['date','open','high','low', 'close','adjust','change_value','change_percent','volume_match','value_match',
                            'volume_settlement','value_settlement','refprice']]

        page_exist = True
        if len(values.index) == 0:
            page_exist = False # check whether it is the last page or not

        sort_dataset.set_index('date',inplace=True)
        return sort_dataset, page_exist

    def downloadPageByAPIUPCOM(self, page):
        form_data = {'ctl00$ContentPlaceHolder1$scriptmanager':'ctl00$ContentPlaceHolder1$ctl03$panelAjax|ctl00$ContentPlaceHolder1$ctl03$pager2',
                        'ctl00$ContentPlaceHolder1$ctl03$txtKeyword':self.symbol,
                        'ctl00$ContentPlaceHolder1$ctl03$dpkTradeDate1$txtDatePicker':self.start,
                        'ctl00$ContentPlaceHolder1$ctl03$dpkTradeDate2$txtDatePicker':self.end,
                        '__EVENTTARGET':'ctl00$ContentPlaceHolder1$ctl03$pager2',
                        '__EVENTARGUMENT':page,
                        '__ASYNCPOST':'true'}
        url = URL_CAFE+ self.symbol+"-1.chn"
        r = requests.post(url, data = form_data, headers = HEADERS, verify=False)

        #print(r.content)

        soup = BeautifulSoup(r.content, 'html.parser')
        # print(soup)
        table = soup.find('table')
        
        # print(table)
        values = pd.read_html(str(table))[0]
        

        values.drop(index=[0, 1],inplace=True)
        values[5] = values[4].copy()
        values = values[[0,1,2,3,4,5,6,7,8,9,10,11,12,13]]
        values[4] = values[4].apply(extractMoneyChange)
        values[5] = values[5].apply(extractPercentChange)

        #values.drop(index=[0, 1],inplace=True)
        values.columns =  ['date', 'adjust','close','vwap','change_value','change_percent','volume_match','value_match',
                            'volume_settlement','value_settlement','refprice','open','high','low']
        
        sort_dataset = values[['date','open','high','low','close','adjust','change_value','change_percent','volume_match',
                                'value_match','volume_settlement','value_settlement','refprice','vwap']]

        page_exist = True
        if len(values.index) == 0:
            page_exist = False # check whether it is the last page or not

        sort_dataset.set_index('date',inplace=True)
        return sort_dataset, page_exist

def getStockLastExchange(stock):
    try:
        df = pd.read_csv(DATA_PATH +'/event/listed_event/' + stock  +'.csv')

        string_value = df.iloc[-1].type

        if string_value.find('Hose') != -1:
            return 'HOSE'

        if string_value.find('Hnx') != -1:
            return 'HNX'

        if string_value.find('Upcom') != -1:
            return 'UPCOM'
    except Exception as e:
        print(e)
        print('The stock might not exist')
def normalizeByExchange(stock, df):


    if getStockLastExchange(stock) == 'HOSE':
        return normalizeHOSE(df)
    elif getStockLastExchange(stock) == 'HNX':
        return normalizeHNX(df)
    elif getStockLastExchange(stock) == 'UPCOM':
        return normalizeUPCOM(df)


# def crawl the industry and the subindustry and sector

def crawlIndustry(symbol):
    
    url = 'https://finfo-api.vndirect.com.vn/v4/industry_classification?q=codeList:' + symbol +'~industryLevel:'
    params = '&fields=industryCode,industryLevel,vietnameseName,englishName'
    original_df = pd.DataFrame()
    for level in range(1,5):
        res = requests.get(url + str(level) + params, headers=HEADERS)
        
        data = res.json()['data']
        df = pd.DataFrame(data)
        level_string = 'Level' + str(level)
        df.columns = ['englishName' + level_string, 'industryLevel' + str(level), 'vietnameseName' + level_string, 'industryCode' + level_string]

        for value in df.columns.values:
            original_df[value] = df[value].copy()
    original_df.index = [symbol]
    return original_df
def normalizeString(aString):
    return aString[aString.index(' ') + 1:]
def crawlVietstockIndustry(symbol):
    url = 'https://finance.vietstock.vn/'+ symbol +'-ngan-hang-tmcp-a-chau.htm?languageid=2'
    
    res = requests.get(url, headers = HEADERS)

    soup = BeautifulSoup(res.content, 'html.parser')
    href = soup.find_all('div', {'class':'m-b-xs sector-level'})
    
    industry_string = href[0].text
    industry_values = industry_string.split('/')
    industry_values = [normalizeString(value) for value in industry_values]
    
    industry_mapping = {'sector':industry_values[0],'industry':industry_values[1],'sub-industry':industry_values[2]}
    industry_mapping = pd.Series(industry_mapping, name=symbol)
    return industry_mapping
    
#***************Test and Debug ******************


if __name__ == '__main__':
    #loader = StockLoader('DKP','UPCOM','2011-01-01','2021-01-01')
#
    #df = loader.downloadPage(1)[0]
#
    #print(loader.download())
    #print(getStockLastExchange('DKP'))
#
#    success_list = ['AC4', 'AFC', 'AGC', 'AGD', 'ALP', 'AQN', 'ASD', 'ATD', 'AVS',
#                 'BAF', 'BAM', 'BAS', 'BCI', 'BDC', 'BDF', 'BDP', 'BGM', 'BHS', 
#                 'BHV', 'BIG', 'BM9', 'BTC', 'BTR', 'BXD', 'C71', 'CBC', 'CCH',
#                  'CCR', 'CEC', 'CER', 'CFC', 'CGP', 'CIC', 'CKH', 'CLP', 'CLS', 
#                  'CNA', 'CNH', 'CPW', 'CSG', 'CTM', 'CTV', 'CVC', 'CVH', 'CXH', 
#                  'CZC', 'D26', 'DAN', 'DBF', 'DCC', 'DCD', 'DCI', 'DFS', 'DGL', 
#                  'DHI', 'DHL', 'DLC', 'DLV', 'DMN', 'DNF', 'DNR', 'DNS', 'DNY', 
#                  'DSS', 'DTN', 'DVD', 'DVH', 'DWC', 'EBA', 'FBT', 'FDT', 'FPC',
#                   'FSC', 'GBS', 'GFC', 'GGS', 'GHA', 'GMH', 'GTC', 'HBB', 'HBE',
#                    'HBI', 'HBW', 'HCS', 'HD3', 'HFS', 'HFT', 'HHA', 'HHL', 'HMR',
#                     'HNM', 'HPL', 'HPR', 'HPS', 'HPU', 'HRG', 'HST', 'HTB', 'HTU',
#                      'HVC', 'I40', 'IDN', 'IFC', 'IKH', 'IMT', 'JSC', 'KBE', 'KBT',
#                       'KDF', 'KGU', 'KLS', 'KSA', 'KSC', 'KSE', 'KSS', 'KTB', 'KTU', 
#                       'MAX', 'MCL', 'MCV', 'MDN', 'MDT', 'MHP', 'MHY', 'MIH', 'MJC',
#                        'MKT', 'MLN', 'MMC', 'MNC', 'MSC', 'MTM', 'MVY', 'NBR', 'NBS', 
#                        'NHN', 'NHS', 'NHW', 'NIS', 'NKD', 'NLC', 'NMK', 'NNB', 'NPH', 
#                        'NPS', 'NSN', 'NSP', 'NTR', 'NVC', 'NVN', 'NXT', 'ODE', 'PBK', 
#                        'PFV', 'PHT', 'PKR', 'PPG', 'PTK', 'PTM', 'PVF', 'QBR', 'REM', 
#                        'RHC', 'RHN', 'RLC', 'RTH', 'RTS', 'S33', 'S64', 'S91', 'SBC', 
#                        'SCH', 'SDE', 'SDF', 'SDI', 'SDS', 'SEC', 'SEL', 'SFT', 'SHV', 
#                        'SKS', 'SLC', 'SME', 'SNG', 'SOV', 'SSS', 'STU', 'STV', 'SVS', 
#                        'SZG', 'TAS', 'TBN', 'TCS', 'THR', 'THV', 'TIC', 'TIN', 'TLC', 
#                        'TND', 'TNY', 'TRI', 'TSM', 'TTJ', 'TTR', 'TTV', 'TVU', 'VCH', 
#                        'VCV', 'VEE', 'VIA', 'VNN', 'VPK', 'VPL', 'VT1', 'VT8', 'VTF', 
#                        'VTZ', 'WTN', 'X18', 'YRC', 'YSC']
#    for stock in success_list[success_list.index('DMN'):]:
#        try:
#            print('Start download the stock ', stock)
#            loader = StockLoader(stock, getStockLastExchange(stock), '2000-01-01','2023-01-01')
#            loader.download()
#            print('Sucess crawl the stock', stock)
#
#        except Exception as e:
#            print(e)
    print(crawlVietstockIndustry('SSI'))