

# this file of python is used to fix bugs and other format errors of the database


from contextlib import ExitStack
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
import os
import download
DATA_PATH = r'C:\Users\ThanhDat\Desktop\Vietnam market simulator\Worldquant Vietnam Data'


def listExistedStock():
    existed_stock = os.listdir(DATA_PATH + '/ohlc')
    existed_stock = [stock[:3] for stock in existed_stock]
    return existed_stock


# the function will replace all the 'avg' term exist with the 'vwap' notation
def fix_vwap_issue(stock):

    file_name = DATA_PATH + '/ohlc/' + stock + '.csv'
    temp_df = pd.read_csv(file_name)

    #temp_df.drop(columns= ['Unnamed: 0','Unnamed: 0.1'], inplace=True)
    temp_df.rename(columns={'avg': 'vwap'}, inplace=True)
    temp_df.set_index('date', inplace=True)
    temp_df.to_csv(file_name)


def fix_vwap_issue_all_stock():
    existed_stock = listExistedStock()
    for stock in existed_stock:
        print('Fix the vwap stock ' + stock)
        fix_vwap_issue(stock)


# update the stock that has not been listed in the universe
# copy from the vndirect API we only got the currently available stock not all the universet that have been existed

def get_unlisted_stock_in_ohlc():

    df = pd.read_csv('isins.csv')

    stock_exist_list = listExistedStock()
    stock_list_vsd = []
    for stock in df.Stock.unique().tolist():
        if len(stock) == 3:
            stock_list_vsd.append(stock)
    delisted_stock = []

    for stock in stock_list_vsd:

        if stock not in stock_exist_list:
            delisted_stock.append(stock)
    success = 0
    failures = 0
    success_list = []
    for stock in delisted_stock:
        try:
            print(stock)
            loader = download.requestCafefData(
                stock, 1, '2011-01-01', '2021-01-01')
            if len(loader.index) <= 2:
                failures += 1
                continue
            success_list.append(stock)
            success += 1
        except:
            failures += 1

    print(success, failures)
    print(success_list)

    return success_list


def update_listed_event_for_unlisted_stock():
    unlisted_stock_list = ['AC4', 'AFC', 'AGC', 'AGD', 'ALP', 'AQN', 'ASD', 'ATD', 'AVS', 'BAF', 'BAM', 'BAS', 'BDC', 'BDF',
                           'BDP', 'BGM', 'BHS', 'BHV', 'BIG', 'BM9', 'BTC', 'BTR', 'BXD', 'C71', 'CBC',
                           'CCH', 'CCR', 'CEC', 'CER', 'CFC', 'CGP', 'CIC', 'CKH', 'CLP', 'CLS', 'CNA', 'CNH',
                           'CPW', 'CSG', 'CTM', 'CTV', 'CVC', 'CVH', 'CXH', 'CZC', 'D26', 'DAN', 'DCC', 'DCD', 'DCI', 'DFS', 'DGL', 'DHI', 'DHL', 'DLC', 'DLV', 'DMN', 'DNF', 'DNR', 'DNS', 'DNY', 'DSS', 'DTN', 'DVD', 'DVH',
                           'DWC', 'EBA', 'FBT', 'FDT', 'FPC', 'FSC', 'GBS', 'GFC', 'GGS', 'GHA', 'GMH',
                           'GTC', 'HBB', 'HBE', 'HBI', 'HBW', 'HCS', 'HD3', 'HFS', 'HFT', 'HHA', 'HHL',
                           'HMR', 'HNM', 'HPC', 'HPL', 'HPR', 'HPS', 'HPU', 'HRG', 'HST', 'HTB', 'HTU',
                           'HVC', 'IDN', 'IFC', 'IKH', 'IMT', 'JSC', 'KBE', 'KBT', 'KDF', 'KGU', 'KLS', 'KSA',
                           'KSC', 'KSE', 'KSS', 'KTB', 'KTU', 'MAX', 'MCL', 'MCV', 'MDN', 'MDT', 'MHP', 'MHY',
                           'MIH', 'MJC', 'MKT', 'MLN', 'MMC', 'MNC', 'MSC', 'MTM', 'MVY', 'NBR', 'NBS', 'NHN',
                           'NHS', 'NHW', 'NIS', 'NKD', 'NLC',
                           'NMK', 'NNB', 'NPH', 'NPS', 'NSN', 'NSP', 'NTR', 'NVC', 'NVN', 'NXT',
                           'ODE', 'PBK', 'PFV', 'PHT', 'PKR', 'PPG', 'PTK', 'PTM', 'PVF', 'QBR', 'REM',
                           'RHC', 'RHN', 'RLC', 'RTH', 'RTS', 'S33', 'S64', 'S91', 'SCH', 'SDE', 'SDF',
                           'SDI', 'SDS', 'SEC', 'SEL', 'SFT', 'SHV', 'SKS', 'SLC', 'SME', 'SNG', 'SOV', 'SSS',
                           'STU', 'STV', 'SVS', 'SZG', 'TAS', 'TBN', 'THR', 'THV', 'TIC', 'TIN', 'TLC', 'TND',
                           'TNY', 'TRI', 'TSM', 'TTJ', 'TTR', 'TTV', 'TVU', 'VCH', 'VCV', 'VEE', 'VIA', 'VNN',
                           'VPK', 'VPL', 'VT1', 'VT8', 'VTF', 'VTZ', 'WTN', 'X18', 'YRC', 'YSC']

    for stock in unlisted_stock_list:
        print('Success fully process the stock ', stock)
        download.summarizeListedDelisted(stock)


if __name__ == '__main__':
    update_listed_event_for_unlisted_stock()
