

# this file will not used for the simulator
# used only for collecting data purpose

import pandas as pd
import numpy as np
import requests
import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common import by
from selenium.webdriver.firefox.options import Options
import matplotlib.pyplot as plt
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
import info
import os
from bs4 import BeautifulSoup



CHROME_DRIVER_PATH = r'C:\Users\ThanhDat\Desktop\worldquant 2021\chromedriver.exe'

def check(first_df, second_df):
    return first_df.iloc[0].Stock != second_df.iloc[0].Stock
url = 'https://vsd.vn/en/tra-cuu-thong-ke/TK_MACK_CHUYENSAN?tab=3'
driver = webdriver.Chrome(CHROME_DRIVER_PATH)
driver.get(url)
time.sleep(5)
driver.maximize_window()
current_df = None
last_df = None
first_time = 0
page = 0
while page <= 5000:
    try:
        table_element = driver.find_element(By.XPATH,'//*[@id="tblListMaISIN"]')
        html_source = table_element.get_attribute('outerHTML')
        table_isin_df = pd.read_html(table_element.get_attribute('outerHTML'))[0]
        table_isin_df.columns = ['Position','ISIN','Stock','Name']

        isin_links = []
        stock_links = []
        soup = BeautifulSoup(html_source, 'html.parser')

        row = 0
        for trow in soup.findAll('tr'):
            list_data_cell = trow.findAll('td')
            links_row = []
            for td in list_data_cell:
                try:
                    link = td.find('a')['href']
                    links_row.append(link)
                except:
                    pass
            if len(links_row) <  2:
                continue
            first_link, second_link = links_row

            isin_links.append(first_link)
            stock_links.append(second_link)

            row += 1
        
        table_isin_df['ISIN_links'] = pd.Series(isin_links)
        table_isin_df['Stock_links'] = pd.Series(stock_links)
        table_isin_df.set_index('Position', inplace=True)
    except Exception as e:
        print(e)
        print(page)
        continue
            

    if first_time == 0:
        first_time = 1
        last_page_df = table_isin_df
        current_df = table_isin_df
    else:
        if check(last_page_df, table_isin_df):
            current_df = current_df.append(table_isin_df)
            last_page_df = table_isin_df
    
    print(current_df)
    next_button = driver.find_element(By.XPATH, '//*[@id="d_number_of_page"]/button[6]')
    next_button.click()
    time.sleep(2)