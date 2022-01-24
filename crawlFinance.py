from datetime import datetime
from time import time
from typing import ItemsView
import pandas as pd
import numpy as np
import time
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common import by
from selenium.webdriver.firefox.options import Options
import matplotlib.pyplot as plt
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select


HEADERS = {'content-type': 'application/x-www-form-urlencoded', 'User-Agent': 'Mozilla'}
URL_CAFE = "http://s.cafef.vn/Lich-su-giao-dich-"
STORE_PATH = r'C:\Users\ThanhDat\Desktop\secrete stock\Data'
CHROME_DRIVER_PATH = r'C:\Users\ThanhDat\Desktop\worldquant 2021\chromedriver.exe'

def processFinanceStatement(financial_report_table, balance_sheet_table):
    financial_report = pd.read_html(financial_report_table.get_attribute('outerHTML'))[0]
    balance_sheet = pd.read_html(balance_sheet_table.get_attribute('outerHTML'))[0]
    
    financial_report_thead_element = financial_report_table.find_element(By.TAG_NAME, 'thead')
    balance_sheet_thead_element = balance_sheet_table.find_element(By.TAG_NAME, 'thead')

    thead_financial_report_soup = BeautifulSoup(financial_report_thead_element.get_attribute('outerHTML'),'html.parser') 
    thead_balance_sheet_soup = BeautifulSoup(balance_sheet_thead_element.get_attribute('outerHTML'),'html.parser') 


    #drop some redundant columns
    financial_report.columns = range(financial_report.shape[1])
    balance_sheet.columns = range(balance_sheet.shape[1])
    financial_report.drop([1,2], axis = 1, inplace=True)
    balance_sheet.drop([1,2], axis = 1,inplace =True)
    # end the drop part 

    financial_report_columns = ['Category']
    balance_sheet_columns = ['Category']
    for element in thead_financial_report_soup.find_all('b'):
        financial_report_columns.append(element.get_text())
    for element in thead_balance_sheet_soup.find_all('b'):
        balance_sheet_columns.append(element.get_text())
   

    financial_report.columns = financial_report_columns
    balance_sheet.columns = balance_sheet_columns
    
    return financial_report, balance_sheet

def crawlFinancialStatement(symbol, option = 'quarterly'):
    
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(CHROME_DRIVER_PATH,chrome_options=chrome_options)
    driver.get('https://finance.vietstock.vn/' + symbol + '/financials.htm?languageid=2')
    time.sleep(5)

    driver.maximize_window()

    #update the unit value of the financial statement

    dropdown = driver.find_element_by_xpath('//*[@id="finance-content"]/div/div/div[3]/div/div[2]/select')
    dropdown_element = Select(dropdown)
    dropdown_element.select_by_value('1000')


    select_value = 0
    if option == 'yearly':
        select_value = 1
    else:
        select_value = 2
    dropdown_period = driver.find_element(By.XPATH, '//*[@id="finance-content"]/div/div/div[3]/div/div[1]/select')
    dropdown_period_element = Select(dropdown_period)
    dropdown_period_element.select_by_value(str(select_value))

    watch_button =  driver.find_element_by_xpath('//*[@id="finance-content"]/div/div/div[3]/div/button')
    watch_button.click()
    time.sleep(1.5) # sleep some seconds for the webpage load the new content
    # crawl the table summary balance sheet and financial report
    finance_report_origin = None
    balance_sheet_origin = None

    first_time = False
    while True:
        try: 
            left_button = driver.find_element_by_xpath('//*[@id="finance-content"]/div/div/div[3]/div/div[3]/div[2]')
            val_attribute  = left_button.get_attribute('class')
        except:
            print('there is an error here')
            financial_report_table = driver.find_element_by_xpath('//*[@id="finance-content"]/div/div/div[4]/div[1]/table')
            balance_sheet_table = driver.find_element_by_xpath('//*[@id="finance-content"]/div/div/div[4]/div[2]/table')
            finance_report , balance_sheet  = processFinanceStatement(financial_report_table, balance_sheet_table)

            if first_time == False:
                finance_report_origin = finance_report
                balance_sheet_origin =  balance_sheet
                first_time = True

            else:
                for name in finance_report.columns.values:
                    finance_report_origin[name] = finance_report[name].copy()

                for name in balance_sheet.columns.values:
                    balance_sheet_origin[name] = balance_sheet[name].copy()

            break
        
        financial_report_table = driver.find_element_by_xpath('//*[@id="finance-content"]/div/div/div[4]/div[1]/table')
        balance_sheet_table = driver.find_element_by_xpath('//*[@id="finance-content"]/div/div/div[4]/div[2]/table')
        finance_report , balance_sheet  = processFinanceStatement(financial_report_table, balance_sheet_table)

        if first_time == False:
            finance_report_origin = finance_report
            balance_sheet_origin =  balance_sheet
            first_time = True

        else:
            for name in finance_report.columns.values:
                finance_report_origin[name] = finance_report[name].copy()

            for name in balance_sheet.columns.values:
                balance_sheet_origin[name] = balance_sheet[name].copy()
        if val_attribute.find('disabled') != -1:
            break
        left_button.click()
        time.sleep(3)

    balance_sheet_origin.set_index('Category', inplace=True)
    finance_report_origin.set_index('Category', inplace=True)

    # sort the columns  in an order

    if option == 'yearly':
        balance_sheet_columns = balance_sheet_origin.columns.tolist()
        finance_report_columns = finance_report_origin.columns.tolist()

        reverse_balance_sheet = [item[-4:] for item in balance_sheet_columns]
        reverse_finance_report = [item[-4:] for item in finance_report_columns]

        dict_mapping_balance_sheet = {}
        dict_mapping_finance_report = {}

        for i, item in enumerate(reverse_finance_report):
            dict_mapping_finance_report[item] = finance_report_columns[i]
        for i, item in enumerate(reverse_balance_sheet):
            dict_mapping_balance_sheet[item] = balance_sheet_columns[i]


        reverse_finance_report.sort()
        reverse_balance_sheet.sort()

        sorted_array_balance_sheet = []
        for item in reverse_balance_sheet:
            sorted_array_balance_sheet.append(dict_mapping_balance_sheet[item])
        sorted_array_finance_report = []
        for item in reverse_finance_report:
            sorted_array_finance_report.append(dict_mapping_finance_report[item])

        balance_sheet_origin = balance_sheet_origin[sorted_array_balance_sheet]
        finance_report_origin = finance_report_origin[sorted_array_finance_report]

    else:
        balance_sheet_columns = balance_sheet_origin.columns.tolist()
        finance_report_columns = finance_report_origin.columns.tolist()

        reverse_balance_sheet = [item[-4:] + item[-6]for item in balance_sheet_columns]
        reverse_finance_report = [item[-4:] + item[-6]for item in finance_report_columns]

        dict_mapping_balance_sheet = {}
        dict_mapping_finance_report = {}

        for i, item in enumerate(reverse_finance_report):
            dict_mapping_finance_report[item] = finance_report_columns[i]
        for i, item in enumerate(reverse_balance_sheet):
            dict_mapping_balance_sheet[item] = balance_sheet_columns[i]


        reverse_finance_report.sort()
        reverse_balance_sheet.sort()

        sorted_array_balance_sheet = []
        for item in reverse_balance_sheet:
            sorted_array_balance_sheet.append(dict_mapping_balance_sheet[item])
        sorted_array_finance_report = []
        for item in reverse_finance_report:
            sorted_array_finance_report.append(dict_mapping_finance_report[item])

        balance_sheet_origin = balance_sheet_origin[sorted_array_balance_sheet]
        finance_report_origin = finance_report_origin[sorted_array_finance_report]
    print(balance_sheet_origin)
    print(finance_report_origin)
    balance_sheet_origin.to_csv(STORE_PATH + '/finance/balancesheet/'+symbol +'.csv')
    finance_report_origin.to_csv(STORE_PATH + '/finance/report/'+symbol +'.csv')

    
    driver.close()

if __name__ == '__main__':
    crawlFinancialStatement('HMR')