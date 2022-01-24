import pandas as pd
import numpy as np
import download
import requests


DATA_PATH = r'C:\Users\ThanhDat\Desktop\Worldquant Vietnamese project\Worldquant Vietnam Data' 


# The first part is used to get the universe of the stock market
# get all the stock existed
def getAllExistedStock():
    current_available_stock = getCurrentAvailableStock('all')
    unlisted_stock = getDelistedStock()
    total_stock = []
    for stock in current_available_stock:
        if len(stock) == 3:
            total_stock.append(stock)
    
    for stock in unlisted_stock:
        if len(stock) == 3:
            total_stock.append(stock)

    return total_stock
def getCurrentAvailableStock(floor='HOSE'):
    url = 'https://api-finfo.vndirect.com.vn/v4/stocks?q=type:STOCK~status:LISTED&fields=floor,code&size=9999'
    HEADERS = {'content-type': 'application/x-www-form-urlencoded', 'User-Agent': 'Mozilla'}

    res = requests.get(url, headers = HEADERS, verify=False)
    data = res.json()['data']   
    list_stock = pd.DataFrame(data)
    
    if floor == 'all':
        list_stock = list_stock['code'].to_list()
        list_stock.sort()
        return list_stock
    list_stock = list_stock[list_stock.floor == floor]
    list_stock = list_stock['code'].to_list()
    list_stock.sort()
    
    return list_stock

def getDelistedStockVersionCode():
    df = pd.read_csv('isins.csv')
    
    stock_vsd = df.Stock.unique().tolist()
    stock_current_available = getCurrentAvailableStock('all')

    delisted_stock = []
    for stock in stock_vsd:
        if stock not in stock_current_available:
            delisted_stock.append(stock)
    
    stock_before_exchange = []
    stock_on_exchange = []

    for stock in delisted_stock:

        try:
            loader = download.StockLoader(stock, 'HOSE', '2000-01-01','2022-01-01')
            temp_df = loader.downloadRandom(1)
            
            if temp_df.shape[0] > 2:
                stock_on_exchange.append(stock)
            else:
                stock_before_exchange.append(stock)
        except Exception as e:
            print(e)
            stock_before_exchange.append(stock)

    print(stock_before_exchange)
    print(stock_on_exchange)

def getDelistedStock(): # version hard code
    delisted_stock = ['AC4', 'AFC', 'AGC', 'AGD', 'ALP', 'AQN', 'ASD', 'ASIAGF', 'ATD', 'AVS', 'BAM', 'BAS', 'BCI', 'BDC', 'BDF', 'BDP', 'BGM', 'BHS', 'BHV', 'BM9', 'BTC', 'BTR', 'BXD', 'C36', 'C71', 'CBC', 'CCH', 'CEC', 'CER', 'CFC', 'CFPT2016', 'CFPT2101', 'CFPT2102', 'CFPT2103', 'CFPT2104', 'CFPT2105', 'CGP', 'CHDB2101', 'CHDB2102', 'CHPG2020', 'CHPG2101', 'CHPG2102', 'CHPG2103', 'CHPG2104', 'CHPG2105', 'CHPG2106', 'CHPG2107', 'CHPG2108', 'CHPG2109', 'CHPG2110', 'CHPG2111', 'CIC', 'CKDH2002', 'CKDH2101', 'CKDH2102', 'CKDH2103', 'CKH', 'CLP', 'CLS', 'CMBB2010', 'CMBB2101', 'CMBB2103', 'CMSN2101', 'CMSN2102', 'CMSN2103', 'CMSN2104', 'CMSN2105', 'CMSN2106', 'CMWG2013', 'CMWG2016', 'CMWG2101', 'CMWG2102', 'CMWG2103', 'CMWG2104', 'CMWG2105', 'CMWG2106', 'CMWG2107', 'CNH', 'CNVL2101', 'CNVL2102', 'CPDR2101', 'CPDR2102', 'CPNJ2101', 'CPNJ2102', 'CPNJ2103', 'CPNJ2104', 'CPNJ2105', 'CPW', 'CREE2101', 'CSBT2101', 'CSG', 'CSTB2007', 'CSTB2010', 'CSTB2014', 'CSTB2101', 'CSTB2102', 'CSTB2103', 'CSTB2104', 'CSTB2105', 'CSTB2106', 'CTCB2012', 'CTCB2101', 'CTCB2102', 'CTCB2103', 'CTCB2104', 'CTCB2105', 'CTCH2003', 'CTCH2101', 'CTCH2102', 'CTCH2103', 'CTM', 'CTV', 'CVC', 'CVH', 'CVHM2008', 'CVHM2101', 'CVHM2102', 'CVHM2103', 'CVHM2104', 'CVHM2105', 'CVHM2106', 'CVHM2107', 'CVIC2005', 'CVIC2101', 'CVIC2102', 'CVIC2103', 'CVIC2104', 'CVIC2105', 'CVJC2006', 'CVJC2101', 'CVJC2102', 'CVNM2011', 'CVNM2101', 'CVNM2102', 'CVNM2103', 'CVNM2104', 'CVNM2105', 'CVNM2106', 'CVNM2107', 'CVNM2108', 'CVNM2109', 'CVPB2015', 'CVPB2101', 'CVPB2102', 'CVPB2103', 'CVPB2104', 'CVPB2105', 'CVRE2009', 'CVRE2011', 'CVRE2013', 'CVRE2101', 'CVRE2102', 'CVRE2103', 'CVRE2104', 'CVRE2105', 'CVRE2106', 'CXH', 'CZC', 'D26', 'DCC', 'DCD', 'DCI', 'DFS', 'DGL', 'DHI', 'DHL', 'DKP', 'DLC', 'DLV', 'DNF', 'DNR', 'DNS', 'DNY', 'DSS', 'DTN', 'DVD', 'DVH', 'E1VFVN30', 'EBA', 'FBT', 'FDT', 'FPC', 'FSC', 'FUCTVGF1', 'FUCTVGF2', 'FUCTVGF3', 'FUCVREIT', 'FUEIP100', 'FUEKIV30', 'FUEMAV30', 'FUESSV30', 'FUESSV50', 'FUESSVFL', 'FUEVFVND', 'FUEVN100', 'GBS', 'GFC', 'GGS', 'GHA', 'GTC', 'HBB', 'HBE', 'HBI', 'HBW', 'HCS', 'HD3', 'HFS', 'HFT', 'HHA', 'HHL', 'HIZ', 'HKC', 'HPC', 'HPL', 'HPR', 'HPS', 'HPU', 'HRG', 'HST', 'HTB', 'HTU', 'HVC', 'IDN', 'IFC', 'IKH', 'IMT', 'JSC', 'KBE', 'KBT', 'KDF', 'KGU', 'KLS', 'KSA', 'KSC', 'KSE', 'KSS', 'KTB', 'KTU', 'MAFPF1', 'MAX', 'MCL', 'MCV', 'MDN', 'MDT', 'MHP', 'MHY', 'MIH', 'MJC', 'MKT', 'MLN', 'MMC', 'MNC', 'MSC', 'MTM', 'MVY', 'NBR', 'NBS', 'NCP', 'NHN', 'NHS', 'NHW', 'NIS', 'NKD', 'NLC', 'NMK', 'NNB', 'NPH', 'NPS', 'NSN', 'NSP', 'NTR', 'NVC', 'NVN', 'PBK', 'PFV', 'PHT', 'PKR', 'PME', 'PPG', 'PRUBF1', 'PTK', 'PTM', 'PVF', 'QBR', 'REM', 'RHC', 'RHN', 'RLC', 'RTH', 'RTS', 'S33', 'S64', 'S91', 'SCH', 'SDE', 'SDF', 'SDI', 'SDS', 'SEC', 'SEL', 'SFT', 'SHV', 'SKS', 'SLC', 'SME', 'SNG', 'SOV', 'SPC', 'SSS', 'STU', 'STV', 'SUM', 'SVS', 'TAS', 'TBN', 'THR', 'THV', 'TIC', 'TLC', 'TND', 'TNY', 'TRI', 'TSM', 'TTJ', 'TTR', 'TTV', 'TVU', 'VCH', 'VCV', 'VEE', 'VFMVF1', 'VFMVF4', 'VFMVFA', 'VIA', 'VNN', 'VPK', 'VPL', 'VT1', 'VT8', 'VTF', 'WTN', 'X18', 'YRC', 'YSC']
    delisted_stock = [stock for stock in delisted_stock if len(stock) == 3]
    
    return delisted_stock

    # note that there are some stock will not be listed on the Cafef due to lack of the dataset OHLC
    # DBF, I40, SBC, TCS
    # despite the data is avaliable on the Vndirect API


# The second part get the industry

if __name__  == '__main__':


    print(len(getDelistedStock()))
    print(len(getAllExistedStock()))
    print('BHS' in getAllExistedStock())
