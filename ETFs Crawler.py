import numpy as np 
import pandas as pd 
from tqdm import tqdm 
import time, datetime as dt

# Selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support import expected_conditions as EC


def Address_Money_Text(text):
    """
    Transform text number to float number
    """
    try:
        text = text.replace('$',"")
    except:
        pass

    try:
        text = text.replace('%','')
    except:
        pass

    try:
        text = text.replace(',',"")
    except:
        pass 

    try:
        text = float(text)
    except:
        pass

    return text


def ETF_Screener_Crawler(driver,Type):
    """
    This web crawler extract echange trdaed fund data from ETFDB(), you can slect different type of data and output in dataframe datatype
    Parameter Involve: driver and Type
    Type: ETF Overview, ETF Return, ETF Flow, ETF ESG, ETF Volatility
    driver: Your selenium dirver, encourage to you selenium==4.0.0 above version.
    """
    # Crawler
    df_overview_fund = pd.DataFrame()
    driver.find_element(By.XPATH,'//*[@id="mid-menu-ul"]/li[3]/a').click()
    driver.find_element("link text","ETF Screener").click()

    # Tpye of Information
    Overview = driver.find_element(By.XPATH,'//*[@id="basic"]/div/div[2]/div/div[1]/ul/li[1]')
    Fund_Flow_Buttom = driver.find_element(By.XPATH,'//*[@id="basic"]/div/div[2]/div/div[1]/ul/li[3]')
    Volatility_Buttom = driver.find_element(By.XPATH,'/html/body/div[4]/div[5]/div[2]/div[2]/div[1]/div/div[2]/div/div[1]/ul/li[7]')
    ESG_Score_Buttom = driver.find_element(By.XPATH,'/html/body/div[4]/div[5]/div[2]/div[2]/div[1]/div/div[2]/div/div[1]/ul/li[5]')
    Return_Buttom = driver.find_element(By.XPATH,'//*[@id="basic"]/div/div[2]/div/div[1]/ul/li[2]')

    if Type == 'ETF Overview':
        Overview.click()
        page = 1
        pandas_page = page -1 

    if Type == 'ETF Return':
        Return_Buttom.click()
        page = 2
        pandas_page = page -1 

    if Type == 'ETF Flow':
        Fund_Flow_Buttom.click()
        page = 3
        pandas_page = page -1 

    if Type == 'ETF ESG':
        ESG_Score_Buttom.click()
        page = 5
        pandas_page = page -1 
    
    if Type == 'ETF Volatility':
        Volatility_Buttom.click()
        page = 7
        pandas_page = page -1 

    # Start Download Table Data
    while True:
        try:
            
            page_df = pd.read_html(driver.page_source)[pandas_page][:-1]
            df_overview_fund = pd.concat([df_overview_fund,page_df],axis=0)
            time.sleep(1)
            WebDriverWait(driver, 3).until(
                EC.presence_of_element_located((By.XPATH,'//*[@id="mobile_table_pills"]/div['+str(page)+']/div/div[2]/div/ul/li[8]/a'))
            )
            Next_Buttom = driver.find_element(By.XPATH,'//*[@id="mobile_table_pills"]/div['+str(page)+']/div/div[2]/div/ul/li[8]/a')
            Next_Buttom.click()
        except:
            page_df = pd.read_html(driver.page_source)[pandas_page][:-1]
            df_overview_fund = pd.concat([df_overview_fund,page_df],axis=0)
            break

    return df_overview_fund.drop_duplicates()


def clean_fund_overview(df_overview_fund):

    df_overview_fund['Total Assets ($MM)'] = df_overview_fund.apply(lambda x : Address_Money_Text(x['Total Assets ($MM)']),axis=1)
    df_overview_fund['YTD Price Change'] = df_overview_fund.apply(lambda x : Address_Money_Text(x['YTD Price Change']),axis=1)
    df_overview_fund = df_overview_fund.fillna(value=0)

    return df_overview_fund

def clean_fund_flow_df(df_fund_flow,df_overview_fund):

    # Clean data for "df overview fund" dataframe
    df_fund_flow['1 Week ($MM)'] = df_fund_flow.apply(lambda x : Address_Money_Text(x['1 Week ($MM)']),axis=1)
    df_fund_flow['4 Week ($MM)'] = df_fund_flow.apply(lambda x : Address_Money_Text(x['4 Week ($MM)']),axis=1)
    df_fund_flow['YTD ($MM)'] = df_fund_flow.apply(lambda x : Address_Money_Text(x['YTD ($MM)']),axis=1)
    df_fund_flow['1 Year ($MM)'] = df_fund_flow.apply(lambda x : Address_Money_Text(x['1 Year ($MM)']),axis=1)
    df_fund_flow['3 Year ($MM)'] = df_fund_flow.apply(lambda x : Address_Money_Text(x['3 Year ($MM)']),axis=1)
    df_fund_flow['5 Year ($MM)'] = df_fund_flow.apply(lambda x : Address_Money_Text(x['5 Year ($MM)']),axis=1)

    # Add Asset Class to fund flow in order to us to use groupby on Asset Class
    df_fund_flow = pd.merge(df_fund_flow,df_overview_fund[['Symbol','ETF Name','Asset Class New']],left_on=['Symbol','ETF Name'],right_on=['Symbol','ETF Name'],how='right').drop_duplicates()
    
    return df_fund_flow

def clean_fund_performance_df(df_fund_performance,df_overview_fund):

    # Clean data for "df overview fund" dataframe
    df_fund_performance['1 Week'] = df_fund_performance.apply(lambda x : Address_Money_Text(x['1 Week']),axis=1)
    df_fund_performance['1 Month'] = df_fund_performance.apply(lambda x : Address_Money_Text(x['1 Month']),axis=1)
    df_fund_performance['YTD'] = df_fund_performance.apply(lambda x : Address_Money_Text(x['YTD']),axis=1)
    df_fund_performance['1 Year'] = df_fund_performance.apply(lambda x : Address_Money_Text(x['1 Year']),axis=1)
    df_fund_performance['3 Year'] = df_fund_performance.apply(lambda x : Address_Money_Text(x['3 Year']),axis=1)
    df_fund_performance['5 Year'] = df_fund_performance.apply(lambda x : Address_Money_Text(x['5 Year']),axis=1)
    

    df_fund_performance= pd.merge(df_fund_performance,df_overview_fund[['Symbol','ETF Name','Asset Class New']],left_on=['Symbol','ETF Name'],right_on=['Symbol','ETF Name'],how='right').drop_duplicates()

    return df_fund_performance

def clean_fund_volatility_df(df_fund_volatility,df_overview_fund):

    # Clean data for "df overview fund" dataframe
    df_fund_volatility['Standard Deviation'] = df_fund_volatility.apply(lambda x : Address_Money_Text(x['Standard Deviation']),axis=1)
    df_fund_volatility['P/E Ratio'] = df_fund_volatility.apply(lambda x : Address_Money_Text(x['P/E Ratio']),axis=1)
    df_fund_volatility['Beta'] = df_fund_volatility.apply(lambda x : Address_Money_Text(x['Beta']),axis=1)
    df_fund_volatility['5-Day Volatility'] = df_fund_volatility.apply(lambda x : Address_Money_Text(x['5-Day Volatility']),axis=1)

    df_fund_volatility['20-Day Volatility'] = df_fund_volatility.apply(lambda x : Address_Money_Text(x['20-Day Volatility']),axis=1)
    df_fund_volatility['50-Day Volatility'] = df_fund_volatility.apply(lambda x : Address_Money_Text(x['50-Day Volatility']),axis=1)
    df_fund_volatility['200-Day Volatility'] = df_fund_volatility.apply(lambda x : Address_Money_Text(x['200-Day Volatility']),axis=1)
    

    df_fund_volatility = pd.merge(df_fund_volatility,df_overview_fund[['Symbol','ETF Name','Asset Class New']],left_on=['Symbol','ETF Name'],right_on=['Symbol','ETF Name'],how='right').drop_duplicates()

    return df_fund_volatility

def clean_esg_fund_df(df_esg_performance,df_overview_fund):

    df_esg_performance = df_esg_performance.sort_values(by='ESG Score',ascending=False).reset_index(drop=True)

    # Clean data for "df overview fund" dataframe
    df_esg_performance['ESG Score	'] = df_esg_performance.apply(lambda x : Address_Money_Text(x['ESG Score']),axis=1)
    df_esg_performance['ESG Score Peer Percentile (%)'] = df_esg_performance.apply(lambda x : Address_Money_Text(x['ESG Score Peer Percentile (%)']),axis=1)
    df_esg_performance['ESG Score Global Percentile (%)'] = df_esg_performance.apply(lambda x : Address_Money_Text(x['ESG Score Global Percentile (%)']),axis=1)
    df_esg_performance['Carbon Intensity (Tons of CO2e / $M Sales)'] = df_esg_performance.apply(lambda x : Address_Money_Text(x['SRI Exclusion Criteria (%)']),axis=1)
    df_esg_performance['SRI Exclusion Criteria (%)'] = df_esg_performance.apply(lambda x : Address_Money_Text(x['SRI Exclusion Criteria (%)']),axis=1)
    df_esg_performance['Sustainable Impact Solutions (%)'] = df_esg_performance.apply(lambda x : Address_Money_Text(x['Sustainable Impact Solutions (%)']),axis=1)

    df_esg_performance = pd.merge(df_esg_performance,df_overview_fund[['Symbol','ETF Name','Asset Class New']],left_on=['Symbol','ETF Name'],right_on=['Symbol','ETF Name'],how='right').drop_duplicates()

    return df_esg_performance[['Symbol', 'ETF Name', 'ESG Score','Asset Class New', 'ESG Score Peer Percentile (%)','ESG Score Global Percentile (%)','Carbon Intensity (Tons of CO2e / $M Sales)','SRI Exclusion Criteria (%)', 'Sustainable Impact Solutions (%)']]



def main(Username,Password):

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    url = 'https://etfdb.com/screener/#tab=overview&page=1'
    driver.get(url)

    driver.find_element(By.XPATH,'//*[@id="navbar-collapse"]/ul[2]/li[3]/a').click()
    Login = driver.find_element("link text","Login")
    Login.click()

    Username_Blank = driver.find_element(By.NAME,'amember_login')
    Password_Blank = driver.find_element(By.NAME,'amember_pass')

    Username_Blank.send_keys(Username)
    Password_Blank.send_keys(Password)
    Password_Blank.send_keys(Keys.RETURN)

    # fund overview dataframe
    df_overview_fund = ETF_Screener_Crawler(driver,Type='ETF Overview')
    df_overview_fund = clean_fund_overview(df_overview_fund)

    # fund flow dataframe
    df_fund_flow = ETF_Screener_Crawler(driver,Type='ETF Flow')
    df_fund_flow = clean_fund_flow_df(df_fund_flow,df_overview_fund)

    # fund performance datafrmae
    df_fund_performance = ETF_Screener_Crawler(driver,Type='ETF Return')
    df_fund_performance = clean_fund_performance_df(df_fund_performance,df_overview_fund)

    # fund volatility dataframe
    df_fund_volatility = ETF_Screener_Crawler(driver,Type='ETF Volatility')
    df_fund_volatility = clean_fund_volatility_df(df_fund_volatility,df_overview_fund)

    # fund esg dataframe
    df_esg_performance = ETF_Screener_Crawler(driver,Type='ETF ESG')
    df_esg_performance = clean_esg_fund_df(df_esg_performance,df_overview_fund)

    return df_overview_fund, df_fund_flow, df_fund_performance, df_esg_performance


if __name__ == "__main__":
    
    Username = '****'
    Password = '****'
    df_overview_fund, df_fund_flow, df_fund_performance, df_esg_performance = main(Username,Password)

    df_overview_fund.to_excel(r'./Data/df_fund_overview.xlsx')
    df_fund_flow.to_excel(r'./Data/df_fund_flow.xlsx')
    df_fund_performance.to_excel(r'./Data/df_fund_performance.xlsx')
    df_esg_performance.to_excel(r'./Data/df_esg_performance.xlsx')


