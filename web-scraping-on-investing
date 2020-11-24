import pandas as pd
import numpy as np
import investpy
import requests
from bs4 import BeautifulSoup

url_index = 'https://www.investing.com/indices/spain-35-components'

def get_asset_name_and_url(results):
    # Create a list and iterate over all found nodes. 
    asset_name_list = []
    asset_url_list = []
    for result in results:
        # Get response text
        asset_name = result.text
        # Get url
        asset_url = result.find('a')['href']
        # Append value 
        asset_name_list.append(asset_name)
        asset_url_list.append(asset_url)
    return asset_name_list, asset_url_list

def get_asset_ticker(asset_full_url):
    url = asset_full_url
    # Set headers parametres
    headers = {'User-Agent' : "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36"}
    # Send get request
    response = requests.get(url, headers = headers)
    # Parse the html code received 
    soup = BeautifulSoup(response.text, 'html.parser')
    # Find specific node on html response
    results = soup.find('h1', attrs={'class':'float_lang_base_1 relativeAttr'}).text
    start = results.find('(')
    end = results.find(')')
    ticker = results[(start+1):end]
    return ticker

def get_asset_links(asset_url_list):
    #Create list to save full url for each asset
    asset_full_url_list = []
    #Create list to save investing ticker for each asset
    asset_ticker_list = []
    for asset_url in asset_url_list:
        # Build link for every asset
        asset_full_url = "https://www.investing.com" + str(asset_url) + "-historical-data"
        # Append full asset url to asset_full_url_list
        asset_full_url_list.append(asset_full_url)
        # Get asset ticker
        asset_ticker = get_asset_ticker(asset_full_url)
        # Append asset ticker to asset_ticker_list
        asset_ticker_list.append(asset_ticker)
    return asset_full_url_list, asset_ticker_list

def get_index_composition(url_index):
    url = url_index
    # Set headers parametres
    headers = {'User-Agent' : "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36"}
    # Send get request
    response = requests.get(url, headers = headers)
    # Parse the html code received 
    soup = BeautifulSoup(response.text, 'html.parser')
    # Find specific nodeS on html response
    results = soup.findAll('td', attrs={'class':'bold left noWrap elp plusIconTd'})
    # Get asset name and url for the index
    asset_name_list, asset_url_list = get_asset_name_and_url(results)
    # Get full url and investing ticker of every asset
    asset_full_url_list, asset_ticker_list = get_asset_links(asset_url_list)
    # Create pandas DataFrame with the three lists
    index_composition_df = pd.DataFrame([asset_name_list, asset_full_url_list, asset_ticker_list]).T
    return index_composition_df


index_composition_df = get_index_composition(url_index)
