import pandas as pd
import numpy as np
import investpy
import requests
from bs4 import BeautifulSoup
import logging
import boto3
from botocore.exceptions import ClientError

class investing_class:
    def __init__(self,  index_name, url, country):
        self.s3 = boto3.client('s3')                            # Save s3 class
        self.index_name = index_name                            # Index name
        self.url = url                                          # Index url
        self.country = country
        self.headers = {'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36'}
        self.bucket_name = 'investingdata'

    def get_asset_name_and_url(self, results):
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
    
    def get_asset_ticker(self, asset_full_url):
        url = asset_full_url
        # Send get request
        response = requests.get(url, headers = self.headers)
        # Parse the html code received 
        soup = BeautifulSoup(response.text, 'html.parser')
        # Find specific node on html response
        results = soup.find('h1', attrs={'class':'float_lang_base_1 relativeAttr'}).text
        start = results.find('(')
        end = results.find(')')
        ticker = results[(start+1):end]
        return ticker
        
    def get_asset_links(self, asset_url_list):
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
            asset_ticker = self.get_asset_ticker(asset_full_url)
            # Append asset ticker to asset_ticker_list
            asset_ticker_list.append(asset_ticker)
        return asset_full_url_list, asset_ticker_list

    def get_index_composition(self):
        # Send get request
        response = requests.get(self.url, headers = self.headers)
        # Parse the html code received 
        soup = BeautifulSoup(response.text, 'html.parser')
        # Find specific nodeS on html response
        results = soup.findAll('td', attrs={'class':'bold left noWrap elp plusIconTd'})
        # Get asset name and url for the index
        asset_name_list, asset_url_list = self.get_asset_name_and_url(results)
        # Get full url and investing ticker of every asset
        asset_full_url_list, asset_ticker_list = self.get_asset_links(asset_url_list)
        # Create pandas DataFrame with the three lists
        index_composition_df = pd.DataFrame([asset_name_list, asset_full_url_list, asset_ticker_list]).T
        # Set DataFrame columns names
        index_composition_df.columns = ['asset_name', 'asset_url', 'asset_ticker']
        return index_composition_df

    def save_to_pickle_upload_index_file(self, index_composition_df):
        """Upload a file to an S3 bucket
        :param file_name: File to upload
        :param bucket: Bucket to upload to
        :param object_name: S3 object name. If not specified then file_name is used
        :return: True if file was uploaded, else False
        """
        index_composition_df.to_pickle('/tmp/index_composition_pkl')
        # If S3 object_name was not specified, use file_name
        try:
            response = self.s3.upload_file('/tmp/index_composition_pkl',
                                           self.bucket_name, self.index_name + '/index_composition')
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def get_index_assets_upload(self, index_composition_df):
        # Get DF lenght
        lenght = len(index_composition_df)
        # Downlaod ohlc of every index asset
        for i in range(0, lenght, 1):
            asset_name = index_composition_df.iloc[i, 0]
            asset_ticker = index_composition_df.iloc[i, 2]
            try:
                asset_data_df = investpy.get_stock_recent_data(asset_ticker, self.country)
            except:
                print(f'We could not download asset', asset_name)
            asset_data_df.to_pickle('/tmp/' + asset_name)
            self.s3.upload_file('/tmp/' + asset_name, self.bucket_name, 
                                           self.index_name + '/' + asset_name)
            print(f' We have downloaded {asset_name} ohlc data')



'''

def create_bucket(bucket_name, region=None):
    
    :param bucket_name: Bucket to create
    :param region: String region to create bucket in, e.g., 'us-west-2'
    :return: True if bucket created, else False
    
    # Create bucket
    try:
        if region is None:
            s3_client = boto3.client('s3')
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client = boto3.client('s3', region_name=region)
            location = {'LocationConstraint': region}
            s3_client.create_bucket(Bucket=bucket_name,
                                    CreateBucketConfiguration=location)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def create_bucket_folder(bucket_name, folder_name):
    s3 = boto3.client('s3')
    directory_name = folder_name
    s3.put_object(Bucket=bucket_name, Key=(directory_name +'/'))

def get_existing_buckets(): 
    # Retrieve the list of existing buckets
    s3_client = boto3.client('s3')
    response = s3_client.list_buckets()
    # Output the bucket names
    print('Existing buckets:')
    for bucket in response['Buckets']:
        print(f'  {bucket["Name"]}')

'''


