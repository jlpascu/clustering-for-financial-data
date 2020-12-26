import pandas as pd
import clustering_support 

def download_data(event = None, context = None):
    
    url_index = 'https://www.investing.com/indices/nq-100-components'
    index_name = 'nasdaq'
    country = 'United States'
    
    # Create investing_class
    invest_class = clustering_support.investing_class(index_name, url_index, country)
    
    # Get index Composition
    index_composition_df = invest_class.get_index_composition()
    
    # Save DataFrame into pickle format and upload file to S3
    invest_class.save_to_pickle_upload_index_file(index_composition_df)
    
    # Get asset prices and upload
    invest_class.get_index_assets_upload(index_composition_df)

download_data()