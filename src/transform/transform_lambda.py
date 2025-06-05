from src.transform.transform_utils import get_all_table_data_from_ingest_bucket

def lambda_handler(event, context):
    ingested_data = get_all_table_data_from_ingest_bucket()



    
    # Build the dataframes from ingested_data, a dict whose keys are table names
    # and whose values are lists of dicts with the data from that table


    # get the example tables from the ingestion s3 bucket and save in data folder

    # get the right columns from the right tables and put them together
    # split some columns into separate columns where required

    # have data in dataframes 
        # ids are unique - could have them as the dataframe indices
        # look into what dataframe format we should save them in
        # remember to have the column names

    # change files into parquet format 
    # save the files in the transform s3 bucket

    # save the packages used to requirements file

