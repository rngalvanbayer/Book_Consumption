import modules
import credentials
import pandas as pd
import gptmodules
import sqlalchemy
from databricks import sql
from credentials import dbxcat, dbxhost, dbxpath, dbxschema, dbxtoken
import os
import openpyxl



def main():
    # list all files in the sharepoint folder
    access_token = modules.get_graph_token()                            # Graph API token
    site_id = modules.get_site_id(access_token, 
                                 'bayergroup.sharepoint.com', 
                                 'BookConsumption')                     # sharepoint site id
    drive_id= modules.get_drive_id(access_token, site_id)               # sharepoint drive id
    folder_path = 'Files'                                               # child folder in sharepoint
    files = modules.get_sharepoint_files(access_token, 
                                         site_id, folder_path)          # list of all files in child folder

    no_of_files = len(files)
    #no_of_files = 1

    
    
    finished_data = pd.DataFrame()
    provided_data = pd.DataFrame()
    order_item = pd.DataFrame()
    
    # ======= start iteration here ========= 
    for i in range(no_of_files):
        filename = files[i]['name']
        if str('.pdf') in str(filename):
                      
            file_id = modules.get_file_id(access_token, site_id, drive_id, filename)
            modules.download_file(access_token, site_id, drive_id, file_id, filename)

            # scan the file - finished data 
            f_data = gptmodules.get_finished_data(filename, credentials.GPT4V_KEY, credentials.GPT4V_ENDPOINT)
            finished_data = pd.concat([finished_data, f_data], ignore_index=True)
            #print(f_data)

            # scan the file - provided material
            p_data = gptmodules.get_provided_data(filename, credentials.GPT4V_KEY, credentials.GPT4V_ENDPOINT)
            provided_data = pd.concat([provided_data, p_data], ignore_index=True)
            #print(p_data)
        
            # scan the filename - order item
            o_item = modules.extract_numbers(filename)
            order_item = o_item[0]
            print(order_item)

            # delete the pdf file and pngs
            modules.delete_file(filename)
            f_data['Material No'] = str(order_item)
            merged_df = modules.alternate_rows(f_data, p_data)
            merged_df['Remarks'] = " "
            #print(merged_df.to_markdown())
            fname = "GVT2-" + order_item + ".xlsx"
            merged_df.to_excel(fname, index=False, sheet_name=order_item)

    # ===== Databricks =====
    dbxcat = "efdataonelh_prd"
    dbxschema = "iaut_dmart"
    dbxhost     = os.environ["DBXHOST"]                                       ## Credentials
    dbxpath     = os.environ["DBXPATH"]                                       ## Credentials
    dbxtoken    = os.environ["DBXTOKEN"]          

    extra_connect_args = {
        "_tls_verify_hostname": True,
        "_user_agent_entry": "ELSA Databricks Client",
    }


    engine = sqlalchemy.create_engine(
        f"databricks://token:{dbxtoken}@{dbxhost}?http_path={dbxpath}&catalog={dbxcat}&schema={dbxschema}",
        connect_args=extra_connect_args, echo=False,
    )

    cnxn = engine.connect()
    # === EEND Databricks

    # list all files in the directory
    files = os.listdir()

    # Convert the list of files to a DataFrame
    df = pd.DataFrame(files, columns=['Filename'])
    
    # filter out only GVT2 Excel files
    gvt2_df = df[df['Filename'].str.contains('GVT2-')]
  
    
    # loop around the numbers of GVT2 excel files
    for i in range(len(gvt2_df)):
        filename = gvt2_df.iloc[i]['Filename']
        print(filename)
        
        # load the excel file to a temporary dataframe
        df = pd.read_excel(filename)
        print(df.to_markdown())
        
        #loop around the df:
        for j in range(len(df)):
            type = df.iloc[j]['Material Type']
            matno = str(df.iloc[j]['Material No'])
            batch = str(df.iloc[j]['Batch'])
            qty = df.iloc[j]['Qty']
            #print(type)
            
            # IF type is Bulk
            if type == 'Bulk':
                #print('bulk')
                sqlquery = "SELECT MATNR, CHARG, CINSM, CLABS, LFMON, LFGJA FROM efdataonelh_prd.generaldiscovery_matmgt_r.all_mchbh_view where MATNR LIKE '%" + matno + "' AND CHARG = '" + batch + "' AND LFGJA IN ('2024','2025')"
                #print(sqlquery)
                try:
                    df2 = pd.read_sql_query(sqlquery, cnxn)
                    dbx_qty = (df2['CLABS'].sum()) * 1000
                    # write the quantity to the excel file
                    workbook = openpyxl.load_workbook(filename=filename)
                    sheet = workbook.active
                    cellno = "E" + str(j+2)
                    sheet[cellno] = dbx_qty 
                    workbook.save(filename)
                    #print(dbx_qty)
                    #print(df2.to_markdown())
            
                except:
                    print("error")
            else:
                sqlquery = "SELECT MATNR, CHARG, CINSM, CLABS, LFMON, LFGJA FROM efdataonelh_prd.generaldiscovery_matmgt_r.all_mchbh_view where MATNR LIKE '%" + matno + "' AND CHARG = '" + batch + "' AND LFGJA IN ('2024','2025')"
                #print(sqlquery)
                try:
                    df2 = pd.read_sql_query(sqlquery, cnxn)
                    dbx_qty = df2['CINSM'].sum()
                    # write the quantity to the excel file
                    workbook = openpyxl.load_workbook(filename=filename)
                    sheet = workbook.active
                    cellno = "E" + str(j+2)
                    sheet[cellno] = dbx_qty
                    workbook.save(filename)
                    #print(dbx_qty)
                    #print(df2.to_markdown())
            
                except:
                    print("error")
            
    
if __name__ == "__main__":
    main()