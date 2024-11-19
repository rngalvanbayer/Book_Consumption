import modules
import credentials
import pandas as pd
import gptmodules
import sqlalchemy
from databricks import sql


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

    #no_of_files = len(files)
    no_of_files = 1
    finished_data = pd.DataFrame()
    provided_data = pd.DataFrame()
    # ======= start iteration here ========= 
    for i in range(no_of_files):
        filename = files[i]['name']
        file_id = modules.get_file_id(access_token, site_id, drive_id, filename)
        modules.download_file(access_token, site_id, drive_id, file_id, filename)

        # scan the file - finished data 
        f_data = gptmodules.get_finished_data(filename, credentials.GPT4V_KEY, credentials.GPT4V_ENDPOINT)
        finished_data = pd.concat([finished_data, f_data], ignore_index=True)
        print(f_data)

        # scan the file - provided material
        p_data = gptmodules.get_provided_data(filename, credentials.GPT4V_KEY, credentials.GPT4V_ENDPOINT)
        provided_data = pd.concat([provided_data, p_data], ignore_index=True)
        print(p_data)

        # delete the pdf file and pngs
        modules.delete_file(filename)
    
    finished_data.to_excel('finished_product.xlsx', index=False)
    provided_data.to_excel('provided_materials.xlsx', index=False)

    # ===== Databricks =====
    dbxcat = "iaut_dmart"
    dbxschema = "exp_ic_162105_open_items"
    dbxhost = "adb-4071335540424391.11.azuredatabricks.net"
    dbxpath = "/sql/1.0/warehouses/63b06d5d61cc1af1"
    dbxtoken = "dapi1d3596c872e65f3f95430ff5a75f6d90-2"

    connection = sql.connect(server_hostname = dbxhost, http_path = dbxpath, access_token = dbxtoken)
    cursor = connection.cursor()

    filename = "exp_ic_162105_open_items"
    local_path = filename + ".xlsx"
    flag = 0

    extra_connect_args = {
        "_tls_verify_hostname": True,
        "_user_agent_entry": "ELSA Databricks Client",
    }


    engine = sqlalchemy.create_engine(
        f"databricks://token:{dbxtoken}@{dbxhost}?http_path={dbxpath}&catalog={dbxcat}&schema={dbxschema}",
        connect_args=extra_connect_args, echo=False,
    )

    cnxn = engine.connect()

    pvd = pd.DataFrame()
    for i in range(len(provided_data)):
        mn = str(provided_data['Material No'][i]).zfill(len(str(provided_data['Material No'][i]))+10)
        #print(mn)
        bn = provided_data['Batch No'][i]
        #bn = finished_data[i]['Batch No']
        sqlquery = "SELECT MATNR, CHARG, CINSM, LFMON FROM efdataonelh_prd.generaldiscovery_matmgt_r.all_mchbh_view where MATNR = '" + mn + "' AND CHARG = '" + bn + "'  AND LFGJA = '2024'"
        print(sqlquery)
        try:
            df = pd.read_sql_query(sqlquery, cnxn)
            pvd = pd.concat([pvd, df], ignore_index=True)
            
        except:
            print("error")

    print(pvd.to_markdown())

if __name__ == "__main__":
    main()