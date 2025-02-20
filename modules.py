import credentials
import requests
from github import Github
import requests
import os
import re 
import pandas as pd 

def extract_numbers(s):
    return re.findall(r'\d+', s)

def get_graph_token():
    g = Github(credentials.GITHUBTOKEN)
    repo = g.get_repo("rngalvanbayer/Graph_token")
    token = ''
    contents = ''
    try:
        file_content = repo.get_contents('token.txt')
        token  = file_content.decoded_content.decode('utf-8')
    except Exception as e:
        print(f"Failed to retrieve file: {e}")
    return token

def delete_file(file_path):
    try:
        os.remove(file_path)
        print(f"File {file_path} has been deleted.")
    except FileNotFoundError:
        print(f"File {file_path} not found.")
    except PermissionError:
        print(f"Permission denied: {file_path}.")
    except Exception as e:
        print(f"Error occurred while deleting file {file_path}: {e}")

def list_sharepoint_contents(access_token, site_id, folder_path):
    headers = {"Authorization": f"Bearer {access_token}"}
    if folder_path:
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:/{folder_path}:/children"
    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Raises an error for bad responses
    items = response.json().get('value', [])
    return items

def get_site_id(access_token, hostname, site_name):
    url = f"https://graph.microsoft.com/v1.0/sites/{hostname}:/sites/{site_name}"
    headers = {"Authorization": f"Bearer {access_token}"}
    
    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Raises an error for bad responses
    site_data = response.json()
    siteid = str(site_data['id']).split(',')
    return siteid[1]


def get_sharepoint_files(access_token, site_id, folder_path):
    # Step 1: Get folder item ID from folder path
    folder_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:/{folder_path}"
    headers = {"Authorization": f"Bearer {access_token}"}
    folder_response = requests.get(folder_url, headers=headers)
    folder_response.raise_for_status()
    folder_item = folder_response.json()
    item_id = folder_item['id']
    
    # Step 2: Get files in the folder
    files_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/items/{item_id}/children"
    files_response = requests.get(files_url, headers=headers)
    files_response.raise_for_status()
    files = files_response.json()
    #print(files)
    return files['value']

def get_drive_id(access_token, site_id):
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives"
    headers = {"Authorization": f"Bearer {access_token}"}
    
    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Raises an error for bad responses
    drive_id = response.json()["value"][0]["id"]
    return drive_id

def download_file(access_token, site_id, drive_id, filename):
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:{filename}:/content"
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        with open(filename, "wb") as file:
            file.write(response.content)
        print("File downloaded successfully.")
    else:
        print("Failed to download file:", response.status_code, response.json())

def get_file_id(access_token, site_id, drive_id, filename):
    filepath = "/Encoding/" + filename
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:{filepath}"
    headers = {"Authorization": f"Bearer {access_token}"}
    
    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Raises an error for bad responses
    item_id = response.json()["id"]
    return item_id

def download_file_old(access_token, site_id, drive_id, filename):
    filepath = "/Encoding/" + filename
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:{filepath}"
    headers = {"Authorization": f"Bearer {access_token}"}
    
    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Raises an error for bad responses
    item_id = response.json()["id"]
    print(item_id)

    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/items/{item_id}/content"
    headers = {"Authorization": f"Bearer {access_token}"}
    
    if response.status_code == 200:
    # Save the file content to a local file
        with open(filename, "wb") as file:
            file.write(response.content['@microsoft.graph.downloadUrl'])
        print("File downloaded successfully.")
    else:
        print("Failed to download file:", response.status_code, response.json())

def download_file(access_token, site_id, drive_id, item_id, file_name):
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/items/{item_id}/content"
    
    headers = {
        'Authorization': f'Bearer {access_token}'
    }
    
    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Raise an error for bad responses
    
    with open(file_name, 'wb') as f:
        f.write(response.content)
    print(f"Downloaded file: {file_name}")
    
def alternate_rows(df1, df2):
    # Create an empty list to hold the combined rows
    combined_rows = []
    
    # Get the length of the longer DataFrame
    max_len = max(len(df1), len(df2))
    
    for i in range(max_len):
        if i < len(df1):
            combined_rows.append(df1.iloc[i])
        if i < len(df2):
            combined_rows.append(df2.iloc[i])
    
    # Create a new DataFrame from the combined rows
    return pd.DataFrame(combined_rows)

def upload_file(access_token, site_id, drive_id, folder_name, file_name):
    filepath = file_name
    url =  f'https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/items/root:/{folder_name}/{file_name}:/content'
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/octet-stream',
        'Content-Length': str(os.path.getsize(file_name))
    }
    with open(filepath, 'rb') as file:
        response = requests.put( url, headers=headers, data=file)
        response.raise_for_status()
        
def delete_file_sp(access_token, drive_id, item_id):
    #url =  f'https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/items/{item_id}'
    url =  f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}"
    headers = {
        'Authorization': f'Bearer {access_token}'
    }
    response = requests.delete(url, headers=headers)
    response.raise_for_status()