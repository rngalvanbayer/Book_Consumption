from pdf2image import convert_from_path
import requests
import base64
import pandas as pd
from modules import delete_file



def get_finished_data(filename, gptkey, gptendpoint):
    pdf_file_path = filename
    images = convert_from_path(pdf_file_path, dpi=600)          # we can modify the dpi or move to grayscale
    filename =''
    for i, image in enumerate(images):
      image.save(f'page_{i + 1}.png', 'PNG') 
      if i < 1:
        filename = f'page_{i + 1}.png'
    encoded_image = base64.b64encode(open(filename, 'rb').read()).decode('ascii')
    headers = {"Content-Type": "application/json", "api-key": gptkey}

    # Payload for the request
    payload = {
      "messages": [
           {
          "role": "system",
          "content": [
            {
              "type": "text",
              "text": "You are an American English document scanner"
            }
          ],
          "role": "user",
          "content": [
            {
              "type": "image_url",
              "image_url": {
                "url": f"data:image/jpeg;base64,{encoded_image}"
              }
            },
            {
              "type": "text",
              "text": "What are the Bayer Material numbers and corresponding batch numbers and quantity of the finised product, in one continous string using this format 'material number - batch number - quantity'. Batch numbers always starts with 'B0'. Show only the results"
            }
          ]
        }
      ],
      "temperature": 0.05,
      "top_p": 0.1,
      "max_tokens": 800
    }
    # Send request
    try:
        response = requests.post(gptendpoint, headers=headers, json=payload)
        response.raise_for_status() 
    except requests.RequestException as e:
        raise SystemExit(f"Failed to make the request. Error: {e}")
    
    delete_file(filename)
    results = response.json()['choices'][0]['message']['content'] 
    rows = str(results).splitlines()
    data = pd.DataFrame()

    # format data as pandas df
    for i in range(len(rows)):
        try:
            dat = str(rows[i]).split()
            mat_no = str(dat[0]).split('/')[0]
            bat_no = str(dat[2])
            quantity = str(str(dat[4]).replace('.','')).replace(',','')
            new_rows = pd.DataFrame({'Material No' : [mat_no], 'Batch No': [bat_no] , 'Quantity': [quantity]})
            data = pd.concat([data, new_rows], ignore_index=True)
        except Exception as e:
           print(f"An unexpected error occurred: {e}")
           print(rows[i])
    return data



def get_provided_data(filename, gptkey, gptendpoint):
    pdf_file_path = filename
    images = convert_from_path(pdf_file_path, dpi=600)
    filename =''
    for i, image in enumerate(images):
      image.save(f'page_{i + 1}.png', 'PNG') 
      if i < 1:
        filename = f'page_{i + 1}.png'
    encoded_image = base64.b64encode(open(filename, 'rb').read()).decode('ascii')
    headers = {"Content-Type": "application/json", "api-key": gptkey}

    # Payload for the request
    payload = {
      "messages": [
           {
          "role": "system",
          "content": [
            {
              "type": "text",
              "text": "You are an American English document scanner"
            }
          ],
          "role": "user",
          "content": [
            {
              "type": "image_url",
              "image_url": {
                "url": f"data:image/jpeg;base64,{encoded_image}"
              }
            },
            {
              "type": "text",
              "text": "What are the Bayer Material numbers and corresponding batch numbers and KG total under the 'PROVIDED MATERIAL' column, this is the right side of the page, in one continous string using this format 'material number - batch number - KG total'. Here, the batch number starts with 'BX' and the material number is on the left side of this batch number. Show only the results"
            }
          ]
        }
      ],
      "temperature": 0.05,
      "top_p": 0.1,
      "max_tokens": 800
    }
    # Send request
    try:
        response = requests.post(gptendpoint, headers=headers, json=payload)
        response.raise_for_status() 
    except requests.RequestException as e:
        raise SystemExit(f"Failed to make the request. Error: {e}")
    
    delete_file(filename)
    results = response.json()['choices'][0]['message']['content'] 
    rows = str(results).splitlines()
    data = pd.DataFrame()

    # format data as pandas df
    for i in range(len(rows)):
        try:
            dat = str(rows[i]).split()
            mat_no = str(dat[0])
            bat_no = str(dat[2])
            quantity = str(str(dat[4]).replace('.','')).replace(',','')
            new_rows = pd.DataFrame({'Material No' : [mat_no], 'Batch No': [bat_no] , 'Quantity': [quantity]})
            data = pd.concat([data, new_rows], ignore_index=True)
        except Exception as e:
           print(f"An unexpected error occurred: {e}")
           print(rows[i])
    return data

