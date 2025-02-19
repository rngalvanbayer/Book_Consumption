import pdfplumber
import pandas as pd 
import io

def encode(filename):
    pd.options.mode.copy_on_write = True
    pdf_path = filename

    # Open the PDF file
    with pdfplumber.open(pdf_path) as pdf:
        extracted_text = ""
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                extracted_text += text + "\n"


    data =  ' '.join(extracted_text.split('\n'))
    data_list = data.split(' ')
    df = pd.DataFrame({'data': pd.Series(data_list)})


    # == provided material
    filtered_df = df.loc[df['data'].str.match(r'^8[a-zA-Z0-9]*$')] 
    filtered_df = filtered_df.loc[filtered_df['data'].str.len() >= 8]
    provided_material = filtered_df.loc[filtered_df['data'].str.len() >= 13]
    provided_material['batch'] = provided_material['data'].str[-7:]
    provided_material['mat'] = provided_material['data'].str[:-7]
    df['qty'] = ''
    for index, row in provided_material.iterrows():
        df.iloc[index, 1] = df.iloc[index +2, 0]

    provided_material['qty'] = df['qty']
    provided_material = provided_material.drop('data', axis=1)
    p_data = pd.DataFrame()
    p_data['Material Type']  = ''
    p_data['Material No'] = provided_material['mat']
    p_data['Batch'] = provided_material['batch']
    p_data['Qty'] = provided_material['qty']
    p_data['Material Type']  = 'Bulk'


    # == finished products
    filtered_df = None
    filtered_df = df.loc[df['data'].str.match(r'^8[a-zA-Z0-9]*$')] 
    filtered_df = filtered_df.loc[filtered_df['data'].str.len() <= 14]
    finished_product = filtered_df.loc[filtered_df['data'].str.len() > 10]
    finished_product['ver'] = finished_product['data'].str[-4:]
    finished_product['mat'] = finished_product['data'].str[:-4]
    df['batch'] = ''
    df['qty'] = ''
    for index, row in finished_product.iterrows():
        df.iloc[index, 2] = df.iloc[index +1, 0]
        df.iloc[index, 1] = df.iloc[index +2, 0]
    finished_product['batch'] =  df['batch']
    finished_product['qty'] = df['qty'].str.replace(r'[a-zA-Z]', '', regex=True)
    finished_product = finished_product.drop('data', axis=1)
    finished_product = finished_product.drop('ver', axis=1)
    f_data = pd.DataFrame()
    f_data['Material Type']  = ''
    f_data['Material No'] = finished_product['mat']
    f_data['Batch'] = finished_product['batch']
    f_data['Qty'] = finished_product['qty']
    f_data['Material Type']  = 'Finished Good'
    
    print(f_data.to_markdown())
    print("\n")
    print(p_data.to_markdown())
    print("\n")
    return f_data, p_data