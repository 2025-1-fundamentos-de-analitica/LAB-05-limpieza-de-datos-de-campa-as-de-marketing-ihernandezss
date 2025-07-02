# Implemente el código necesario para llevar a cabo la tarea de limpieza y transformación de datos de campaña.

import pandas as pd
import os
import zipfile
import io

def read_compressed_csvs(input_folder):
    data_frames = []
    for file in os.listdir(input_folder):
        if file.endswith('.zip'):
            with zipfile.ZipFile(os.path.join(input_folder, file), 'r') as archive:
                for info in archive.infolist():
                    if info.filename.endswith('.csv'):
                        with archive.open(info) as csvfile:
                            data_frames.append(pd.read_csv(io.BytesIO(csvfile.read())))
    return pd.concat(data_frames, ignore_index=True)

def process_client_data(df):
    return pd.DataFrame({
        'client_id': df['client_id'],
        'age': df['age'],
        'job': df['job'].str.replace('.', '', regex=False).str.replace('-', '_', regex=False),
        'marital': df['marital'],
        'education': df['education'].str.replace('.', '_', regex=False).replace('unknown', pd.NA),
        'credit_default': df['credit_default'].map(lambda x: 1 if x == 'yes' else 0),
        'mortgage': df['mortgage'].map(lambda x: 1 if x == 'yes' else 0)
    })

def process_campaign_data(df):
    month_map = {
        'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04', 'may': '05',
        'jun': '06', 'jul': '07', 'aug': '08', 'sep': '09', 'oct': '10',
        'nov': '11', 'dec': '12'
    }
    date_series = '2022-' + df['month'].str.lower().map(month_map) + '-' + df['day'].astype(str).str.zfill(2)

    return pd.DataFrame({
        'client_id': df['client_id'],
        'number_contacts': df['number_contacts'],
        'contact_duration': df['contact_duration'],
        'previous_campaign_contacts': df['previous_campaign_contacts'],
        'previous_outcome': df['previous_outcome'].map(lambda x: 1 if x == 'success' else 0),
        'campaign_outcome': df['campaign_outcome'].map(lambda x: 1 if x == 'yes' else 0),
        'last_contact_date': date_series
    })

def process_economics_data(df):
    return df[['client_id', 'cons_price_idx', 'euribor_three_months']]

def save_to_csv(df, path):
    df.to_csv(path, index=False)

def clean_campaign_data():
    os.makedirs('files/output', exist_ok=True)
    df = read_compressed_csvs('files/input')
    
    client_df = process_client_data(df)
    campaign_df = process_campaign_data(df)
    economics_df = process_economics_data(df)

    save_to_csv(client_df, 'files/output/client.csv')
    save_to_csv(campaign_df, 'files/output/campaign.csv')
    save_to_csv(economics_df, 'files/output/economics.csv')

if __name__ == "__main__":
    clean_campaign_data()
