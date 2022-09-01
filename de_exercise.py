"""
MissionWired Data Engineer Exercise
"""

import pandas as pd

def make_people_file(
    cons_path = 'https://als-hiring.s3.amazonaws.com/fake_data/2020-07-01_17%3A11%3A00/cons.csv',
    email_path = 'https://als-hiring.s3.amazonaws.com/fake_data/2020-07-01_17%3A11%3A00/cons_email.csv',
    subs_path = 'https://als-hiring.s3.amazonaws.com/fake_data/2020-07-01_17%3A11%3A00/cons_email_chapter_subscription.csv'
    ):
    """
    Creates people file of primary email addresses and subscription status

    Parameters
    ----------
        cons_path: str
            File path of constituents information file
        email_path: str
            File path of constituent email addresses file
        subs_path: str
            File path of constituent subcription information file
    
    """
    #Read in data
    df_cons = pd.read_csv(cons_path, usecols=['cons_id', 'source', 'create_dt', 'modified_dt'])
    df_email = pd.read_csv(email_path, usecols = ['cons_email_id', 'cons_id', 'is_primary', 'email'])
    df_subs = pd.read_csv(subs_path, usecols = ['cons_email_id', 'chapter_id', 'isunsub'])

    #Convert string date fields to datetime
    df_cons[['create_dt', 'modified_dt']] = df_cons[['create_dt', 'modified_dt']].apply(pd.to_datetime)
    
    #Keep primary emails
    df_email = df_email[df_email.is_primary == 1]

    #Keep subscriptions with chapter_id 1
    df_subs = df_subs[df_subs.chapter_id == 1]

    #Merge constituent file to primary emails
    df = pd.merge(df_cons, df_email, how = 'right', on = 'cons_id')

    #Merge onto subscription file
    df = df.merge(df_subs, how = 'left', on = 'cons_email_id')

    #Fill in missing subscription status, convert to boolean
    df['isunsub'] = df.isunsub.fillna(0).astype(bool)

    #Reorder and rename table
    df = df[['email', 'source', 'isunsub', 'create_dt', 'modified_dt']]
    df = df.rename(columns = {'isunsub':'is_unsub', 'create_dt':'created_dt', 'modified_dt':'updated_dt'})

    #Export csv
    df.to_csv('people.csv', index=False)

def make_acquisitions_file(file = 'people.csv'):
    """
    Creates acquisitions file about when users were acquired

    Parameters
    ----------
        file: str
            File path of people file
    """

    #Read people file
    df = pd.read_csv(file, parse_dates=['created_dt', 'updated_dt'])

    #Aggregate data
    df['acquisition_date'] = df['created_dt'].dt.date
    df = df.groupby('acquisition_date')['email'].count().reset_index()

    #Export csv
    df = df.rename(columns = {'email':'acquisitions'})
    df.to_csv('acquisition_facts.csv', index=False)
    

if __name__ == '__main__':
    make_people_file()
    make_acquisitions_file()