import sys, logging
import pandas as pd

import utils


OO_file = '/mnt/c/Users/laure/OneDrive/Documents/Obsidian Vault/Joe/data/2023 PRI OO UID - GENERAL.csv'


def data_cleanup(df):
   
    # cleaning data in OO 5.3 HF
    logging.info('Cleaning OO 5.3 HF')
    condition = (df['indicator'] == 'OO 5.3 HF') & (df['question_text'] == '(H) Other strategies - Specify:')
    df.loc[condition, 'question_text'] = 'Provide a further breakdown of your internally managed hedge fund assets.'
    df.loc[condition, 'sub_question_text'] = '(I) Other strategies - Specify:'

    # cleaning data in OO 5.3 INF
    logging.info('Cleaning OO 5.3 INF')
    condition = (df['indicator'] == 'OO 5.3 INF') & (df['question_text'] == '(J) Other - Specify:')
    df.loc[condition, 'question_text'] = 'Provide a further breakdown of your internally managed infrastructure AUM.'
    df.loc[condition, 'sub_question_text'] = '(K) Other - Specify:'

    # cleaning data in OO 5.3 LE
    logging.info('Cleaning OO 5.3 LE')
    condition = (df['indicator'] == 'OO 5.3 LE') & (df['question_text'] == '(D) Other strategies - Specify:')
    df.loc[condition, 'question_text'] = 'Provide a further breakdown of your internally managed listed equity AUM.'
    df.loc[condition, 'sub_question_text'] = '(E) Other strategies - Specify:'

    # cleaning data in OO 5.3 PE
    logging.info('Cleaning OO 5.3 PE')
    condition = (df['indicator'] == 'OO 5.3 PE') & (df['question_text'] == '(F) Other - Specify:')
    df.loc[condition, 'question_text'] = 'Provide a further breakdown of your internally managed private equity AUM.'
    df.loc[condition, 'sub_question_text'] = '(G) Other - Specify:'

    # cleaning data in OO 5.3 RE
    logging.info('Cleaning OO 5.3 RE')
    condition = (df['indicator'] == 'OO 5.3 RE') & (df['question_text'] == '(K) Other - Specify:')
    df.loc[condition, 'question_text'] = 'Provide a further breakdown of your internally managed real estate AUM.'
    df.loc[condition, 'sub_question_text'] = '(L) Other - Specify:'

    # cleaning data in OO 10
    logging.info('Cleaning OO 10')
    df.loc[df['indicator'] == 'OO 10', 'sub_question_text'] = df.loc[df['indicator'] == 'OO 10', 'sub_question_text'].str.replace('Stewardship, excluding (proxy) voting', '(A) Stewardship, excluding (proxy) voting')

    # cleaning data in OO 15
    logging.info('Cleaning OO 15')
    df.loc[df['indicator'] == 'OO 15', 'sub_question_text'] = df.loc[df['indicator'] == 'OO 15', 'sub_question_text'].str.replace('Externally managed', '(A) Externally managed')
    df.loc[df['indicator'] == 'OO 15', 'sub_question_text'] = df.loc[df['indicator'] == 'OO 15', 'sub_question_text'].str.replace('Internally managed', '(B) Internally managed')
    
    # cleaning data in OO 16
    logging.info('Cleaning OO 16')
    df.loc[df['indicator'] == 'OO 16', 'sub_question_text'] = df.loc[df['indicator'] == 'OO 16', 'sub_question_text'].str.replace('Externally managed', '(A) Externally managed')
    df.loc[df['indicator'] == 'OO 16', 'sub_question_text'] = df.loc[df['indicator'] == 'OO 16', 'sub_question_text'].str.replace('Internally managed', '(B) Internally managed')

    ## cleaning data in OO 18
    logging.info('Cleaning OO 18')
    condition = (df['indicator'] == 'OO 18') & (df['question_text'] == 'Additional information: (Voluntary)')
    df.loc[condition, 'question_text'] = 'Do you explicitly market any of your products and/or funds as ESG and/or sustainable?'
    df.loc[condition, 'sub_question_text'] = '(D) Additional information: (Voluntary)'
    
    # cleaning data in OO 20
    logging.info('Cleaning OO 20')
    condition = (df['indicator'] == 'OO 20') & (df['question_text'] == '(F) Other - Specify:')
    df.loc[condition, 'question_text'] = 'What percentage of your total environmental and/or social thematic bonds are labelled by the issuers in accordance with industry-recognised standards?'
    df.loc[condition, 'sub_question_text'] = '(G) Other - Specify:'

    # cleaning data in OO 21
    logging.info('Cleaning OO 21')
    df.loc[df['indicator'] == 'OO 21', 'sub_question_text'] = df.loc[df['indicator'] == 'OO 21', 'sub_question_text'].str.replace('Confidence Building Measures', '(1) Confidence Building Measures')
    df.loc[df['indicator'] == 'OO 21', 'sub_question_text'] = df.loc[df['indicator'] == 'OO 21', 'sub_question_text'].str.replace('Policy, Governance and Strategy', '(2) Policy, Governance and Strategy')

    return df

def check_format(df):
    # Define the expected pattern using regular expressions
    # expected_pattern = r'\([^)]+\)\s.*'
    expected_pattern = r'^\([A-Za-z0-9]+\)\s.+'

    # Filter rows where 'sub_question_text' does not match the expected pattern
    regex_match = df['sub_question_text'].str.match(expected_pattern)
    
    # Filter rows where 'sub_question_text' is NaN
    nan_rows = df['sub_question_text'].isna()
    
    # Combine the two conditions using bitwise OR (|)
    mismatched_rows = df[~(regex_match | nan_rows)]

    if mismatched_rows.empty:
        logging.info("Checked format for column 'sub_question_text'")
    else:
        for index, row in mismatched_rows.iterrows():
            logging.error(f"Row {index}: '{row['Signatory Name']}' '{row['indicator']}' '{row['question_text']}' '{row['sub_question_text']}' does not match the specified format.")
        
    return df

def split_label_from_text(df, col, label_col):

    logging.info( 'Spliting label from text in column ' + col)

    df = df.copy()  # Ensure we're working with a copy of the DataFrame

    split_columns = df[col].str.split(')', n=1, expand=True)
    df.loc[:,label_col] = split_columns[0].str.replace('(', '')
    df.loc[:,col] = split_columns[1]

    if split_columns.shape[1] > 2:
        logging.error( 'split_label_from_text: extracted more than 2 columns for column '+ col )

    return df

def pre_processing(df):
    
    df = split_label_from_text(df,'sub_question_text', 'sub_question_label')
    df = split_label_from_text(df,'sub_sub_question_text', 'sub_sub_question_label')

    return df

def add_subsidiaries_data(df, df_oo2, df_oo2_1, df_oo2_2 ):
 
    logging.info('Adding subsidiaries data')

    df = pd.merge(df, df_oo2[['report_ID', 'sub_question_text']], on='report_ID', how='left')
    df['sub_question_text'] = df['sub_question_text'].replace('(A) Yes', True)
    df['sub_question_text'] = df['sub_question_text'].replace('(B) No', False)
    df = df.rename(columns={'sub_question_text': 'OO2_has_subsidiaries'})

    df = pd.merge(df, df_oo2_1[['report_ID', 'sub_question_text']], on='report_ID', how='left')
    df['sub_question_text'] = df['sub_question_text'].replace('(A) Yes', True)
    df['sub_question_text'] = df['sub_question_text'].replace('(B) No', False)
    df = df.rename(columns={'sub_question_text': 'OO2_subsidiaries_PRI_signatory'})

    df_right = df_oo2_2[(df_oo2_2['question_text'] == 'How many subsidiaries of your organisation are PRI signatories in their own rights?')]
    df = pd.merge(df, df_right[['report_ID', 'sub_question_text']], on='report_ID', how='left')
    df = df.rename(columns={'sub_question_text': 'OO2_n_subsidiaries'})

    df_right = df_oo2_2[(df_oo2_2['question_text'] == 'List any subsidiaries of your organisation that are PRI signatories in their own right and indicate if the responsible investment activities of the listed subsidiaries will be reported in this submission.')\
                         & (df_oo2_2['question_type_pri'] == 'Text')]
    df = pd.merge(df, df_right[['report_ID', ' Signatory_Public_Response ']], on='report_ID', how='left')
    df = df.rename(columns={' Signatory_Public_Response ': 'OO2_subsidiary_signatory'})

    df_right = df_oo2_2[(df_oo2_2['question_text'] == 'List any subsidiaries of your organisation that are PRI signatories in their own right and indicate if the responsible investment activities of the listed subsidiaries will be reported in this submission.')\
                         & (df_oo2_2['question_type_pri'] == 'Single Choice')]
    df = pd.merge(df, df_right[['report_ID', 'sub_sub_question_text']], on='report_ID', how='left')
    
    df['sub_sub_question_text'] = df['sub_sub_question_text'].replace('(1) Yes, the responsible investment activities of this subsidiary will be included in this report', True)
    df['sub_sub_question_text'] = df['sub_sub_question_text'].replace('(2) No, the responsible investment activities of this subsidiary will be included in their separate report', False)
    
    df = df.rename(columns={'sub_sub_question_text': 'OO2_subsidiary_activity_included'})

    return df

def add_fundraising_data( df, df_oo3 ):

    logging.info('Adding fundraising data')

    df = utils.merge_and_rename_column(df,  df_oo3[(df_oo3['question_type_pri'] == 'Text')], ' Signatory_Public_Response ', 'OO3_fundraising_end_date')

    return df

def add_aum_data(df, df_oo4, df_oo5, df_oo6, df_oo7 ):

    logging.info('Adding AUM data')


    # OO 4
    df_temp = df_oo4[(df_oo4['question_type_pri'] == 'Money')]
    df = utils.merge_and_rename_column(df, df_temp[(df_temp['sub_question_text'] == '(A) AUM of your organisation, including subsidiaries, and excluding the AUM subject to execution, advisory, custody, or research advisory only' )], ' Signatory_Public_Response ', 'OO4_AUM_org')
    df = utils.merge_and_rename_column(df,  df_temp[ (df_temp['sub_question_text'] == '(B) AUM of subsidiaries that are PRI signatories in their own right and excluded from this submission, as indicated in [OO 2.2]' )], ' Signatory_Public_Response ', 'OO4_AUM_subsidiaries')
    df = utils.merge_and_rename_column(df,  df_temp[ (df_temp['sub_question_text'] == '(C) AUM subject to execution, advisory, custody, or research advisory only' )], ' Signatory_Public_Response ', 'OO4_AUM_exec')

    df = utils.merge_and_rename_column(df,  df_oo4[(df_oo4['question_type_pri'] == 'Text')], ' Signatory_Public_Response ', 'OO4_AUM_FX_Rate')

    # OO 5
    sub_sub_question_1 = '(1) Percentage of Internally managed AUM'
    sub_sub_question_2 = '(2) Percentage of Externally managed AUM'
    
    df_temp = df_oo5[(df_oo5['question_type_pri'] == 'Percentage') & (df_oo5['sub_question_text'] == '(A) Listed equity')]
    df = utils.merge_and_rename_column(df, df_temp[(df_temp['sub_sub_question_text'] == sub_sub_question_1)], ' Signatory_Public_Response ', 'OO5_AUM_PCT_Equity_INT')    
    df = utils.merge_and_rename_column(df, df_temp[(df_temp['sub_sub_question_text'] == sub_sub_question_2)], ' Signatory_Public_Response ', 'OO5_AUM_PCT_Equity_EXT')    

    df_temp = df_oo5[(df_oo5['question_type_pri'] == 'Percentage') & (df_oo5['sub_question_text'] == '(B) Fixed income') ]
    df = utils.merge_and_rename_column(df, df_temp[(df_temp['sub_sub_question_text'] == sub_sub_question_1)], ' Signatory_Public_Response ', 'OO5_AUM_PCT_Fixed_Income_INT')    
    df = utils.merge_and_rename_column(df, df_temp[(df_temp['sub_sub_question_text'] == sub_sub_question_2)], ' Signatory_Public_Response ', 'OO5_AUM_PCT_Fixed_Income_EXT')  

    df_temp = df_oo5[(df_oo5['question_type_pri'] == 'Percentage') & (df_oo5['sub_question_text'] == '(C) Private equity') ]
    df = utils.merge_and_rename_column(df, df_temp[(df_temp['sub_sub_question_text'] == sub_sub_question_1)], ' Signatory_Public_Response ', 'OO5_AUM_PCT_Private_Equity_INT')     
    df = utils.merge_and_rename_column(df, df_temp[(df_temp['sub_sub_question_text'] == sub_sub_question_2)], ' Signatory_Public_Response ', 'OO5_AUM_PCT_Private_Equity_EXT')     

    df_temp = df_oo5[(df_oo5['question_type_pri'] == 'Percentage') & (df_oo5['sub_question_text'] == '(D) Real estate') ]
    df = utils.merge_and_rename_column(df, df_temp[(df_temp['sub_sub_question_text'] == sub_sub_question_1)], ' Signatory_Public_Response ', 'OO5_AUM_PCT_Real_Estate_INT')     
    df = utils.merge_and_rename_column(df, df_temp[(df_temp['sub_sub_question_text'] == sub_sub_question_2)], ' Signatory_Public_Response ', 'OO5_AUM_PCT_Real_Estate_EXT') 

    df_temp = df_oo5[(df_oo5['question_type_pri'] == 'Percentage') & (df_oo5['sub_question_text'] == '(E) Infrastructure') ]
    df = utils.merge_and_rename_column(df, df_temp[(df_temp['sub_sub_question_text'] == sub_sub_question_1)], ' Signatory_Public_Response ', 'OO5_AUM_PCT_Infrastructure_INT')     
    df = utils.merge_and_rename_column(df, df_temp[(df_temp['sub_sub_question_text'] == sub_sub_question_2)], ' Signatory_Public_Response ', 'OO5_AUM_PCT_Infrastructure_EXT') 

    df_temp = df_oo5[(df_oo5['question_type_pri'] == 'Percentage') & (df_oo5['sub_question_text'] == '(F) Hedge funds') ]
    df = utils.merge_and_rename_column(df, df_temp[(df_temp['sub_sub_question_text'] == sub_sub_question_1)], ' Signatory_Public_Response ', 'OO5_AUM_PCT_Hedge_Fund_INT')     
    df = utils.merge_and_rename_column(df, df_temp[(df_temp['sub_sub_question_text'] == sub_sub_question_2)], ' Signatory_Public_Response ', 'OO5_AUM_PCT_Hedge_fund_EXT') 

    df_temp = df_oo5[(df_oo5['question_type_pri'] == 'Percentage') & (df_oo5['sub_question_text'] == '(G) Forestry') ]
    df = utils.merge_and_rename_column(df, df_temp[(df_temp['sub_sub_question_text'] == sub_sub_question_1)], ' Signatory_Public_Response ', 'OO5_AUM_PCT_Forestry_INT')     
    df = utils.merge_and_rename_column(df, df_temp[(df_temp['sub_sub_question_text'] == sub_sub_question_2)], ' Signatory_Public_Response ', 'OO5_AUM_PCT_Forestry_EXT') 

    df_temp = df_oo5[(df_oo5['question_type_pri'] == 'Percentage') & (df_oo5['sub_question_text'] == '(I) Other') ]
    df = utils.merge_and_rename_column(df, df_temp[(df_temp['sub_sub_question_text'] == sub_sub_question_1)], ' Signatory_Public_Response ', 'OO5_AUM_PCT_Other_INT')     
    df = utils.merge_and_rename_column(df, df_temp[(df_temp['sub_sub_question_text'] == sub_sub_question_2)], ' Signatory_Public_Response ', 'OO5_AUM_PCT_Other_EXT') 

    df_temp = df_oo5[(df_oo5['question_type_pri'] == 'Text') & (df_oo5['sub_question_text'] == '(I) Other - (1) Percentage of Internally managed AUM - Specify:') ]
    df = utils.merge_and_rename_column(df, df_temp[(df_temp['sub_sub_question_text'] == sub_sub_question_1)], ' Signatory_Public_Response ', 'OO5_AUM_PCT_Other_details_INT')   
    df_temp = df_oo5[(df_oo5['question_type_pri'] == 'Text') & (df_oo5['sub_question_text'] == '(I) Other - (2) Percentage of Externally managed AUM - Specify:') ]
    df = utils.merge_and_rename_column(df, df_temp[(df_temp['sub_sub_question_text'] == sub_sub_question_1)], ' Signatory_Public_Response ', 'OO5_AUM_PCT_Other_details_EXT')   

    df_temp = df_oo5[(df_oo5['question_type_pri'] == 'Percentage') & (df_oo5['sub_question_text'] == '(J) Off-balance sheet') ]
    df = utils.merge_and_rename_column(df, df_temp[(df_temp['sub_sub_question_text'] == sub_sub_question_1)], ' Signatory_Public_Response ', 'OO5_AUM_PCT_Off_balance_INT')     
    df = utils.merge_and_rename_column(df, df_temp[(df_temp['sub_sub_question_text'] == sub_sub_question_2)], ' Signatory_Public_Response ', 'OO5_AUM_PCT_Off_balance_EXT') 

    df_temp = df_oo5[(df_oo5['question_type_pri'] == 'Text') & (df_oo5['sub_question_text'] == '(J) Off-balance sheet - (1) Percentage of Internally managed AUM - Specify:') ]
    df = utils.merge_and_rename_column(df, df_temp[(df_temp['sub_sub_question_text'] == sub_sub_question_1)], ' Signatory_Public_Response ', 'OO5_AUM_PCT_Off_balance_details_INT')   
    df_temp = df_oo5[(df_oo5['question_type_pri'] == 'Text') & (df_oo5['sub_question_text'] == '(J) Off-balance sheet - (2) Percentage of Externally managed AUM - Specify:') ]
    df = utils.merge_and_rename_column(df, df_temp[(df_temp['sub_sub_question_text'] == sub_sub_question_1)], ' Signatory_Public_Response ', 'OO5_AUM_PCT_Off_balance_details_EXT')   

    # OO 6
    df = utils.merge_and_rename_column(df, df_oo6, ' Signatory_Public_Response ', 'OO6_AUM_PCT_subsidiaries_PRI_signatory')   
    
    # OO 7
    df = utils.merge_and_rename_column(df, df_oo7[(df_oo7['sub_question_text'] == '(A) Listed equity')], 'sub_sub_sub_question_text', 'OO7_AUM_PCT_Listed_Equity_Emerging_mkt')     
    df = utils.merge_and_rename_column(df, df_oo7[(df_oo7['sub_question_text'] == '(B) Fixed income – SSA')], 'sub_sub_sub_question_text', 'OO7_AUM_PCT_Fixed_Income_SSA_Emerging_mkt')     
    df = utils.merge_and_rename_column(df, df_oo7[(df_oo7['sub_question_text'] == '(C) Fixed income – corporate')], 'sub_sub_sub_question_text', 'OO7_AUM_PCT_Fixed_Income_Corp_Emerging_mkt')    

    df = utils.merge_and_rename_column(df, df_oo7[(df_oo7['sub_question_text'] == '(D) Fixed income – securitised')], 'sub_sub_sub_question_text', 'OO7_AUM_PCT_Fixed_Income_Securities_Emerging_mkt')     
    df = utils.merge_and_rename_column(df, df_oo7[(df_oo7['sub_question_text'] == '(E) Fixed income – private debt')], 'sub_sub_sub_question_text', 'OO7_AUM_PCT_Fixed_Income_Private_Debt_Emerging_mkt')     
    df = utils.merge_and_rename_column(df, df_oo7[(df_oo7['sub_question_text'] == '(F) Private equity')], 'sub_sub_sub_question_text', 'OO7_AUM_PCT_Private_Equity_Emerging_mkt')   

    df = utils.merge_and_rename_column(df, df_oo7[(df_oo7['sub_question_text'] == '(G) Real estate')], 'sub_sub_sub_question_text', 'OO7_AUM_PCT_Real_State_Emerging_mkt')     
    df = utils.merge_and_rename_column(df, df_oo7[(df_oo7['sub_question_text'] == '(H) Infrastructure')], 'sub_sub_sub_question_text', 'OO7_AUM_PCT_Infra_Emerging_mkt')     
    df = utils.merge_and_rename_column(df, df_oo7[(df_oo7['sub_question_text'] == '(I) Hedge funds')], 'sub_sub_sub_question_text', 'OO7_AUM_PCT_Hedge_Funds_Emerging_mkt')   

    # OO 8

    return df

def loading_Overview(filename=OO_file):
    logging.info('Starting to load data from OO file')

    df = utils.load_CSV(filename, encoding='ISO-8859-1')

    df_form = utils.extract_form(df)
   
    df_signatory = utils.build_signatory_profile(df[df['indicator'] == 'OO 1'])
    df = df[df['indicator'] != 'OO 1']

    df = utils.set_report_ID(df, df_signatory)

    df_signatory = add_subsidiaries_data(df_signatory,\
                                        df[(df['indicator'] == 'OO 2') & (df['response_answer'] == 'Selected')],\
                                        df[(df['indicator'] == 'OO 2.1') & (df['response_answer'] == 'Selected')],\
                                        df[(df['indicator'] == 'OO 2.2') & (df['response_answer'] == 'Selected')] )
    
    

    df_signatory = add_fundraising_data( df_signatory, df[(df['indicator'] == 'OO 3') & (df['response_answer'] == 'Selected')] )

    df_signatory = add_aum_data( df_signatory,\
                                 df[(df['indicator'] == 'OO 4') & (df['response_answer'] == 'Selected')],\
                                 df[(df['indicator'] == 'OO 5') & (df['response_answer'] == 'Selected')],\
                                 df[(df['indicator'] == 'OO 6') & (df['response_answer'] == 'Selected')],\
                                 df[(df['indicator'] == 'OO 7') & (df['response_answer'] == 'Selected')] )

    df = df[~df['indicator'].isin(['OO 2', 'OO 2.1', 'OO 2.2', 'OO 3', 'OO 4', 'OO 5', 'OO 6', 'OO 7'])]

    logging.info('Exporting signatory data to CSV')
    df_signatory.to_csv('/home/lngo/projects/Joe/signatory.csv', index=False)

    df = data_cleanup(df)
    df = check_format(df)
    df = pre_processing(df)
    

    df_form.to_csv('/home/lngo/projects/Joe/questions.csv', index=False)
    df.to_csv('/home/lngo/projects/Joe/test.csv', index=False)

    logging.info('Overview complete')

if __name__ == '__main__':
    logging.basicConfig( level=logging.INFO)

    logging.info('starting from this file')
    loading_Overview()