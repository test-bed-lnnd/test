from tqdm import tqdm
import sys, logging
import pandas as pd

def load_csv(filename, chunk_size=10000, encoding='utf-8'):
    """
    Load a large CSV file in chunks and concatenate the chunks into a single DataFrame.
    
    Parameters:
    filename (str): The path to the CSV file.
    chunk_size (int): The number of rows per chunk. Default is 10000.
    encoding (str): The encoding of the CSV file. Default is 'utf-8'.

    Returns:
    pd.DataFrame: The concatenated DataFrame containing all the data from the CSV file.
    """
    
    df = pd.DataFrame()

    total_rows = sum(1 for _ in open(filename, encoding=encoding))

    with tqdm(total=total_rows) as pbar:
        for chunk in pd.read_csv(filename, chunksize=chunk_size, encoding=encoding):
            df = pd.concat([df, chunk], ignore_index=True)
            pbar.update(chunk.shape[0])

    return df

def remove_columns(df, columns_to_remove):
    """
    Remove specified columns from the DataFrame.
    
    Parameters:
    df (pd.DataFrame): The DataFrame from which columns should be removed.
    columns_to_remove (list): A list of column names to remove from the DataFrame.
    
    Returns:
    pd.DataFrame: The DataFrame with the specified columns removed.
    """
    # Check if columns_to_remove is a list
    if not isinstance(columns_to_remove, list):
        logging.error("columns_to_remove should be a list of column names")
    
    # Check if all columns in columns_to_remove are in the DataFrame
    missing_columns = [col for col in columns_to_remove if col not in df.columns]
    if missing_columns:
        logging.error(f"Columns {missing_columns} are not in the DataFrame")
    
    # Drop the specified columns
    df = df.drop(columns=columns_to_remove)
    
    return df

def merge_and_rename_column(df, df_right, col_name, new_name):

    if df.shape[0] * 1.1 < df_right.shape[0]:
        logging.error('There are more rows in df_right ||||||||||||||||||||||||||||||||||||||||||||')
    df = pd.merge(df, df_right[['report_ID', col_name]], on='report_ID', how='left', validate=None)
    df = df.rename(columns={col_name: new_name})

    return df

def extract_date(df):
    """
    Filter the DataFrame df_oo1 by column 'response_answer' equal to 'Selected'.
    
    Parameters:
    df_oo1 (pd.DataFrame): DataFrame containing data where 'indicator' column equals 'OO 1'.

    Returns:
    pd.DataFrame: Filtered DataFrame containing only rows where 'response_answer' column is 'Selected'.
    """


    logging.info('Extracting date from \'OO 1\' questions')
    df = df[df['response_answer'] == 'Selected']
        
    # Group by 'Signature Name' to process each group individually
    groupedby= 'Signatory Name'
    grouped = df.groupby(groupedby)

    df = df[df['sub_sub_question_text'] == 'Date']
    
    for name, group in grouped:
        day = group[group['sub_sub_question_text'] == 'Date']['sub_sub_sub_question_text'].values
        month = group[group['sub_sub_question_text'] == 'Month']['sub_sub_sub_question_text'].values
        year = group[group['sub_sub_question_text'] == 'Year']['sub_sub_sub_question_text'].values
        
        if day.size > 0 and month.size > 0 and year.size > 0:
            date_str = f"{day[0]}/{month[0]}/{year[0]}"
            df.loc[df[groupedby] == name, 'sub_sub_sub_question_text'] = date_str
        else:
            logging.error('Incorrect date format for Signatory Name ' + name )

    return df

def build_signatory_profile(df):

    df=extract_date(df)

    df = remove_columns(df, ['indicator', 'question_text', 'sub_question_text', 'sub_sub_question_text', 'response_group_text', 'module_short'])
    df = remove_columns(df, ['section', 'subsection', 'question_type_pri', 'response_answer', ' Signatory_Public_Response ', 'Column2', 'UID'])

    df = df.rename(columns={'sub_sub_sub_question_text': 'OO1_year_end_date'})

    # Check for duplicate combinations
    duplicates = df.duplicated(subset=[groupedby, 'signatory_category', 'aum_band', 'Peering Country', 'region', 'sigtype'], keep=False)
    
    if duplicates.any():
        duplicate_rows = df[duplicates]
        logging.error(f"Duplicate combinations found:\n{duplicate_rows}")
    else:
        logging.info("All Signatories are unique.")

    df['report_ID'] = range(1, 1+ len(df))

    return df

def set_report_id(df, df_signatory):

    match_cols = [groupedby, 'signatory_category', 'aum_band', 'Peering Country', 'region', 'sigtype']

    df = pd.merge(df, df_signatory[match_cols + ['report_ID']], on=match_cols, how='left', validate=None)

    return df

def extract_form(df):


    logging.info('extracting questions details')
    # Extract unique rows based on the specified columns
    df_form = df[['module_short', 'question_type_pri', 'indicator', 'Core/Plus',\
                'question_text', 'sub_question_text', 'sub_sub_question_text','sub_sub_sub_question_text']].drop_duplicates()

    return df_form


def extract_survey(df_res, df, df_form):
    
    questions = df_form['indicator'].unique()
    questions_list = questions.tolist()
    questions_list.sort()

    for q in questions_list:
        question_type = df_form[(df_form['indicator'] == q)]['question_type_pri'].unique()
        for t in question_type.tolist():

            public_response = ' Signatory_Public_Response '

            logging.info('extracting ' + q + ' (' + t + ')')
            if q in ['OO 2.2', 'OO 5', 'OO 5.1', 'OO 5.2', 'OO 5.3 FI', 'OO 5.3 INF', 'OO 5.3 LE', 'OO 5.3 PE', 'OO 5.3 RE'] :
                logging.error('Skipping ' + q + ' since it has multiple answers for a single choice')

            elif q in ['OO 7', 'OO 8', 'OO 9', 'OO 11', 'OO 12', 'OO 13', 'OO 14', 'OO 18.2', 'OO 20', 'OO 21'] :
                logging.error('Skipping ' + q + ' since it has multiple answers for a single choice')
            
            elif q == 'OO 4':
                df_res = merge_and_rename_column(df_res,  df[(df['indicator'] == q) & (df['question_type_pri'] == MONEY_TYPE_PRI) & (df['sub_question_text'] == 'A')], public_response, q +' A '+ MONEY_TYPE_PRI)
                df_res = merge_and_rename_column(df_res,  df[(df['indicator'] == q) & (df['question_type_pri'] == MONEY_TYPE_PRI) & (df['sub_question_text'] == 'B')], public_response, q +' B '+ MONEY_TYPE_PRI)
                df_res = merge_and_rename_column(df_res,  df[(df['indicator'] == q) & (df['question_type_pri'] == MONEY_TYPE_PRI) & (df['sub_question_text'] == 'C')], public_response, q +' C '+ MONEY_TYPE_PRI)
                df_res = merge_and_rename_column(df_res,  df[(df['indicator'] == q) & (df['question_type_pri'] == TEXT_TYPE_PRI)], public_response, q +' '+ TEXT_TYPE_PRI)

            elif t == TEXT_TYPE_PRI:
                df_res = merge_and_rename_column(df_res,  df[(df['indicator'] == q) & (df['question_type_pri'] == TEXT_TYPE_PRI)], public_response, q +' '+ TEXT_TYPE_PRI)
            elif t == MONEY_TYPE_PRI:
                df_res = merge_and_rename_column(df_res,  df[(df['indicator'] == q) & (df['question_type_pri'] == MONEY_TYPE_PRI)], public_response, q +' '+ MONEY_TYPE_PRI)
            elif t == SINGLE_CHOICE_TYPE_PRI:
                df_res = merge_and_rename_column(df_res,  df[(df['indicator'] == q) & (df['question_type_pri'] == SINGLE_CHOICE_TYPE_PRI)], 'sub_question_text', q + ' sub')   
                df_res = merge_and_rename_column(df_res,  df[(df['indicator'] == q) & (df['question_type_pri'] == SINGLE_CHOICE_TYPE_PRI)], 'sub_sub_question_text', q +' sub_sub')   
            else:
                logging.error('Unsupported question type: ' + t)


    return df_res

TEXT_TYPE_PRI = 'Text'
MONEY_TYPE_PRI = 'Money'
SINGLE_CHOICE_TYPE_PRI = 'Single Choice'
MULTI_TYPE_PRI = 'Multi Choice'


if __name__ == '__main__':
    
    OO_file = '/mnt/c/Users/laure/OneDrive/Documents/Obsidian Vault/Joe/data/2023 PRI OO UID - GENERAL.csv'

    logging.basicConfig( level=logging.INFO)

    logging.info('starting from this file')
    df = load_csv(OO_file, encoding='ISO-8859-1')


    df_signatory = build_signatory_profile(df[df['indicator'] == 'OO 1'])
    df = df[df['indicator'] != 'OO 1']

    df = set_report_id(df, df_signatory)

    df_form = extract_form(df)
    
    df_form.to_csv('/home/lngo/projects/Joe/questions.csv', index=False)

    sys.exit(0)

    # clean answer columns
    df['sub_question_text'] = df['sub_question_text'].replace(r'^(\(.{1,2}\)).*', r'\1', regex=True)
    df['sub_sub_question_text'] = df['sub_sub_question_text'].replace(r'^(\(.{1,2}\)).*', r'\1', regex=True)
    
    df['sub_question_text'] = df['sub_question_text'].replace('(', '')
    df['sub_sub_question_text'] = df['sub_sub_question_text'].replace('(', '')
    
    df['sub_question_text'] = df['sub_question_text'].replace(')', '')
    df['sub_sub_question_text'] = df['sub_sub_question_text'].replace(')', '')


    df_res = extract_survey(df_signatory, df[(df['response_answer'] == 'Selected')],\
                            df_form[df_form['indicator'] != 'OO 1'])

    df_signatory.to_csv('/home/lngo/projects/Joe/test.csv', index=False)
    df_res.to_csv('/home/lngo/projects/Joe/answers.csv', index=False)
