import pandas as pd
import os
from datetime import datetime

###########################################################################################
# Constants and Variables

    # date and path
CURRENT_DATE = datetime.now()
DATE_STRING = CURRENT_DATE.strftime('%m_%d_%Y')
INPUT_PATH = './data/'
OUTPUT_PATH = './output/'

    # project and output 
name_output = 'merged_result'
project_name = 'ProjectDemo'
text_name = 'info'
info_text = []

OUTPUT_STRING_XLSX = OUTPUT_PATH + project_name + '_' + name_output + '_' + DATE_STRING + '.xlsx'
OUTPUT_STRING_CSV =  OUTPUT_PATH + project_name + '_' + name_output + '_' + DATE_STRING + '.csv'
OUTPUT_STRING_TXT = OUTPUT_PATH + text_name + '_' + DATE_STRING + '.txt'

    # dataframe columns two versions (look columns up in Excel-File) default Version: _one
columns_main_dataframe_one = ['record_id', 'included', 'asreview_ranking', 'authors', 
                            'title', 'publication_year', 'abstract']
columns_merge_dataframe_one = ['record_id', 'included', 'asreview_ranking']

columns_final_dataframe_one = ['record_id', 'authors', 'title', 'publication_year', 
                            'abstract', 'included_x', 'included_y', 'asreview_ranking_x', 
                            'asreview_ranking_y']

columns_main_dataframe_two = ['record_id', 'included', 'asreview_ranking', 'first_authors', 
                            'primary_title', 'publication_year', 'notes_abstract']
columns_merge_dataframe_two = ['record_id', 'included', 'asreview_ranking']

columns_final_dataframe_two = ['record_id', 'first_authors', 'primary_title', 'publication_year', 
                            'notes_abstract', 'included_x', 'included_y', 'asreview_ranking_x', 
                            'asreview_ranking_y']
    # merge and sort

merge_on = 'record_id'
sort_by = 'asreview_ranking_x'


    # mapper and meanings
MAPPER = {
    0: 'TWO_REV_MATCH_excluded',
    2: 'TWO_REV_MATCH_included',
    1: 'TWO_REV_DIFF',
    -98: 'ONE_REV',
    -99: 'ONE_REV',
    -198: 'NO_REV',
}

MEANINGS = ['Meanings', 'NO_REV: not reviewed', 'ONE_REV: reviewed by one reviewer', 
            'TWO_REV_MATCH_included: included Reviewers match','TWO_REV_MATCH_excluded: excluded Reviewers match',
             'TWO_REV_DIFF: Rating differs']

SEPERATE = '_________________________'

######################################################################################
# functions and main 

def load_dataframes():
    print(SEPERATE)
    print('...loading dataframes...' )
    print('')
    input_files = os.listdir(INPUT_PATH)
    input_file = []

    for filename in input_files:
        input_file.append(INPUT_PATH + filename)

    read_main = pd.read_excel(input_file[0])
    read_merge = pd.read_excel(input_file[1])
    
    df_main = pd.DataFrame(read_main, columns=columns_main_dataframe_one)
    info_main = 'loaded main dataframe. Shape: ' + str(df_main.shape)
    print(info_main)
    df_merge = pd.DataFrame(read_merge, columns=columns_merge_dataframe_one)
    info_merge = 'loaded merge dataframe . Shape: ' + str(df_merge.shape)
    print(info_merge)
    print(SEPERATE)

    info_text.append(info_main)
    info_text.append(info_merge)
    info_text.append(SEPERATE)

    return df_main, df_merge

def merge_dataframes(df_main, df_merge):
    # merge
    df = df_main.merge(df_merge, left_on=merge_on, right_on=merge_on )
    df = df.loc[:, columns_final_dataframe_one]
    df = df.sort_values(sort_by, ascending=True)
    df = df.fillna(-99)
    # compare and recode
    df['MATCH_INFO'] = df['included_x'] + df['included_y']
    df['MATCH_INFO'] = [MAPPER[i] for i in df['MATCH_INFO']]
    df_final = df

    info_final = 'dataframes merged. Shape: ' + str(df_final.shape)
    print(info_final)
    print(SEPERATE)

    info_text.append(info_final)
    info_text.append(SEPERATE)

    return df_final

def save_dataframe(df):
    df.to_csv(OUTPUT_STRING_CSV, sep=',', header=True)
    print('saved dataframe: ', OUTPUT_STRING_CSV)
    df.to_excel(OUTPUT_STRING_XLSX, header=True)
    print('saved dataframe: ', OUTPUT_STRING_XLSX)
    print(SEPERATE)

def main():
    df_main, df_merge = load_dataframes()
    df_final = merge_dataframes(df_main, df_merge)
    save_dataframe(df_final)

    match_info = df_final['MATCH_INFO'].value_counts()

    info_text.append(str(match_info))
    info_text.append(SEPERATE)
    print('Match Info')
    print(match_info)
    print(SEPERATE)

    for i in MEANINGS:
        print(i)
        info_text.append(i)
    
    info_text.append(SEPERATE)
        
    with open(OUTPUT_STRING_TXT, 'w') as f:
        for i in info_text:
            f.write(i)
            f.write('\n')
           
        f.close()
    
if __name__ == "__main__":
    main()