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

OUTPUT_STRING_XLSX = OUTPUT_PATH + project_name + '_' + name_output + '_' + DATE_STRING + '.xlsx'
OUTPUT_STRING_CSV =  OUTPUT_PATH + project_name + '_' + name_output + '_' + DATE_STRING + '.csv'

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

    # mapper
mapper = {
    0: 'TWO_REV_MATCH_excluded',
    2: 'TWO_REV_MATCH_included',
    1: 'TWO_REV_DIFF',
    -98: 'ONE_REV',
    -99: 'ONE_REV',
    -198: 'NO_REV',
}
######################################################################################
# functions and main 

def load_dataframes():
    print('_________________________')
    print('...loading dataframes...' )
    print('')
    input_files = os.listdir(INPUT_PATH)
    input_file = []

    for filename in input_files:
        input_file.append(INPUT_PATH + filename)

    read_main = pd.read_excel(input_file[0])
    read_merge = pd.read_excel(input_file[1])
    
    df_main = pd.DataFrame(read_main, columns=columns_main_dataframe_one)
    print('loaded main dataframe. Shape: ', df_main.shape)
    df_merge = pd.DataFrame(read_merge, columns=columns_merge_dataframe_one)
    print('loaded merge dataframe . Shape: ', df_merge.shape)
    print('_________________________')

    return df_main, df_merge

def merge_dataframes(df_main, df_merge):
    # merge
    df = df_main.merge(df_merge, left_on=merge_on, right_on=merge_on )
    df = df.loc[:, columns_final_dataframe_one]
    df = df.sort_values(sort_by, ascending=True)
    df = df.fillna(-99)
    # compare and recode
    df['MATCH_INFO'] = df['included_x'] + df['included_y']
    df['MATCH_INFO'] = [mapper[i] for i in df['MATCH_INFO']]
    df_final = df

    print('dataframes merged. Shape: ', df_final.shape)
    print('_________________________')

    return df_final

def save_dataframe(df):
    df.to_csv(OUTPUT_STRING_CSV, sep=',', header=True)
    print('saved dataframe: ', OUTPUT_STRING_CSV)
    df.to_excel(OUTPUT_STRING_XLSX, header=True)
    print('saved dataframe: ', OUTPUT_STRING_XLSX)
    print('_________________________')

def main():
    df_main, df_merge = load_dataframes()
    df_final = merge_dataframes(df_main, df_merge)
    save_dataframe(df_final)

    match_info = df_final['MATCH_INFO'].value_counts()
    print('Match Info')
    print(match_info)
    print('_________________________')
    print('Meanings')
    print('NO_REV = not reviewed')
    print('ONE_REV =  reviewed by one reviewer')
    print('TWO_REV_MATCH_included = included Reviewers match')
    print('TWO_REV_MATCH_excluded = excluded Reviewers match')
    print('TWO_REV_DIFF = Rating differs')
    


if __name__ == "__main__":
    main()