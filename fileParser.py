import os
import re
import pandas as pd
import histogram as hg
import stats as st
import yaml


def set_root(dir):
    os.chdir(dir)
    cwd = os.getcwd()
    return cwd


def parse_dir(root_dir, fields_list, exclusion_list, sorting_order):
    counter = 0
    for root, dirs, files in os.walk(root_dir, topdown=True):
        dirs = [d for d in dirs if re.match(r'0\d - ', d)]
        folders = dirs
        for dir in folders:
            counter += 1
            os.chdir(os.path.join(root, dir))
            cd = os.getcwd()
            print('Dir:' + str(counter))
            print(cd)
            contents = os.listdir(cd)
            if not os.path.exists(cd + '\Reporting'):
                os.mkdir(cd + '\Reporting')
            folder_path = cd + '\Reporting\\'
            files = [f for f in contents if re.match(r'aggregate', f)]
            aggfiles = []
            for file in files:
                print('Reading file: ' + str(file))
                inputFile = pd.DataFrame(pd.read_csv(file))
                aggfiles.append(inputFile)
            aggfile = ''
            if len(aggfiles) > 1:
                aggfile = st.combine_data(aggfiles)
            else:
                aggfile = aggfiles[0]
            # MODIFYING DATAFRAME
            print ('Removing unwanted fields')
            st.unwanted_fields(aggfile)
            # STATS
            print('Creating stats')
            aggfile = st.generate_stats(aggfile, folder_path, fields_list)
            # FILTER
            print('Filtering data')
            aggfile = st.filter_agg_stats(aggfile, fields_list, exclusion_list)
            cols = aggfile.columns.tolist()
            while cols[0] != 'label':
                cols = cols[-1:] + cols[:-1]
            aggfile = aggfile[cols]
            aggfile['label'] = pd.Categorical(aggfile['label'], sorting_order, ordered=True)
            aggfile = aggfile.sort_values('label')
            aggfile.to_csv(folder_path + '\stats.csv', index=False)
            if 'median' in fields_list and 'samples' in fields_list and '90th' in fields_list and '95th' in fields_list:
                # HISTOGRAM
                print('Creating histogram')
                labelword = aggfile['label']
                ninetyline = aggfile['90th']
                ninetyfiveline = aggfile['95th']
                labelword = labelword.tolist()
                hg.histogram(labelword, ninetyline, ninetyfiveline, folder_path)
            else:
                print('Histogram could not be generated because the required values weren\'t requested')


if __name__ == "__main__":
    doc = yaml.load(open('fields', 'r'))
    rd = set_root(doc['directory'])
    fields = doc['fields']
    exclusions = doc['exclusions']
    sorting = doc['sorting order']
    parse_dir(rd, fields,exclusions, sorting)
