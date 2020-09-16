import ssl, os, pandas as pd

'''
table structure for get_latest_data() and get_all_data() will be below
CREATE TABLE `covid_data` (
  `county` varchar(50) DEFAULT NULL,
  `state` varchar(2) DEFAULT NULL,
  `date` date DEFAULT NULL,
  `confirmed_cases` int(11) DEFAULT NULL,
  `confirmed_deaths` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


table  structure for  get_last_60day_data will be different.

'''
def quotify(str):
    return '"'+str+'"'


def get_latest_data(df):
    newdf = df.iloc[:, [1, 2, -1]]
    string = 'Insert into covid_data (county, state, date, confirmed_cases) values'

    for row in range(newdf.shape[0]):
        if newdf.iloc[row, 0].strip() == 'Statewide Unallocated':
            pass
        else:
            string = string + '(' + quotify(newdf.iloc[row, 0]) + ',' + \
                     quotify(newdf.iloc[row, 1]) + ',' + to_date(newdf.columns[-1]) + ',' +\
                     str(newdf.iloc[row, 2]) + '),'

    print(string[:-1])


def to_date(ele):
    return "STR_TO_DATE(" + quotify(ele) + ',' + quotify('%m/%d/%y') + ')'


def get_all_data(df):
    print('inside get all data')
    print(df.shape[0])
    with open('covid_data_inserts.sql','w') as f:
        for row in range(df.shape[0]):
            print(row)
            if df.loc[row][1] =='Statewide Unallocated':
                pass
            else:
                insert = '''insert into covid_data(county, state, date, confirmed_cases) values (''' + \
                          quotify(df.loc[row,'County Name']) +"," +quotify(df.loc[row,'State']) + ","

                for ele in df.columns[4:]:
                    insert2 = insert + to_date(ele) + ',' + str(df.loc[row, ele]) + ");\n"
                    f.write(insert2)

    print('Done!!')


def get_last_60day_data(df):
    print('Inside last  60 day data function')
    col_list = list(df.columns[:4])+list(df.columns[-60:])
    df = df[col_list]


    with open('covid_confirmed_usafacts_last60days.sql','w') as f:

        insert_string = '''insert into  covid_data values ({}),'''.format(','.join(quotify(ele) for ele in col_list))

        for row in range(df.shape[0]):
            if df.loc[row][1] == 'Statewide Unallocated':
                pass
            else:
                braces_start = "("
                insert1 = ''
                braces_end = "),"
                for ele in col_list:
                    insert1 = insert1 + quotify(str(df.loc[row,ele])) + ','
                insert1  = braces_start + insert1[:-1] + braces_end + '\n'
                insert_string = insert_string + insert1
        f.write(insert_string[:-2])
    print('Done!!!')


def main():
    if (not os.environ.get('PYTHONHTTPSVERIFY', '') and getattr(ssl, '_create_unverified_context', None)):
        print('Inside if')
        # if not given we will get SSL: CERTIFICATE_VERIFY_FAILED
        ssl._create_default_https_context = ssl._create_unverified_context

    print("reading the data from the file online")
    df = pd.read_csv("https://usafactsstatic.blob.core.windows.net/public/data/covid-19/covid_confirmed_usafacts.csv")
    # df = pd.read_csv('covid_confirmed_usafacts.csv')
    print('##### Create insert scripts####')

    # get_latest_data(df)
    # get_all_data(df)
    get_last_60day_data(df)


if __name__ == '__main__':
    main()