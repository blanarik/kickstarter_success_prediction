import pandas as pd
from bs4 import BeautifulSoup
from google.cloud import translate
from time import sleep
from datetime import datetime
from ast import literal_eval


class Translator:

    """
    With this class, you are able to extract text from scraped website
    and detect it's language based on first and last 100 characters.

    Usage:

        translator = Translator(file_name)

        # extract text from HTML
        # use this only for first run
        translator.extract_text()

        # detect language
        # only 2 millions characters can be processed daily
        translator.detect_language()

        # save partly language detected texts
        translator.save(file_name)

    """

    def __init__(self, f_name):
        self.df = pd.read_csv(f_name, encoding='latin', index_col=0)
        print('Number of loaded observations: ' + str(self.df.shape[0]))

    def extract_text(self, out_col='text'):
        self.df[out_col] = self.df['db_description_full'].apply(lambda x: BeautifulSoup(x, 'html5lib').get_text())

    def detect_language(self, key_path='private/key.json', in_col='text', verbose=True):
        print(datetime.now())
        self.df[in_col].fillna(' ', inplace=True)
        client = translate.Client.from_service_account_json(key_path)
        for i in range(self.df.shape[0]):
            if 'lang_1' in self.df:
                if not pd.isna(self.df.loc[i, 'lang_1']):
                    continue

            detected = False
            attempts = 0
            sleep_time = 0.05
            while not detected:
                try:
                    response = client.detect_language([self.df.loc[i, in_col][:100], self.df.loc[i, in_col][-100:]])
                    if len(response) != 2:
                        print('ERROR: Response contains ' + str(len(response)) + ' parts (text #' + str(i) + ')')
                        return

                    r_dict = literal_eval(str(response[0]))
                    self.df.loc[i, 'lang_1'] = r_dict['language']
                    self.df.loc[i, 'conf_1'] = r_dict['confidence']

                    r_dict = literal_eval(str(response[1]))
                    self.df.loc[i, 'lang_2'] = r_dict['language']
                    self.df.loc[i, 'conf_2'] = r_dict['confidence']

                    if i % 100 == 99 and verbose:
                        print('Number of translated texts so far: ' + str(i + 1))
                    detected = True
                    sleep(sleep_time)
                except:
                    print('Exception occurred while detecting language #: ' + str(i)) + '  -> Sleep(' + str(sleep_time) + ')'
                    attempts += 1
                    sleep(sleep_time)
                    if attempts == 1:
                        sleep_time = 3
                    if attempts > 2:
                        sleep_time *= 2
                    if attempts == 10:
                        client = translate.Client.from_service_account_json(key_path)
                        sleep_time = 2
                    if attempts == 13:
                        print('Limit exceeded')
                        return

        if verbose:
            print('Language detection completed')
        print(datetime.now())

    def save(self, f_name):
        self.df.to_csv(f_name, encoding='utf-8')

