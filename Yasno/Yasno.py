import os
import re

import pandas as pd
from datetime import datetime, timedelta
import requests
import locale
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials

from Lib.API_Connectors import AppsFlyerConnect
from Lib.data_loader import Data_loading
from Lib.API_sources import Vkads_stats_api
from Lib.SlackReport import SlackReportClass
from config import *

# Ignore all warnings
#warnings.filterwarnings("ignore")
# Setting Russian date locale
locale.setlocale(locale.LC_TIME, 'ru_RU')
scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']



class YasnoReport:
    def __init__(self, token:str='service_credentials.json', 
                 table_dict:dict={'ads':None, 'buy':None},
                 fillepath:str=os.getcwd()+'\\', platform_souces:dict=None):
        # id for google spreadssheets
        if any(table_dict.values()) is False: raise ValueError('Не указаны URL адреса для Гугл таблиц')
        self.ads_table_id = table_dict.setdefault('ads', None) #Ads
        self.buy_table_id = table_dict.setdefault('buy', None) #Buyers

        # google table connection
        self.credentials = ServiceAccountCredentials.from_json_keyfile_name(token, scope)
        #self.table_id = table_id
        self.client = gspread.authorize(self.credentials)
        self.ads_table = self.client.open_by_key(self.ads_table_id) if self.ads_table_id is not None else None
        self.buy_table = self.client.open_by_key(self.buy_table_id) if self.buy_table_id is not None else None

        # variables for loading data
        self.api_ids = ['id1584370233', 'live.yasno.app'] # , 'live.yasno.app' - Android данные
        self.event_names = ['af_complete_registration', 'af_lead', 'af_login', 'af_purchase', 'af_re_engage']

        self.filepath = fillepath
        self.platform_sourses = platform_souces

        # service variables
        self.inup_frame = None
        self.installs = None
        self.platform_frames = None
        self.result_frame = None

    def _load_from_traker(self, load_leftowers:bool, api_load:bool):
        """Method to load data from the tracker.
            Args:
            load_leftowers (bool): Flag indicating whether to load data for previous period. Defaults to False.
            api_load (bool): Flag indicating whether to load data via API. Defaults to True.
        """
        if api_load:
            print('Используются данные по API')
            loader = AppsFlyerConnect(token=os.getcwd() + '\\token\\token_af.txt')
            events = ','.join(self.event_names)
            last_date = (datetime.today() - timedelta(days=1))
            if load_leftowers:
                first_date = last_date.replace(month=last_date.month-1, day=1).strftime("%Y-%m-%d")
            else:
                first_date = last_date.replace(day=1).strftime("%Y-%m-%d")
            last_date = last_date.strftime("%Y-%m-%d")  # Меняем тип с datetime на str
            installs_dr = loader.get_events(app_id=self.api_ids[1], 
                                            start_date=first_date, 
                                            end_date=last_date)
            installs_ios = loader.get_events(app_id=self.api_ids[0], 
                                             start_date=first_date, 
                                             end_date=last_date)
            inup_dr = loader.get_events(app_id=self.api_ids[1], 
                                        start_date=first_date, 
                                        end_date=last_date, 
                                        flag='app_events_report',
                                        events=events)
            inup_ios = loader.get_events(app_id=self.api_ids[0], 
                                         start_date=first_date, 
                                         end_date=last_date, 
                                         flag='app_events_report',
                                         events=events)
            self.inup_frame = pd.concat([inup_dr, inup_ios], ignore_index=True)
            self.installs = pd.concat([installs_dr, installs_ios], ignore_index=True) 

        else:
            self.inup_frame = pd.read_csv(self.filepath + 'inup.csv')
            self.installs = pd.read_csv(self.filepath + 'installs.csv')
            print('Данные беруться из файлов. Последнее обновление файлов: ' + str(max(self.inup_frame.iloc[:,3].max(), self.installs.iloc[:,3].max())))

        # Создаём цикл для 2 таблиц (баеров и реклов), затем меняем столбцы дату/время на дату 
        for df in [self.inup_frame, self.installs]:
            df.iloc[:, 1:4] = df.iloc[:, 1:4].astype("datetime64[ns]")
            for i in df.columns:
                if df[i].dtypes == "datetime64[ns]":
                    df[i] = pd.to_datetime(df[i], format='%m/%d/%Y').dt.date

            df = df[config_table_name]
        if api_load:
            self.inup_frame.to_csv(self.filepath + 'inup.csv', index=False)
            self.installs.to_csv(self.filepath + 'installs.csv', index=False)

    def _load_plarin(self, missing=['{{banner_id}}'], client_name='Tutu', df=None):
        """Additional function for processing Plarin data source"""
        filtered_df = df[df['Клиент'].str.contains(client_name)]
        filtered_df['Дата'] = pd.to_datetime(filtered_df['Дата'])

        pl_frame = filtered_df
        pl_frame['ID Объявления'] = pl_frame['ID Объявления'].astype(str)
        unique_dates = pl_frame['Дата'].unique()
        add_frame = pd.DataFrame([(date, ad_id) for date in unique_dates for ad_id in missing],
                                 columns=['Дата', 'ID Объявления'])
        pl_frame = pd.concat([pl_frame, add_frame], ignore_index=True)
        pl_frame['Название кампании'] = pl_frame['Название кампании'].fillna(pl_frame['Название кампании'].mode()[0])
        return pl_frame

    def _load_platforms(self):
        """Method for loading data from platform sources. Their specifications are passed
                 to the dict when the class object is created """
        self.platform_frames = {}
        for key in self.platform_sourses.keys():
            if type(self.platform_sourses[key][0][1]) is dict:
                # getting data from api if api parameters ara specified
                last_date = (datetime.today() - timedelta(days=1))
                first_date = last_date.replace(day=1).strftime("%Y-%m-%d")
                last_date = last_date.strftime("%Y-%m-%d")
                api_type, acc_ids = list(self.platform_sourses[key][0][1].items())[0]
                loader = Vkads_stats_api(acc_ids=acc_ids)
                df = loader.req_base_metrics(start_date=first_date, end_date=last_date)[['ID Объявления', 'Дата', 'Название объявления', 'ID Кампании', 'Название кампании',
                                                                                         'ID Клиента', 'Клиент', 'Показы', 'Расход', 'Клики', 'Конверсии', 'cpm', 'cpc', 'cpa',
                                                                                         'ctr', 'cr', 'vk']]
                # df = load_base_data(start_date=first_date, end_date=last_date, api_type=api_type, acc_ids=acc_ids)
                path = self.filepath + self.platform_sourses[key][0][0]
                if os.path.exists(path):
                    for file in os.listdir(path):
                        os.remove(os.path.join(path, file))
                else:
                    os.makedirs(path)
                df.to_excel(path + '\\' + f'{key}-{first_date}-{last_date}.xlsx')

            else:
                # loadind data from files
                df = Data_loading().get_data(directory=self.filepath + self.platform_sourses[key][0][0],
                                             set_dates=False, read_xlsx=True, skip=self.platform_sourses[key][0][1])
            if key == 'Plarin':
                df = self._load_plarin(df=df)
            columns = self.platform_sourses[key][2]
            df = df.rename(columns={columns[0]: 'Date', columns[1]: 'Ad ID', columns[2]: 'Spend'})
            df = df.dropna(subset=['Ad ID'])
            df = df[(df['Ad ID'] != '--') & (df['Ad ID'] != 0)]
            df['Ad ID'] = df['Ad ID'].apply(lambda x: '{:.0f}'.format(x) if isinstance(x, (int, float)) else str(x))
            df['Date'] = pd.to_datetime(df['Date'])
            df['Date'] = df['Date'].dt.strftime('%d.%m.%Y')
            self.platform_frames[key] = df

    def _merge_data(self):
        """Method for combining data from traket and platform sources"""
        for key in self.platform_frames.keys():
            merge = self.platform_sourses[key][3]
            pl_frame = self.platform_frames[key]
            # calculate installs
            temp_inst_frame = self.installs[self.installs[merge].isin(pl_frame[merge].unique())]
            inst_grouped = temp_inst_frame[['Install Time', merge, 'Event Name']].groupby(
                ['Install Time', merge]).count().reset_index()

            merged_df = pd.merge(pl_frame, inst_grouped, left_on=['Date', merge], right_on=['Install Time', merge],
                                 how='outer')
            merged_df['Date'] = merged_df['Date'].fillna(merged_df['Install Time'])
            merged_df.drop('Install Time', axis=1, inplace=True)
            # calculate events by install date
            temp_inup_frame = self.inup_frame[self.inup_frame[merge].isin(pl_frame[merge].unique())]
            temp_inup_frame = temp_inup_frame[temp_inup_frame['Install Time'].isin(pl_frame['Date'].unique())]
            inups_by_inst = temp_inup_frame.pivot_table(index=['Install Time', merge], columns='Event Name',
                                                        aggfunc='size', fill_value=0).reset_index()

            inups_by_inst.rename(columns={self.event_names[0]: self.event_names[0] + ' by inst date',
                                          self.event_names[1]: self.event_names[1] + ' by inst date',
                                          self.event_names[2]: self.event_names[2] + ' by inst date'}, inplace=True)

            if self.event_names[2] + ' by inst date' not in inups_by_inst.columns:
                inups_by_inst[self.event_names[2] + ' by inst date'] = 0

            merged_df = pd.merge(merged_df, inups_by_inst, left_on=['Date', merge], right_on=['Install Time', merge],
                                 how='left')
            merged_df['Date'] = merged_df['Date'].fillna(merged_df['Install Time'])
            merged_df.drop('Install Time', axis=1, inplace=True)

            # calculate events by event date
            inups_by_ev = temp_inup_frame.pivot_table(index=['Event Time', merge], columns='Event Name', aggfunc='size',
                                                      fill_value=0).reset_index()

            inups_by_ev.rename(columns={self.event_names[0]: self.event_names[0] + ' by event date',
                                        self.event_names[1]: self.event_names[1] + ' by event date',
                                        self.event_names[2]: self.event_names[2] + ' by event date'}, inplace=True)
            merged_df = pd.merge(merged_df, inups_by_ev, left_on=['Date', merge], right_on=['Event Time', merge],
                                 how='outer')
            merged_df['Date'] = merged_df['Date'].fillna(merged_df['Event Time'])
            merged_df.drop('Event Time', axis=1, inplace=True)


            merged_df.fillna(0, inplace=True)
            merged_df.to_excel(self.filepath + 'totest.xlsx')
            self.platform_frames[key] = merged_df

# {'ads_table':[], 'buy_table':[]}
# ads_table ['Plarin', 'AF_inst', 'AF_events']
# buy_table ['Plarin', 'AF_Installs', 'AF_Events']

    def _update_report(self, clear_mode=True):
        """Method for loading results into Google Spreadsheet"""
        for key in self.platform_frames.keys():
            for worksheet_type, worksheet_names in self.platform_sourses[key][4].items():
                table = self.buy_table if worksheet_type == 'buy_table' else self.ads_table
                for worksheet_name in worksheet_names:
                    try:
                        assert any(worksheet.title == worksheet_name for worksheet in table.worksheets())
                    except AssertionError as e:
                        print("Не найдено таблицы с таким названием: " + str(e))
                    # if sheet_exists:
                    #     pass
                    # else:
                    #     new_sheet = table.add_worksheet(title=worksheet_name, rows="1000", cols="50")
                    worksheet = table.worksheet(title=worksheet_name)
                    if worksheet_name == worksheet_names[0]:
                        frame = self.platform_frames[key]
                        frame = frame[['Ad ID', 'Date'] + [col for col in frame.columns if col not in ['Ad ID', 'Date']]]
                        data = pd.DataFrame(columns=frame.columns.values.tolist(), data=frame.values.tolist())
                        data = data[config_plarin_name]
                        data['Date'] = pd.to_datetime(frame['Date'].astype(str), format='%d.%m.%Y').astype(str)
                        data.insert(4, "Источник", 'myTarget')
                        data.insert(10, 'Конверсии по показам', 0)
                        data.insert(15, 'wcr (%)', 0)
                        data.insert(data.shape[1], 'epc', 0) 
                        data.insert(data.shape[1], 'epm', 0) 
                        data.insert(data.shape[1], 'ecpc', 0) 
                        data.insert(data.shape[1], 'ecpm', data['cpm'])
                        set_with_dataframe(worksheet, data)
                        # worksheet.update('A1', [data.columns.values.tolist()]+data.values.tolist(), value_input_option='USER_ENTERED')
                    else:
                        # additionaly loading events to events sheet
                        worksheet = table.worksheet(title=worksheet_name)
                        frame = self.inup_frame.fillna(0) if worksheet_name.lower() == 'af_events' else self.installs.fillna(0)
                        data = pd.DataFrame(columns=frame.columns.values.tolist(), data=frame.values.tolist())[config_table_name]
                        if worksheet_name.lower() == 'af_events':
                            data = data.groupby(['Event Name', 'AppsFlyer ID'], as_index=False).nth(0)[config_table_name]
                        set_with_dataframe(worksheet, data)
                    print(f"Лист {worksheet_name} для {'баеров' if worksheet_type == 'buy_table' else 'реклов'} обновлён!")
                if worksheet_type == 'buy_table':
                    text = table.title
                    patern = re.search(r"\d{2}.\d{2}.\d{4}", text)[0]
                    text = text.replace(patern, datetime.today().strftime('%d.%m.%Y'))
                    table.update_title(text)

    def update_report(self, api_load=True, load_leftowers=False):
        """General method for report updating process
              Args:
              load_leftowers (bool): Flag indicating whether to load data for previous period. Defaults to False.
              api_load (bool): Flag indicating whether to load data via API. Defaults to True.
         """
        self._load_from_traker(api_load=api_load, load_leftowers=load_leftowers)
        self._load_platforms()
        self._merge_data()
        self._update_report()

    def send_slack_message(self, webhook_url, message, hashtag, email_ping:list, table_buy, table_rekl):
        payload = {
            "message": message,
            "hashtag": hashtag,
            "table_buy": table_buy,
            "table_rekl": table_rekl
        }
        for i, email in enumerate(email_ping, start=1):
            payload["email_ping"+str(i)] = email
        response = requests.post(webhook_url, json=payload)
        if response.status_code == 200:
            print('Уведомление отправленно в Slack!')
        else:
            print('Проблема с сообщением в Slack! ' + response.text)

        
    def test(self):
        import time
        first_date = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
        last_date = (datetime.today()).strftime("%Y-%m-%d")
        url = "https://ads.vk.com/api/v2/statistics/banners/day.json"
        access_token = "Ox7C4c81lZg8HgUF6w1q8CtdQYcyU18UvVoD8FPG2UyE0BCttlx8CmHKiwHwWLZX9GelHboonCX1fWLV7wzTJfgo5Ckj2A7RsKZ1Re7NtpPlGtLl1VZ97WrW04SKEXDApYm9VrRUDHLW2yQ4i1WSV7PvvazpKeNR7I3F8GhGxLjJ9LhFpKAqCoix5kizQCIZdjCzqEDask74kivtjrBAljsMslx49zmRR5C"
        headers = {"Authorization": f"Bearer {access_token}"}
        params = {
            # 'attribution': 'conversion',
            # 'conversion_type': 'postview',
            'date_from': first_date,
            'date_to': last_date,
            'metrics': 'all',
        }
        response = requests.get(url, params=params, headers=headers)
        print(response.status_code, response.text, sep='\n')
        while response.status_code >= 500:
            time.sleep(60)
            response = requests.get(url, params=params, headers=headers, timeout=50)
            print(response.status_code, response.text, sep='\n')
        input()


if __name__ == '__main__':
    platform = {'Yasno': # Наименование ключа
                    [['plarin_api', # Наименование папки для загрузки файлов на комп
                        {'vkads_base': # Не меняется
                            ['19573536','19573539']}], # ID объявлений в Plarin
                    'Plarin', # Наименование листа в Гугле
                    ['Дата', 'ID Объявления', 'Расход'], # Переименовывает колонки в ('Date', 'Ad ID', 'Spend') если в "Наименование папки" написано Plarin
                    'Ad ID', # Столбец по которому надо мержить данные в Plarin
                    {'ads_table': # Таблица реклов (Не изменяется название)
                        ['Plarin', 'AF_inst', 'AF_events'], # Название листов, куда нужно загрузить данные (Первый лист идёт выгрузка из Plarin)
                    'buy_table': # Таблица баеров (Не изменяется название)
                        ['Plarin', 'AF_Installs', 'AF_Events'] # Название листов, куда нужно загрузить данные (Первый лист идёт выгрузка из Plarin)
                    }]
                }
    table_dict = {'buy':'1SkOmI-1Anpx4huOmpnhjtLEJK9aJgiDUu7kywYuf4us',
                'ads':'1EExnwGBeX3cW1N81XbryKg7IZiQZPe20s8bJ4NRt9vM'}
    Yasno = YasnoReport(platform_souces=platform, table_dict=table_dict)
    Yasno.update_report(api_load=True)
    # Yasno.test()
    webhook_url = "https://hooks.slack.com/triggers/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"
    hashtag = "#Yasno"
    message = "Обновил " + str(datetime.today().strftime('%d.%m'))
    email_ping = ["artem.pogosyan@huntermob.com", "olesya@huntermob.com", "veronika.kirichuk@huntermob.com"]
    table_buy = f"https://docs.google.com/spreadsheets/d/{Yasno.buy_table_id} Баер"
    table_rekl = f"https://docs.google.com/spreadsheets/d/{Yasno.ads_table_id} Рекл"
    if not input("Нажать Enter для отправки уведомления в Слак "):
        slack = SlackReportClass(webhook_url, message, hashtag, email_ping, table_buy, table_rekl)
        slack.send_slack_message()
