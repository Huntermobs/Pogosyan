import base64
import hashlib
import json
import time
import os

import pandas as pd
import requests
from io import StringIO, BytesIO


class AppsFlyerConnect:

    def __init__(self, token=os.getcwd() + '\\token\\token_af.txt'):
        with open(token, "r", encoding='utf8') as f:
            token = f.readline()
        self.headers = {"accept": "text/csv",
                        "authorization": token}
        self.base_url = 'https://hq1.appsflyer.com/api/raw-data/export/app/'
        self.timezone = 'Europe%2FMoscow'
        self.additional_fields = ('blocked_reason_rule,store_reinstall,impressions,contributor3_match_type,custom_dimension,'
                                'conversion_type,gp_click_time,match_type,mediation_network,oaid,deeplink_url,'
                                'blocked_reason,blocked_sub_reason,gp_broadcast_referrer,gp_install_begin,campaign_type,'
                                'custom_data,rejected_reason,device_download_time,keyword_match_type,'
                                'contributor1_match_type,contributor2_match_type,device_model,monetization_network,'
                                'segment,is_lat,gp_referrer,blocked_reason_value,store_product_page,device_category,'
                                'app_type,rejected_reason_value,ad_unit,keyword_id,placement,network_account_id,'
                                'install_app_store,amazon_aid,att')

    def get_events(self, app_id='', start_date='2024-01-01', end_date='2024-01-16', flag='installs', events=None):
        match flag:
            case 'installs': 
                url = (f'{self.base_url}{app_id}/installs_report/v5?timezone={self.timezone}&from={start_date}&to={end_date}'
                    f'&additional_fields={self.additional_fields}')
            case 'app_events_report': 
                url = (f'{self.base_url}{app_id}/in_app_events_report/v5?timezone={self.timezone}&from={start_date}&to={end_date}&'
                    f'event_name={events}&additional_fields={self.additional_fields}')
            case 'app_events_retarget': 
                url = (f'{self.base_url}{app_id}/in-app-events-retarget/v5?timezone={self.timezone}&maximum_rows=1000000&currency=RUB&from={start_date}&to={end_date}&'
                    f'event_name={events}&additional_fields={self.additional_fields}')
            case _: 
                raise ValueError ('Не установлен корректный флаг. Выберите один из следующих: ' + ', '.join(['installs', 'app_events_report', 'app_events_retarget']))
        response = requests.get(url, headers=self.headers)
        csv_file_like_object = StringIO(response.text)
        df = pd.read_csv(csv_file_like_object)
        return df


# test=AppsFlyerConnect()
# test.get_installs(app_id='id597405601')
# test.get_inup_events(app_id='id597405601',events='s2s-conversion,s2s-cpa-conversion')

class IronsorceConnector:
    def __init__(self, token='token_is.txt'):
        with open(token, "r", encoding='utf8') as f:
            token = f.readline()
            print(token)
        self.headers = {"Authorization": 'Basic ' + base64.b64encode(token.encode('utf-8')).decode('utf-8'),
                        # 'Content-Type': 'application/json'
                        }
        # self.base_url = 'https://hq1.appsflyer.com/api/raw-data/export/app/'
        # self.timezone='Europe%2FMoscow'

    def create_audience(self, type='targeting', name='', description=''):
        url = 'https://platform-api.supersonic.com/audience/api/create'
        # Отправка запроса

        body = {
            "type": type,
            "name": name,
            "description": description
        }
        response = requests.post(url, headers=self.headers, data=body)

        # Печать результата
        print(response.text)
        with open(name + '.txt', "w", encoding='utf8') as f:
            f.write(response.text)

    def show_audience(self, id=''):
        url = 'https://platform-api.supersonic.com/audience/api/show'

        response = requests.get(url, headers=self.headers)

        print(response.content)

    def fill_audience(self, name='', file='list.csv'):
        url = ' https://platform-api.supersonic.com/audience/api'
        frame = pd.read_csv(file)
        records = 900
        list_of_lists = [frame.iloc[i:i + records]['id'].tolist()
                         for i in range(0, len(frame), records)]
        with open(name + '.txt', "r", encoding='utf8') as f:
            id = json.load(f)
            id = id['id']
        i = 0
        for part in list_of_lists:
            body = {
                "addAudience": [id],
                "deviceIds": part,
                "removeAudience": []
            }
            response = requests.post(url, headers=self.headers, data=body)
            if response.status_code == 413:
                raise ValueError
            print(response.content)
            i = i + 1
            print(i)

# test=IronsorceConnector()
# description='Аудитория- органические пользователи, которые пользовались в основном вертикалью КБ. Оттекли за 2023г.'
# # test.create_audience(name='edadeal - organicCB2023',description=description)
# test.show_audience()
# test.fill_audience(name='edadeal - organicCB2023',file='churned_target_users_md5.csv')

class MintegralConnector:
    def __init__(self, token='token_mint.txt'):
        with open(token, "r", encoding='utf8') as f:
            asseskey = f.readline()[:-1]
            apikey = f.readline()
            timestamp = str(int(time.time()))
            print(apikey)
        self.headers = {'access-key': asseskey,
                        'token': hashlib.md5(
                            (apikey + hashlib.md5(timestamp.encode('utf-8')).hexdigest()).encode('utf-8')).hexdigest(),
                        'timestamp': timestamp}
        self.base_url = 'https://ss-api.mintegral.com/api/v2/'
        self.timezone = '+3'

    def collect_creatives_data(self, start_date='2024-01-08', end_date='2024-01-14', dimension='Offer,Campaign'):

        url = self.base_url + 'reports/data'
        params = {
            "start_time": start_date,
            "end_time": end_date,
            "timezone": self.timezone,
            "type": 1,
            "dimension_option": dimension
        }
        response = requests.get(url, params=params, headers=self.headers).json()

        while response['code'] != 200:
            if response['code'] == 202:
                time.sleep(30)
            else:
                time.sleep(10)
            response = requests.get(url, params=params, headers=self.headers).json()

        params["type"] = 2
        response = requests.get(url, params=params, headers=self.headers)
        data_bytes = BytesIO(response.content)

        # Чтение данных в DataFrame
        df = pd.read_csv(data_bytes, delimiter='\t')
        print(df)


# probe=MintegralConnector()
# probe.collect_creatives_data()


class AppMetricaConnect:
    def __init__(self, token='token_am.txt'):
        with open(token, "r", encoding='utf8') as f:
            token = f.readline()
            print(token)
        self.headers = {"accept": "*/&",
                        "Authorization": token}
        self.base_url = 'https://api.appmetrica.yandex.ru/'

    def get_ua_data(self, app_id='', start_date='2024-01-01', end_date='2024-01-14',
                    event_name='[["hotels.happyPage.success"]]',source= 'installation',
                    dimension="urlParameter{'creative_id'},urlParameter{'campaign_id'}", metrics=None):

        # url=f'{self.base_url}stat/v1/data'
        if metrics is None:
            metrics='impressions,clicks,devices,deeplinks,conversion,sessions,event1Count'

        url = "https://api.appmetrica.yandex.ru/v2/user/acquisition"
        params = {
            'lang': 'ru',
            'request_domain': 'ru',
            'id': app_id,
            'date1': start_date,
            'date2': end_date,
            'filters': '',
            'accuracy': 1,
            'dimensions': f"{dimension},date",
            'sort': '-devices',
            'eventNames': event_name,
            'limit': 100000,
            'offset': 1,
            'source': source,
            'proposedAccuracy': True,
            'metrics': metrics,
            'group': 'day'
        }
        response = requests.get(url, params=params, headers=self.headers)
        data = response.json()

        data_list = data.get("data", [])

        rows = []

        for entry in data_list:
            dimensions = entry.get("dimensions", [])
            metrics = entry.get("metrics", [])

            row_dict = {}

            row_dict["ID Объявления"] = dimensions[0]["name"]
            row_dict["Campaign id"] = dimensions[1]["name"]
            row_dict["Date"] = dimensions[2]["name"]

            for i, metric_value in enumerate(metrics):
                metric_name = data["metrics"][i]
                row_dict[metric_name] = metric_value

            rows.append(row_dict)

        df = pd.DataFrame(rows)
        # df.to_excel('test.xlsx')
        return df

    def make_api_request(self):
        url = "https://appmetrica.yandex.ru/statistic"
        params = {
            'group': 'day',
            'appId': '4327126',
            'report': 'skadnetwork',
            'filters': '{"values": []}',
            'metrics': '[[ "installationsAll" ], [ "installationsNew" ], [ "reinstallations" ], [ "targetActions" ]]',
            'sampling': '1',
            'chartType': 'multiline',
            'sort': '-["installationsAll"]',
            'tableView': 'multilevel',
            'chartColumn': '["installationsAll"]',
            'currency': 'rub',
            'dimensions': '[[ "adNetworkAdID" ], [ "eventDate" ]]',
            'period': '2024-03-01:2024-03-11',
            'accuracy': 'absolute'
        }
        try:
            response = requests.get(url, params=params, headers=self.headers)
            response.raise_for_status()  # Raises an HTTPError for bad responses (4xx or 5xx)
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error: {e}")
            return None


# test = AppMetricaConnect()
# test.get_ua_data(app_id='4066306')
# test.make_api_request()

# resp=requests.get('https://oauth.yandex.ru/authorize?response_type=token&client_id=<идентификатор приложения>965edda7adf749e6a4d5eebb96e8b524')
# print(resp.text)


class AdjustConnect:
    def __init__(self, token='token_adj.txt', account_id='742', app_ids=['idfmu3xkpqtc', 'sdf20eu3hibk']):
        with open(token, "r", encoding='utf8') as f:
            token = f.readline()
        self.app_ids = app_ids
        self.headers = {
            # "Authorization": "Bearer " + token
            "Authorization": f"Bearer {token}",
            "X-Account-ID": account_id
        }
        self.base_url = 'https://dash.adjust.com/control-center/reports-service/'

    def get_cohort_data(self, start_date:str, end_date:str,
                        event_names=['lessy_03_discountcardview'], cohort='d0'):

        # url=f'{self.base_url}stat/v1/data'
        url = self.base_url + 'csv_report'

        params = {
            'cost_mode': 'network',
            'app_token__in': ','.join([(value) for value in self.app_ids]),
            'date_period': f'{start_date}:{end_date}',
            'attribution_type': 'all',
            'cohort_maturity': 'immature',
            'dimensions': 'day,partner_name,adgroup,campaign,creative_id_network',
            'metrics': 'installs,'+','.join([value + ('_'+cohort+'_events_cohort') for value in event_names]),
            'reattributed': 'all',
            'sandbox': 'false',
            'sort': '-installs',
            'utc_offset': '+03:00',

        }
        response = requests.get(url, params=params, headers=self.headers)
        data = response.text



        df = pd.read_csv(StringIO(data))

        # df.to_excel('D:\\работа\\huntermob\\Yrealty\\test.xlsx')
        return df

    def get_stats_data(self, start_date='2024-02-01', end_date='2024-02-27',
                        event_names=['Zvonok_Novostroika_paid', 'Card_View_Paid_Newb_sell',]):

        # url=f'{self.base_url}stat/v1/data'
        url = self.base_url + 'csv_report'

        params = {
            'cost_mode': 'network',
            'app_token__in': ','.join([(value) for value in self.app_ids]),
            'date_period': f'{start_date}:{end_date}',
            'attribution_type': 'all',
            'cohort_maturity': 'immature',
            'dimensions': 'day,app_token,campaign_id_network,adgroup_id_network,creative_id_network',
            'metrics': 'installs,'+','.join([value+'_events' for value in event_names]),
            # 'metrics': 'installs',
            'reattributed': 'all',
            'sandbox': 'false',
            'sort': '-installs',
            'utc_offset': '+00:00',
        }
        response = requests.get(url, params=params, headers=self.headers)
        data = response.text



        df = pd.read_csv(StringIO(data))

        # df.to_excel('D:\\работа\\huntermob\\Yrealty\\test.xlsx')
        return df

    def get(self):
        url = 'https://dash.adjust.com/control-center/reports-service/filters_data'
        response = requests.get(url,params={'required_filters':'apps'},headers=self.headers)
        print(response)


# test = AdjustConnect(app_ids=['nh5z5z1uy3gg'],account_id='14575')
# test.get_cohort_data()
# test.get()
# test.get_kpi_data()