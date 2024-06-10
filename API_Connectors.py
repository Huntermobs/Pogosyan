import base64
import hashlib
import json
import time
import os
import logging

import pandas as pd
from datetime import datetime, timedelta
import requests
from google.oauth2 import service_account
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
)
from io import StringIO, BytesIO

# Configure logging
# logging.basicConfig(filename="log.txt", filemode="a", level=logging.INFO, datefmt='%m.%d.%Y %H:%M:%S', format='%(asctime)s - %(levelname)s - %(message)s')


class AppsFlyerConnect:

    def __init__(self, token=os.path.join(os.getcwd(), 'token', 'token_af.txt')):
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

    def get_events(self, app_id, start_date, end_date, flag='installs', events=None, dtype:list=None):
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
        df = pd.read_csv(csv_file_like_object, dtype={i: object for i in dtype})
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
    def __init__(self, token=os.path.join(os.getcwd(), 'token', 'token_mint.txt')):
        with open(token, "r", encoding='utf8') as f:
            apikey = f.readline()[:-1]
            asseskey = f.readline()
        timestamp = str(int(datetime.timestamp(datetime.today())))
        timestamp_hash = hashlib.md5(timestamp.encode('utf-8')).hexdigest()
        self.headers = {'access-key': asseskey,
                        'token': hashlib.md5((apikey + timestamp_hash).encode('utf-8')).hexdigest(),
                        'timestamp': timestamp}
        self.base_url = 'https://ss-api.mintegral.com/api/v2/'
        self.timezone = '+3'

    def collect_creatives_data(self, start_date, end_date, dimension='Offer,Campaign'):
        url = self.base_url + 'reports/data'
        params = {
            "timezone": self.timezone,
            "dimension_option": dimension
        }
        temp_end_date = end_date
        total = None
        for i in range((((datetime.strptime(end_date, "%Y-%m-%d") - datetime.strptime(start_date, "%Y-%m-%d")).days + 1) // 7) + 1):
            start_date = (datetime.strptime(start_date, "%Y-%m-%d") + timedelta(i * 7)).strftime("%Y-%m-%d")
            if (datetime.strptime(end_date, "%Y-%m-%d") - datetime.strptime(start_date, "%Y-%m-%d")).days > 7:
                temp_end_date = (datetime.strptime(start_date, "%Y-%m-%d") + timedelta(6)).strftime("%Y-%m-%d")
            else:
                temp_end_date = end_date
            params["type"] = 1
            params['start_time'] = start_date
            params['end_time'] = temp_end_date
            response = requests.get(url, params=params, headers=self.headers).json()
            while True:
                match response['code']:
                    case 400:
                        logging.info(f"Произошла ошибка авторизации\nJSON-код ответа сервера: {response}")
                        break
                    case 201 | 202:
                        logging.info("Повторная отправка запроса через 30 секунд")
                        time.sleep(30)
                        response = requests.get(url, params=params, headers=self.headers).json()
                    case 200:
                        logging.info(f'Данные загружены из Mintegral за {start_date}:{temp_end_date}')
                        params["type"] = 2
                        response = requests.get(url, params=params, headers=self.headers)
                        data_bytes = BytesIO(response.content)
                        if total is None:
                            total = pd.read_csv(data_bytes, delimiter='\t')
                        else:
                            total = pd.concat([total, pd.read_csv(data_bytes, delimiter='\t')])
                        break
                    case 500:
                        logging.info(f'Сервер недоступен!')
                        break
                    case _:
                        logging.info(f"Произошла непредвиденная ошибка\nJSON-код ответа сервера: {response}")
                        break
        total['Date'] = pd.to_datetime(total['Date'], format='%Y%m%d').dt.strftime('%Y-%m-%d')
        return total
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
    def __init__(self, token='token_adj.txt', account_id='742', app_ids=None):
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


class TiktokConnect:
    '''
    Для получения токена Tiktok необходимо перейти по сслылке в менеджер приложений\n
    https://business-api.tiktok.com/portal/apps
    
    url = 'https://business-api.tiktok.com/open_api/v1.3/oauth2/access_token/'\n
    Переменные забираются по ссылке в соответствующих параметрах\n
    SECRET = '1234567890abcdef'\n
    APP_ID = '1234567890'\n
    Нужно выбрать приложение и в пункте Advertiser authorization URL\n
    Перейти по URL ссылке, в адресе которой будет лежать auth_code\n
    AUTH_CODE = '1234567890abcdef'

    После получения токена, его нужно использовать в параметре access_token
    '''
    def __init__(self, SECRET:str, APP_ID:str, AUTH_CODE:str, access_token:str=None) -> None:
        self.base_url = 'https://business-api.tiktok.com/open_api/v1.3/'
        self.secret = SECRET
        self.app_id = APP_ID
        self.auth_code = AUTH_CODE
        self.access_token = access_token if access_token is not None else self.get_token()
        

    def get_token(self):
        try:
            with open(os.path.join(os.getcwd(), 'token', 'tiktok_token.txt'), "r", encoding="utf-8") as output_file:
                return  json.load(output_file)["data"]["access_token"]
        except OSError:
            url = self.base_url + 'oauth2/access_token/'
            headers = {
                "Content-Type":"application/json"
            }
            payload = {
                "secret": self.secret, 
                "app_id": self.app_id, 
                "auth_code": self.auth_code
            }
            response = requests.post(url, headers=headers, json=payload)
            if response.status_code == 200:
                with open(os.path.join(os.getcwd(), 'token', 'tiktok_token.txt'), "w", encoding="utf-8") as output_file:
                    json.dump(response.json(), output_file)
                logging.info(f"{response.text}\nСохранено по пути {os.path.join(os.getcwd(), 'token', 'tiktok_token.txt')}")
                return response.json()["data"]["access_token"]
            else:
                raise ConnectionError(f'Ошибка!:\n{response.text}')

    def transform_data(self, df):
        lists = None
        for i in df['data']['list']:
            i['metrics']['stat_time_day'] = pd.to_datetime(i['dimensions']['stat_time_day']).date()
            if lists is not None: 
                lists.append(i['metrics'])
            else: 
                lists = [i['metrics']]
        return pd.DataFrame(lists)
         

    def get_data (self, advertiser_id:list, start_date:str, end_date:str) -> pd.DataFrame:
        if len(advertiser_id) > 5: 
            count = (len(advertiser_id) // 5) + 1
        else:
            count = 1
        self.df = []
        for co in range(count):
            url = self.base_url + 'report/integrated/get/'
            payload = {
                "advertiser_ids" : advertiser_id[co*5:(co+1)*5] ,
                "report_type" : "BASIC" ,
                "data_level": "AUCTION_AD",
                "dimensions" : [ 
                    'ad_id',
                    'stat_time_day'
                ],
                "metrics" : [
                    "advertiser_id",
                    "campaign_id",
                    "campaign_name",
                    "adgroup_id",
                    "adgroup_name",
                    "ad_id",
                    "ad_name",
                    "spend"
                ],
                "start_date" : start_date ,
                "end_date" : end_date , 
                "page" : 1 ,
                "page_size" : 1000 
            }

            headers = {
                "Access-Token": self.access_token,
                "Content-Type":"application/json"
            }

            response = requests.get(url, headers=headers, json=payload)
            self.df.append(response.json())
            self.df[co] = self.transform_data(self.df[co])
            if response.json()['data']['page_info']['total_page'] > 1:
                for i in range(2, int(response.json()['data']['page_info']['total_page'])+1):
                    payload["page"] = i
                    response_page = requests.get(url, headers=headers, json=payload)
                    data = response_page.json()
                    self.df[co] = pd.concat([self.df[co], self.transform_data(data)])
            if len(self.df) > 1:
                self.df_final = pd.concat([self.df[co], self.df[co-1]])
                if len(self.df) > 2:
                    self.df_final = pd.concat([self.df[co], self.df_final])
            else:
                self.df_final = self.df[co]
        logging.info(f"Данные с TikTok успешно загружены ({self.df_final['stat_time_day'].max()})")
        self.df = self.df_final
        return self.df

    def sample_table(self):
        self.df = self.df.rename(columns={'currency':'Currency', 'ad_name':'Ad Name', 'ad_id':'Ad ID', 'campaign_id':'Campaign ID',
        'adgroup_name':'Ad Group Name', 'spend':'Cost', 'adgroup_id':'Ad group ID', 'campaign_name':'Campaign name',
        'stat_time_day':'Date'})
        self.df = self.df[['Date', 'Ad Name', 'Ad ID', 'Ad Group Name', 'Ad group ID', 
                'Campaign name', 'Campaign ID', 'Cost', 'Currency']]
        self.df['Cost'] = self.df['Cost'].astype(float)
        self.df = self.df.loc[self.df['Cost'] > 0]
        return self.df

class YandexDirectConnect:
    '''
    Для создания перечисления собственных параметров и метрик, необходимо изменить переменную self.params\n
    Дополнительная документация для Yandex Direct есть здесь: https://yandex.ru/dev/direct/doc/reports/spec.html

    Структура параметров:
    {
        "params" : { /* ReportDefinition */
            "SelectionCriteria": { /* SelectionCriteria */
            "DateFrom": (string),
            "DateTo": (string),
            "Filter": [{ /* FilterItem*/
                "Field": ( "AdGroupId" | ... | "Year" ),  /* required */
                "Operator": ( "EQUALS" | ... | "STARTS_WITH_IGNORE_CASE" ), /* required */
                "Values": [(string), ... ] /* required */
            }, ... ]
            }, /* required */
            "Goals": [(string), ... ],
            "AttributionModels": [( "FC" | "LC" | "LSC" | "LYDC" | "FCCD" | "LSCCD" | "LYDCCD" | "AUTO" ), ... ],
            "FieldNames": [( "AdGroupId" | ... | "Year" ), ... ],  /* required */
            "Page": { /* Page*/
            "Limit": (int) /* required */
            "Offset": (int)
            }
            "OrderBy": [{ /* OrderBy*/
            "Field": ( "AdGroupId" | ... | "Year" ),  /* required */
            "SortOrder": ( "ASCENDING" | "DESCENDING" )
            }, ... ],
            "ReportName": (string),  /* required */
            "ReportType": ( "ACCOUNT_PERFORMANCE_REPORT" | ... | "SEARCH_QUERY_PERFORMANCE_REPORT" ),  /* required */
            "DateRangeType": ( "ALL_TIME" | ... | "YESTERDAY" ),  /* required */
            "Format": ( "TSV" ),  /* required */
            "IncludeVAT": ( "YES" | "NO" ),  /* required */
            "IncludeDiscount": ( "YES" | "NO" ) 
        }
    }
    '''

    def __init__(self, token=os.path.join(os.getcwd(), 'token', 'YandexToken.txt')):
        with open(token, "r", encoding='utf8') as f:
            token = f.readline()
        self.headers = {
            'Authorization': f'Bearer {token}', # Токен Авторизации
            'Accept-Language': 'en',            # Язык ответа на запрос
            'Content-Type': 'application/json', # Формат ответа
            'Client-Login': 'YangoDriveWeb',    # Логин клиента, отчёт которого нужно сформировать
            'Use-Operator-Units': 'true',       # Использовать баллы со стороны Агенства (https://yandex.ru/dev/direct/doc/dg/concepts/units.html)
            'skipReportHeader': 'true',         # Не выводить в отчете строку с названием отчета и диапазоном дат
            'skipReportSummary': 'true',        # Не выводить в отчете строку с количеством строк статистики.
            'returnMoneyInMicros': 'false'      # Если заголовок указан, денежные значения в отчете возвращаются в валюте с точностью до двух знаков после запятой. - 
                                                # Если не указан, денежные значения возвращаются в виде целых чисел — сумм в валюте, умноженных на 1 000 000.
        }
        self.base_url = 'https://api.direct.yandex.com/json/v5/reports'
        self.params = {
            "params" : {    
                "SelectionCriteria": {  # Обязательный критерий, если не нужен фильтр оставляем пустой словарь {}
                    "Filter": [{
                        "Field": "Cost",
                        "Operator": "GREATER_THAN",
                        "Values": ["0"]
                    }]
                }, 
                'FieldNames': ['Date', 'CampaignName', 'CampaignId', 'AdId', 'Impressions', 'Clicks', 'Ctr', 'Cost'],
                'DateRangeType': 'THIS_MONTH', #'LAST_MONTH' 
                'ReportType': 'CUSTOM_REPORT',
                "ReportName": f"YangoDrive_{str(datetime.today().strftime('%d_%m'))}", 
                'IncludeVAT': 'NO',
                "Format": "TSV",
            }
        }

    def encode(self, text):
            if type(text) == type(b''):
                return text.decode('utf8')
            else:
                return text

    def response_direct(self):
        while True:
            try:
                response = requests.get(self.base_url, headers=self.headers, json=self.params)
                self.response = response
                response.encoding = 'utf-8'  # Принудительная обработка ответа в кодировке UTF-8
                if response.status_code == 400:
                    logging.info("Параметры запроса указаны неверно или достигнут лимит отчетов в очереди")
                    logging.info("RequestId: {}".format(response.headers.get("RequestId", False)))
                    logging.info("JSON-код ответа сервера: \n{}".format(self.encode(response.json())))
                    break
                elif response.status_code == 200:
                    logging.info("Отчет создан успешно")
                    logging.info("RequestId: {}".format(response.headers.get("RequestId", False)))
                    # logging.info("Содержание отчета: \n{}".format(self.encode(response.text)))
                    break
                elif response.status_code == 201:
                    logging.info("Отчет успешно поставлен в очередь в режиме офлайн")
                    retryIn = int(response.headers.get("retryIn", 60))
                    logging.info("Повторная отправка запроса через {} секунд".format(retryIn))
                    logging.info("RequestId: {}".format(response.headers.get("RequestId", False)))
                    time.sleep(retryIn)
                elif response.status_code == 202:
                    logging.info("Отчет формируется в режиме офлайн")
                    retryIn = int(response.headers.get("retryIn", 60))
                    logging.info("Повторная отправка запроса через {} секунд".format(retryIn))
                    logging.info("RequestId:  {}".format(response.headers.get("RequestId", False)))
                    time.sleep(retryIn)
                elif response.status_code == 500:
                    logging.info("При формировании отчета произошла ошибка. Пожалуйста, попробуйте повторить запрос позднее")
                    logging.info("RequestId: {}".format(response.headers.get("RequestId", False)))
                    logging.info("JSON-код ответа сервера: \n{}".format(self.encode(response.json())))
                    break
                elif response.status_code == 502:
                    logging.info("Время формирования отчета превысило серверное ограничение.")
                    logging.info("Пожалуйста, попробуйте изменить параметры запроса - уменьшить период и количество запрашиваемых данных.")
                    logging.info("RequestId: {}".format(response.headers.get("RequestId", False)))
                    logging.info("JSON-код ответа сервера: \n{}".format(self.encode(response.json())))
                    break
                else:
                    logging.info("Произошла непредвиденная ошибка")
                    logging.info("RequestId:  {}".format(response.headers.get("RequestId", False)))
                    logging.info("JSON-код ответа сервера: \n{}".format(self.encode(response.json())))
                    break
            # Обработка ошибки, если не удалось соединиться с сервером API Директа
            except ConnectionError:
                # В данном случае мы рекомендуем повторить запрос позднее
                logging.info("Произошла ошибка соединения с сервером API")
                # Принудительный выход из цикла
                break

            # Если возникла какая-либо другая ошибка
            except:
                # В данном случае мы рекомендуем проанализировать действия приложения
                logging.info("Произошла непредвиденная ошибка\n" + response.status_code + '\n' + response.text)
                # Принудительный выход из цикла
                break
        return response
    
    def to_dataframe(self, response):
        string = [string.split('\t') for string in response.text.split('\n')]
        return pd.DataFrame.from_records(string[1:-1], columns=string[0])[['Date','CampaignName','CampaignId','AdId','Cost']]

class GoogleAnalyticsConnector:
    
    def __init__(self, SERVICE_ACCOUNT_FILE:str, property_id:str) -> None:
        scopes=["https://www.googleapis.com/auth/analytics.readonly"]
        credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
        self.client = BetaAnalyticsDataClient(credentials=credentials)
        self.property_id = property_id  # Администратор -> Вкладка "Ресурсы", Информация о ресурсе -> ИДЕНТИФИКАТОР РЕСУРСА

    def request_ga4(self, properties:dict):
        '''
        properties = {
            'dimensions': list
            'metrics': list
            'start_date': str  # YYYY-MM-DD, yesterday, today, или NdaysAgo
            'end_date': str    # YYYY-MM-DD, yesterday, today, или NdaysAgo
        }
        '''
        self.properties = properties
        request = RunReportRequest(
            property=f"properties/{self.property_id}",
            dimensions=[Dimension(name=i) for i in properties['dimensions']],
            metrics=[Metric(name=i) for i in properties['metrics']],
            date_ranges=[DateRange(start_date=properties['start_date'], end_date=properties['end_date'])],
        )
        self.response = self.client.run_report(request)
        return self.response

    def transform_data(self, response=None):
        if response is None: 
            response = self.response
        rows = []
        for row in response.rows:
            first = {k: row.dimension_values[i].value for i, k in enumerate(self.properties['dimensions'])}
            second = {k: row.metric_values[i].value for i, k in enumerate(self.properties['metrics'])}
            first.update(second)
            rows.append(first)
        logging.info(f"Данные с Google успешно загружены ({pd.DataFrame(rows)['date'].max()})")
        return pd.DataFrame(rows)
    

class PetalConnector:
    def __init__(self) -> None:
        self.base_url = 'https://oauth-login.cloud.huawei.com/oauth2/v2'
        self.client_id = '111142983'
        self.redirect_uri = 'https://huntermob.com/'
        
    def authorize_token(self):
        url = self.base_url + '/authorize'
        header = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        params = {
            'response_type': 'code',
            'client_id': self.client_id,
            'access_type': 'offline',   # Установите для этого параметра значение «офлайн» только в том случае, если необходимо вернуть токен обновления.
            'redirect_uri': self.redirect_uri,
            'scope': ' '.join([
                'https://www.huawei.com/auth/account/base.profile', 
                'https://ads.cloud.huawei.com/report',
                'https://ads.cloud.huawei.com/promotion', 
                'https://ads.cloud.huawei.com/tools',
                'https://ads.cloud.huawei.com/account', 
                'https://ads.cloud.huawei.com/finance'
            ])
        }
        request = requests.get(url, headers=header, params=params)
        logging.warning('Для получения токена авторизации, необходимо перейти по ссылке и после авторизации скопировать код авторизации в адресной строке!\n' + request.request.url)

    def access_token(self):
        with open(os.path.join(os.getcwd(), 'token', 'petal_token.txt')) as f:
            auth = f.readline()
        url = self.base_url + '/token?grant_type=authorization_code&code=' + auth
        header = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        params = {
            'client_id': self.client_id,   # Установите для этого параметра значение «офлайн» только в том случае, если необходимо вернуть токен обновления.
            'client_secret': os.environ['CLIENT_SECRET_PETAL'],
            'redirect_uri': 'https://huntermob.com/'
        }
        request = requests.get(url, headers=header, params=params)
        with open(os.path.join(os.getcwd(), 'token', 'petal_token.txt'), mode='w') as f:
            f.writelines((request.json()['access_token'], '\n', request.json()['refresh_token']))

    def download_data(self, start_date, end_date):
        url = 'https://ads-drru.cloud.huawei.ru/openapi/v2/reports/campaign/query'
        with open(os.path.join(os.getcwd(), 'token', 'petal_token.txt')) as f:
            token = f.readline()[:-1]
        header = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization':f'Bearer {token}'
        }
        params = {
            'advertiser_id': '1387065344731920768',
            'time_granularity': 'STAT_TIME_GRANULARITY_DAILY',
            'page_size': '10000',
            'start_date': start_date,
            'end_date': end_date,
            'is_abroad': True
        }
        request = requests.post(url, headers=header, params=params)