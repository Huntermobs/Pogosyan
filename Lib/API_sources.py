import json
import os

import pandas as pd
import requests


class Vkads_stats_api:

    def __init__(self, general_creds='creds_vkads.txt',
                 tokens_folder=os.path.join(os.getcwd(), 'token'), acc_ids=['']):

        self.acc_ids = acc_ids
        self.general_creds = general_creds
        self.tokens_folder = tokens_folder
        self.url = 'https://ads.vk.com/api/v2/'

        self.tokens = {}
        for acc_id in acc_ids:
            for _, _, files in os.walk(tokens_folder):
                if acc_id + '.txt' in files:
                    with open(os.path.join(tokens_folder, acc_id + '.txt'), "r", encoding='utf8') as f:
                        token = json.load(f)
                        token = token["access_token"]
                else:
                    with open(os.path.join(tokens_folder, general_creds), "r", encoding='utf8') as f:
                        credentials = json.load(f)
                    token_response = self._reguest_token(credentials=credentials, acc_id=acc_id)
                    with open(os.path.join(tokens_folder, acc_id + '.txt'), "w", encoding="utf-8") as output_file:
                        json.dump(token_response, output_file)
                    token = token_response["access_token"]
            self.tokens[acc_id] = token
        for root, dirs, files in os.walk(tokens_folder):
            if 'account_token' + '.txt' in files:
                with open(os.path.join(tokens_folder, 'account_token.txt'), "r", encoding='utf8') as f:
                    token = json.load(f)
                    self.main_token = token["access_token"]
            else:
                with open(os.path.join(tokens_folder, general_creds), "r", encoding='utf8') as f:
                    credentials = json.load(f)
                token_response = self._reguest_token(credentials=credentials, acc_id=None)
                with open(os.path.join(tokens_folder, 'account_token.txt'), "w", encoding="utf-8") as output_file:
                    json.dump(token_response, output_file)
                self.main_token = token_response["access_token"]

    def _reguest_token(self, credentials, acc_id):

        url = self.url + "oauth2/token.json"
        payload = {
            "client_id": credentials['client_id'],
            "client_secret": credentials['client_secret'],
        }

        if acc_id:
            payload["grant_type"] = "agency_client_credentials"
            payload["agency_client_id"] = acc_id
        else:
            payload["grant_type"] = "client_credentials"

        headers = {
            "Content-Type": "application/x-www-form-urlencoded"
        }

        response = requests.post(url, data=payload, headers=headers)
        if response.status_code == 200:
            print(response.json())
            return response.json()
        else:
            print(response.json())
            raise ValueError ('Вызвана ошибка запроса: ' + response.text)

    def _refresh_token(self, acc_id):

        with open(os.path.join(self.tokens_folder, self.general_creds), "r", encoding='utf8') as f:
            credentials = json.load(f)
        if acc_id:
            with open(os.path.join(self.tokens_folder, acc_id + '.txt'), "r", encoding='utf8') as f:
                refresh_token = json.load(f)
                refresh_token = refresh_token["refresh_token"]
        else:
            with open(os.path.join(self.tokens_folder, 'account_token.txt'), "r", encoding='utf8') as f:
                refresh_token = json.load(f)
                refresh_token = refresh_token["refresh_token"]
        url = self.url + "oauth2/token.json"
        payload = {
            "grant_type": "refresh_token",
            "client_id": credentials['client_id'],
            "client_secret": credentials['client_secret'],
            "refresh_token": refresh_token
        }

        headers = {
            "Content-Type": "application/x-www-form-urlencoded"
        }

        response = requests.post(url, data=payload, headers=headers)
        if response.status_code == 200:
            if acc_id:
                with open(os.path.join(self.tokens_folder, acc_id + '.txt'), "w", encoding="utf-8") as output_file:
                    json.dump(response.json(), output_file)

                    self.tokens[acc_id] = response.json()["access_token"]
            else:
                with open(os.path.join(self.tokens_folder, 'account_token.txt'), "w", encoding="utf-8") as output_file:
                    json.dump(response.json(), output_file)
                self.main_token = response.json()["access_token"]
        else:
            print(response.json())
            raise ValueError

    def _get_client(self, acc_id):
        url = self.url + 'agency/clients.json'
        headers = {"Authorization": f"Bearer {self.main_token}"}
        params = {
            'limit':50,
            '_user__id':acc_id
        }
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 401:
            self._refresh_token(acc_id=None)
            headers = {"Authorization": f"Bearer {self.main_token}"}
            response = requests.get(url, headers=headers, params=params)
        acc_name = response.json()["items"][0]['user']['client_username']
        return acc_id, acc_name

    def _req_banners(self):
        url = self.url + "banners.json?limit=250&fields=id,name,ad_group_id"
        total = None
        for key in self.tokens.keys():
            offset = 0
            access_token = self.tokens[key]
            headers = {"Authorization": f"Bearer {access_token}"}
            response = requests.get(url, headers=headers)
            if response.status_code == 401:
                self._refresh_token(acc_id=key)
                headers = {"Authorization": f"Bearer {self.tokens[key]}"}
                response = requests.get(url, headers=headers)
            if response.status_code == 404: 
                print('Нет данных по ' + str(key))
                continue
            banners = pd.DataFrame(response.json()["items"])
            while (response.json()['count'] - offset) >= 250:
                offset = offset + 250  # if (response.json()['count']-offset)>250 else offset+
                response = requests.get(f'{url}&offset={offset}', headers=headers)
                if response.status_code == 404: 
                    print('Нет данных по ' + str(key))
                    break
                banners = pd.concat([banners, pd.DataFrame(response.json()["items"])], ignore_index=True)
            banners.rename(columns={"id": "banner_id", "name": "banner_name", "ad_group_id": "campaign_id"},
                           inplace=True)
            if total is None:
                total = banners
            else:
                total = pd.concat([total, banners], ignore_index=True)
        return total

    def _req_adgroups(self):
        # соответствует кампании в пларине
        url = self.url + "ad_groups.json?limit=250"
        total = None
        for key in self.tokens.keys():
            offset = 0
            access_token = self.tokens[key]
            headers = {"Authorization": f"Bearer {access_token}"}
            response = requests.get(url, headers=headers)
            if response.status_code == 401:
                self._refresh_token(acc_id=key)
                headers = {"Authorization": f"Bearer {self.tokens[key]}"}
                response = requests.get(url, headers=headers)
            adgroups = pd.DataFrame(response.json()["items"])
            while (response.json()['count'] - offset) >= 250:
                offset = offset + 250  # if (response.json()['count']-offset)>250 else offset+
                response = requests.get(f'{url}&offset={offset}', headers=headers)
                if response.status_code == 404: 
                    print('Нет данных по ' + str(key))
                    break
                adgroups = pd.concat([adgroups, pd.DataFrame(response.json()["items"])], ignore_index=True)
            adgroups.rename(columns={"id": "campaign_id", "name": "campaign_name"}, inplace=True)
            if total is None:
                total = adgroups
            else:
                total = pd.concat([total, adgroups], ignore_index=True)
        return total

    def _req_adplans(self):
        url = self.url + "ad_plans.json"
        for key in self.tokens.keys():
            access_token = self.tokens[key]
            headers = {"Authorization": f"Bearer {access_token}"}
            response = requests.get(url, headers=headers)
            print(response)

    def req_base_metrics(self, end_date, start_date):
        url = self.url + "statistics/banners/day.json"
        total = None
        for key in self.tokens.keys():
            access_token = self.tokens[key]
            headers = {"Authorization": f"Bearer {access_token}"}

            params = {
                # 'attribution': 'conversion',
                # 'conversion_type': 'postview',
                'date_to': end_date,
                'date_from': start_date,
                'metrics': 'all',
            }

            response = requests.get(url, params=params, headers=headers)

            if response.status_code == 401:
                self._refresh_token(acc_id=key)
                headers = {"Authorization": f"Bearer {self.tokens[key]}"}
                response = requests.get(url, params=params, headers=headers)
            stats = response.json()
            data = []
            for item in stats["items"]:
                id_value = item["id"]
                for row in item["rows"]:
                    row_data = {"id": id_value, "date": row["date"]}
                    row_data.update(row["base"])
                    data.append(row_data)
            df = pd.DataFrame(data)
            acc_id, acc_name = self._get_client(acc_id=key)
            df['ID Клиента'] = acc_id
            df['Клиент'] = acc_name
            if total is None:
                total = df
            else:
                total = pd.concat([total, df], ignore_index=True)

        total.rename(columns={"id": "banner_id"}, inplace=True)
        adgroups = self._req_adgroups()
        banners = self._req_banners()
        banners = pd.merge(banners, adgroups, left_on='campaign_id', right_on="campaign_id", how='left')
        result = pd.merge(total, banners, left_on='banner_id', right_on="banner_id", how='left')
        result['spent'] = result['spent'].astype(float)
        result = result[(result['shows'] != 0) | (result['spent'] != 0)]
        result.rename(columns={'banner_id': 'ID Объявления', 'date': 'Дата', 'campaign_id': 'ID Кампании',
                               'banner_name': 'Название объявления', 'campaign_name': 'Название кампании',
                               'shows': 'Показы', 'clicks': 'Клики', 'goals': 'Конверсии', 'spent': 'Расход'},
                      inplace=True)
        result['cpm'] = result['cpm'].astype(float)
        result['cpc'] = result['cpc'].astype(float)
        result['cpa'] = result['cpa'].astype(float)
        # result['vk'] = '0'
        # result = result[['ID Объявления', 'Дата', 'Название объявления', 'ID Кампании', 'Название кампании',
        #                  'ID Клиента', 'Клиент', 'Показы', 'Расход', 'Клики', 'Конверсии', 'cpm', 'cpc', 'cpa',
        #                  'ctr', 'cr', 'vk']]
        return result


# test = Vkads_stats_api(acc_ids=['20008720','20008726','20531611','20531614'])
# test.req_base_metrics(end_date='2024-03-25', start_date='2024-03-01')

def load_base_data(end_date, start_date, acc_ids, api_type='vkads_base'):
    if api_type == 'vkads_base':
        loader = Vkads_stats_api(acc_ids=acc_ids['acc_ids'])
        return loader.req_base_metrics(end_date=end_date, start_date=start_date)

