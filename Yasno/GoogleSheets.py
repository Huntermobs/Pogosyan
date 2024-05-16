import locale
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials


class GoogleSheetsClass:

    def __init__(self, token:str='service_credentials.json') -> None:
        self.url = ['https://spreadsheets.google.com/feeds',
                    'https://www.googleapis.com/auth/drive']
        self.tables = dict()
        locale.setlocale(locale.LC_TIME, 'en_EN')    # Установка локации для определения формата значений с плавающей точкой и дат
        self.credentials = ServiceAccountCredentials.from_json_keyfile_name(token, self.url)    # Авторизация с сервисным аккаунтом гугл таблиц 
        self.client = gspread.authorize(self.credentials)   # Соединение с таблицей


    def connect_client(self, name:str, id:str):
        '''
        Данная функция устанавливает соединение с таблицами, используя ID в качестве идентификатора.\n
        Все сохранённые таблицы будут лежать в переменной self.tables в качестве словаря.\n
        Ключом является переменная "name", а значением класс клиента гугл таблицы.

        Пример name: "Отчёт годовой"
        Пример id: "hlcyPYi16HnZj_zJSF5MCDpT2b-A3ItdkQG47ULKgBOw"
        '''
        if not isinstance(name, str): raise TypeError(f'Параметр "{name}" должен иметь тип "str"')
        self.tables[name] = self.client.open_by_key(id)
        print('Соединение с таблицей ' + self.tables[name].title + ' установлено!') 
        return self.tables[name]


    def import_gspread(self, worksheet:gspread.client, df:pd.DataFrame, row:int=1, col:int=1):
        '''
        worksheet: Клиент гугл таблиц, куда будет загружаться датафрейм\n
        df: Датафрейм, загружаемый в гугл таблицы\n
        row: Строка начиная с которой будут загружены данные\n
        col: Колонка начиная с которой будут загружены данные
        '''
        set_with_dataframe(worksheet, df, row, col)
        print(f'Данные успешно загружены в {worksheet.title} (Лист {worksheet.sheet1.title})')


    def change_locate(self, local:str):
        try:
            locale.setlocale(locale.LC_TIME, local) 
        except:
            raise ValueError('Не удалось изменить локацию у гугл таблиц. Некорректное значение')
        print('Локация успешно изменена на ' + local)