import requests


class SlackReportClass:
    '''
    Данный класс позволяет отправлять уведомения в слак используя несколько параметров перед инициализацией:

    - webhook_url :str - URL предоставляется в Slack Workflow
    - message     :str - Сообщение которое будет передоваться в теле письма
    - hashtag     :str - Хэштег для пометки работы с клиентами
    - email_ping  :list - Емайлы для упоминания пользователей. ВАЖНО! Первый Емайл отмечает человека ответственного за отчёт!
    - table_buy   :str - URL таблицы Баеров
    - table_rekl  :str - URL таблицы Реклов

    Данные параметры так же применяются в шаблоне Slack Workflow. 
    Для добавления новых емайлов в шаблон, нужно приписывать цифры, например: 
    email_ping1, email_ping2, email_ping3, ...

    Пример результата:
        #Yasno\n
        Обновил 25.04\n
        @Артём Погосьян\n
        https://docs.google.com/spreadsheets/d/1FhvgM0fGj4l0WAAdsVyK_Y84hM7MPjKKqU1XnjgH4mA/edit#gid=1428294449 Баер\n
        https://docs.google.com/spreadsheets/d/1CvQVRD94Lo0VolcV5Iml7G4LSFV8UDGc8xY3l4mGkLM/edit#gid=1381342142 Рекл
    '''
    def __init__(self, webhook_url:str, message:str, hashtag:str, email_ping:list, table_buy:str=None, table_rekl:str=None) -> None:
        self.webhook = webhook_url
        self.message = message
        self.hashtag = hashtag
        self.responsible = email_ping[0]
        self.emails = email_ping[1:]
        self.tables = {
            'ads': table_rekl,
            'buy': table_buy
        }

    def send_slack_message(self, only_buy:bool=False, only_ads:bool=False) -> None:
        '''
        Метод отправки сообщения в Slack:

        - only_buy  :bool - Если необходимо отправить только Баерский отчёт
        - only_ads  :bool - Если необходимо отправить только Рекловский отчёт

        При использовании параметра True для двух переменных, произойдёт ошибка 'ValueError'
        '''
        payload = {
            "message": self.message,
            "hashtag": self.hashtag,
            "responsible":self.responsible
        }
        match [only_buy, only_ads]:
            case [True, False]:
                payload["table_buy"] = self.tables['buy']
            case [False, True]:
                payload["table_rekl"] = self.tables['ads']
            case [False, False]:
                payload["table_buy"] = self.tables['buy']
                payload["table_rekl"] = self.tables['ads']
            case _:
                raise ValueError("Если необходимо загрузить обе таблицы, оставьте переменные 'only_buy' и 'only_ads' со значением 'False'")
        for i, email in enumerate(self.emails, start=1):
            payload["email_ping"+str(i)] = email
        response = requests.post(self.webhook, json=payload)
        if response.status_code == 200:
            print('Уведомление отправленно в Slack!')
        else:
            print('Проблема с сообщением в Slack! ' + response.text)

