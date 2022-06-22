#!/usr/bin/env python
# coding=UTF-8

import requests
import os
import json
from requests_toolbelt.multipart.encoder import MultipartEncoder


# TODO Get Session ID using IWP
class DataImporter():
    def __init__(self, url, user_id, user_passwd):
        self.job_id = None
        self.url = url
        self.user_id = user_id
        self.user_passwd = user_passwd
        self.meta_info = {
            "user": user_id
        }
        # self.jwp_token = self.get_jwp_token()
        # TODO Test 용 token
        self.jwp_token = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VySWQiOiJyb290IiwidXNlclBhc3MiOiJLTHA1QU85NFRxQkRYamQrc2FvWXRRPT0iLCJlbmNyeXB0UGFzcyI6ImQ5OWUyMTRjYjI4NWM1ZjBiZmViODYyMzFlZWE4N2I5YjgzMTY1MjkiLCJncm91cElkIjoiIiwiY3JlYXRlRGF0ZSI6IjIwMTcxMTIyMDAwMDAwIiwibmFtZSI6IiIsImVtYWlsIjoiIiwicGhvbmUiOiIiLCJyb2xlQ29kZSI6IlJPT1QiLCJyb2xlTmFtZSI6IuujqO2KuCIsImRlc2MiOiIiLCJhdXRob3JpdGllcyI6W10sImlzRW5jcnlwdGVkUGFzcyI6ZmFsc2UsImJyaWNrQWRkcmVzcyI6Imh0dHA6Ly8xMC4xLjUxLjI0NTo4MDgwLyIsImlhdCI6MTY1Mzg5NDE5NywiZXhwIjoxNjUzODk3Nzk3LCJpc3MiOiJiLWlyaXMtYnJpY2siLCJzdWIiOiJ1c2VySW5mbyJ9.imsTxzK9XWf58k8doUI740r4PumCKSanBRcKJ66gvgs'

    def get_jwp_token(self):
        url = "{}/authenticate".format(self.url)

        payload = json.dumps({
            "userId": self.user_id,
            "userPass": self.user_passwd
        })

        headers = {
            'Content-Type': 'application/json',
        }

        res = requests.request("POST", url, headers=headers, data=payload)
        if "token" not in res.json():
            raise

        return res.json()['token']

    def upload(self, file_path, sep_info, option=None):
        upload_res = self.pre_upload(file_path, sep_info)
        if upload_res.status_code != 200:
            raise Exception(upload_res.json()['Message'])

        self.job_id = upload_res.json()['jobId']
        if option is not None:
            trans_res = self.pre_transform(option)
            if trans_res.status_code != 200:
                raise

        return upload_res.json()['fields'], upload_res.json()['results']

    def pre_upload(self, file_path, sep_info):
        if os.path.isfile(file_path) is False:
            raise Exception('file not exist error')

        payload = {
            "separator_info": json.dumps(sep_info),
        }

        files = {'file': (file_path.split('/')[-1], open(file_path, 'rb'), 'text/csv')}

        headers = {
            'x-access-token': self.jwp_token
        }

        url = "{}/iris-data-importer/v2/upload".format(self.url)

        res = requests.request("POST", url, headers=headers, data=payload, files=files)

        return res

    def pre_transform(self, option=None):
        payload = {
            'job_id': self.job_id,
            'option': option.get_option()
        }

        headers = {
            'x-access-token': self.jwp_token,
            'Content-type': 'application/json'
        }

        url = "{}/iris-data-importer/v2/transform".format(self.url)
        res = requests.request("POST", url, headers=headers, data=json.dumps(payload))

        return res

    def load(self, target_db, target, is_overwrite):
        target_db['connector_id'] = self.get_storage(target_db)
        target_db.pop('connector_name', None)
        res = self.load_data(target_db, target, is_overwrite)
        if res.status_code == 412:
            raise Exception(res.json()['Message'])
        elif res.status_code != 200:
            raise Exception(res.json())

        return "Success"

    def get_storage(self, target_db):
        headers = {
            'Content-Type': 'application/json',
            'x-access-token': self.jwp_token
        }

        url = "{}/iris-data-importer/v2/storage/get/{}".format(self.url, self.job_id)

        res = requests.request("get", url, headers=headers)

        for result in res.json()['results']:
            if result['name'] == target_db['connector_name'] and result['datasourceName'] == target_db['datasource_name']:
                return result['id']

    def load_data(self, target_db, target, is_overwrite):
        payload = json.dumps({
            'job_id': self.job_id,
            'load_type': is_overwrite,
            'target_db': target_db,
            'target': target
        })

        headers = {
            'Content-type': 'application/json',
            'x-access-token': self.jwp_token
        }

        url = "{}/iris-data-importer/v2/storage/load".format(self.url)

        res = requests.request("POST", url, headers=headers, data=payload)

        return res


if __name__ == '__main__':
    import os
    import file_info as FileInfo
    import option as Option

    # Setting
    conn = DataImporter(url="https://dev.iris.tools", user_id="", user_passwd="")
    '''
    file_info = FileInfo.FileInfo()
    file_info.set_file_type("CSV")

    option = Option.Option()
    option.set_rename("colrename","newname")
    option.set_split("colsplit",2,2)
    merge_col_name = ['colmerge','co']
    option.set_merge(merge_col_name)
    option.set_data_convert("dataconvert",2,3,"replace_from", "replace_to","front","end")
    option.set_move("colmove",2)
    option.set_col_type_convert("coltypeconvert", "TEXT")

    target = {
        'table_name':'tttt'
    } # TODO 위와 같이 고정 필요 

    # File Upload
    fields, results = conn.upload("/Users/sengbumgo/company/dist/test.csv", file_info.get_seperator(), option.get_option())
    '''

    # Load to DB
    # res = conn.load("TestKo", target, False)

    # ...
