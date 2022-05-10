#!/usr/bin/env python
# coding=UTF-8

import requests 
import os
import json
from requests_toolbelt.multipart.encoder import MultipartEncoder

# TODO Get Session ID using IWP
class DataImporter():
    def __init__(self, host=None, port=None, user_id=None, user_passwd=None):
        self.host = host
        self.port = port
        self.user_id = user_id
        self.user_passwd = user_passwd
        self.meta_info = {
           "user":user_id
        }


    def upload(self, file_path, file_info, option=None):
        upload_res = self.pre_upload(file_path, file_info)
        if upload_res.status_code != 200:
            raise Exception(res.json()['Message'])

        self.job_id = upload_res.json()['jobId']
        trans_res = self.pre_transform(self.job_id, option)

        return upload_res.json()['fields'], upload_res.json()['results']


    def pre_upload(self, file_path, file_info):
        if os.path.isfile(file_path) is False:
            raise Exception('file not exist error')

        payload = {
            "file_info": json.dumps(file_info),
            "meta_info": json.dumps(self.meta_info)
        }

        files = {'file': ('test.csv', open(file_path, 'rb'), 'text/plain')}

        if self.port == None:
            url = "http://{}/dis/v2/upload".format(self.host)
        else :
            url = "http://{}:{}/dis/v2/upload".format(self.host, self.port)

        res = requests.request("POST", url, data=payload,files=files)

        return res


    def pre_transform(self, job_id, option=None):
        payload = {
            'job_id': job_id,
            'option': option
        }

        headers = {
            'Content-type': 'application/json'
        }

        if self.port == None:
            url = "http://{}/dis/v2/transform".format(self.host)
        else :
            url = "http://{}:{}/dis/v2/transform".format(self.host, self.port)

        res = requests.request("POST", url, headers=headers, data=json.dumps(payload))

        return res
        

    def load(self, connector_name, target, is_overwrite):
        if is_overwrite:
            res = self.check_storage(self.job_id, connector_name,target)
            if res.json()['exist']:
                res = self.delete_storage(self.job_id, connector_name, target)
                if res.status_code != 200:
                    raise Exception(res.json()['Message'])
            res = self.create_storage(self.job_id, connector_name, target)
            if res.status_code != 200:
                raise Exception(res.json()['Message'])
            res = self.load_data(self.job_id, connector_name, target)
            if res.status_code != 200:
                raise Exception(res.json()['Message'])
        else:
            res = self.check_storage(self.job_id, connector_name,target)
            if res.json()['exist']:
                raise Exception("Already Exist")
            res = self.create_storage(self.job_id, connector_name, target)
            if res.status_code != 200:
                raise Exception(res.json()['Message'])
            res = self.load_data(self.job_id, connector_name, target)
            if res.status_code != 200:
                raise Exception(res.json()['Message'])
    
        return "Success"


    def load_data(self, job_id, connector_name, target):
        payload = json.dumps({
            'job_id': job_id,
            'connector_name': connector_name,
            'target': target
        })

        headers = {
            'Content-type': 'application/json'
        }

        if self.port == None:
            url = "http://{}/dis/v2/storage/load".format(self.host)
        else :
            url = "http://{}:{}/dis/v2/storage/load".format(self.host, self.port)

        res = requests.request("POST", url, headers=headers, data=payload)

        return res



    def delete_storage(self, job_id, connector_name, target):
        payload = json.dumps({
            'job_id': job_id,
            'connector_name': connector_name,
            'target': target
        })

        headers = {
            'Content-type': 'application/json'
        }

        if self.port == None:
            url = "http://{}/dis/v2/storage/delete".format(self.host)
        else :
            url = "http://{}:{}/dis/v2/storage/delete".format(self.host, self.port)

        res = requests.request("POST", url, headers=headers, data=payload)

        return res


    def create_storage(self, job_id, connector_name, target):
        payload = json.dumps({
            'job_id': job_id,
            'connector_name': connector_name,
            'target': target
        })

        headers = {
            'Content-type': 'application/json'
        }

        if self.port == None:
            url = "http://{}/dis/v2/storage/create".format(self.host)
        else :
            url = "http://{}:{}/dis/v2/storage/create".format(self.host, self.port)

        res = requests.request("POST", url, headers=headers, data=payload)

        return res


    def check_storage(self, job_id, connector_name, target):
        payload = json.dumps({
            'job_id': job_id,
            'connector_name': connector_name,
            'target' : target
        })
        

        headers = {
            'Content-Type': 'application/json'
        }

        if self.port == None:
            url = "http://{}/dis/v2/storage/check".format(self.host)
        else :
            url = "http://{}:{}/dis/v2/storage/check".format(self.host, self.port)

        res = requests.request("POST", url, headers=headers, data=payload)

        return res


if __name__ == '__main__': 
    import os
    import file_info as FileInfo
    import option as Option

    # Setting
    conn = DataImporter(host="0.0.0.0", port=9004, user_id="root", user_passwd="")

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

    # Load to DB
    res = conn.load("TestKo", target, False)



    # ...
