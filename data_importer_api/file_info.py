#!/usr/bin/env python
# coding=UTF-8

import json

class FileInfo():
    def __init__(self):
        json_obj = {
                "type":"",
                "optional":{
                    "record_sep":"",
                    "field_sep":"",
                }
        }

        self.json_object = json_obj

    def set_seperator(self, field_sep=None, record_sep='\n'):
        self.json_object['optional']['field_sep'] = field_sep
        self.json_object['optional']['record_sep'] = record_sep

    def set_file_type(self, file_type=None):
        self.json_object['type'] = file_type


    def get_seperator(self):
        return self.json_object


if __name__ == '__main__':
    test = FileInfo()

    test.set_seperator(",,","\r")
    test.set_file_type("csv")

    print(test.get_seperator())
