#!/usr/bin/env python
# coding=UTF-8

import json


class FileInfo():
    def __init__(self):
        self.json_object = {}

    def set_seperator(self, field_sep=None, record_sep='\n'):
        self.json_object = {'separator': json.dumps({'field_sep': field_sep, 'record_sep': record_sep})}

    def set_file_type(self, file_type=None):
        self.json_object = {'type': file_type}

    def get_seperator(self):
        return self.json_object


if __name__ == '__main__':
    test = FileInfo()

    test.set_seperator(",,", "\r")
    test.set_file_type("csv")

    print(test.get_seperator())
