#!/usr/bin/env python
# coding=UTF-8

import json


class Option():
    def __init__(self):
        json_obj = {
            "option": []
        }

        self.json_object = json_obj

    def set_rename(self, old_col_name, new_col_name):
        rename_obj = {
            "option": "rename",
            "old_col_name": old_col_name,
            "new_col_name": new_col_name
        }

        self.json_object['option'].append(rename_obj)

    def set_col_type_convert(self, col_name, col_type):
        col_type_convert_obj = {
            "option": "col_type_convert",
            "col_name": col_name,
            "type": col_type
        }

        self.json_object['option'].append(col_type_convert_obj)

    def set_move(self, col_name, move_to):
        move_obj = {
            "option": "move",
            "col_name": col_name,
            "move_to": move_to
        }

        self.json_object['option'].append(move_obj)

    def set_merge(self, col_name):
        merge_obj = {
            "option": "merge",
            "col_name": col_name
        }

        self.json_object['option'].append(merge_obj)

    def set_split(self, col_name, string_cnt, col_cnt):
        split_obj = {
            "option": "split",
            "string_cnt": string_cnt,
            "col_cnt": col_cnt,
            "col_name": col_name
        }

        self.json_object['option'].append(split_obj)

    def set_data_convert(self, col_name, front_idx, end_idx, replace_from, replace_to, front_add, end_add):
        data_convert_obj = {
            "option": "data_convert",
            "front_idx": front_idx,
            "end_idx": end_idx,
            "col_name": col_name,
            "replace_from": replace_from,
            "replace_to": replace_to,
            "front_add": front_add,
            "end_add": end_add
        }

        self.json_object['option'].append(data_convert_obj)

    def get_option(self):
        return self.json_object['option']


if __name__ == '__main__':
    test = Option()
    test.set_rename("old", "new")
    test.set_merge(["test", "test2"])

    print(test.get_option())
