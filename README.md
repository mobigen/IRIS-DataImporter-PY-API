# IRIS-DataImporter-PY-API
DataImporter로 Request를 보내고, response를 받을 수 있게 해주는 python package

## 설치법
- 기본적인 설치법
```
pip install git+https://github.com/mobigen/IRIS-DataImporter-PY-API.git

or

pip3 install git+https://github.com/mobigen/IRIS-DataImporter-PY-API.git
```

## 사용법

```
    import os
    import file_info as FileInfo
    import option as Option

    # DataImport Host, Port, IRIS ID, PW
    conn = DataImporter(host, port, user_id, user_passwd)


    # file 설정 방법
    file_info = FileInfo.FileInfo()

    ## FileType 지정
    file_info.set_file_type(type)

    ## FileSep 지정
    file_info.set_seperator(field_sep, record_sep)


    # Option 설정 방법
    option = Option.Option()

    ## rename
    option.set_rename(column_name , rename_name)

    ## split
    option.set_split(column_name, string_cnt, col_cnt)

    ## merge
    merge_col_name = [column_name1,column_name2, ...]
    option.set_merge(merge_col_name)

    ## data_convert
    option.set_data_convert(column_name, front_idx, end_idx, replace_from, replace_to, front_add, end_add)

    ## move
    option.set_move(column_name, move_to)

    ## column_type_convert
    option.set_col_type_convert(column_name, column_type)

    # target Table 설정 방법
    target = {
        'table_name':'{{table_name}}'
    } 

    # File Upload
    fields, results = conn.upload(file_path, file_info.get_seperator(), option.get_option())

    # Load to DB connector_name, target_table, overwrite
    res = conn.load(connector_name, target, False)
```
