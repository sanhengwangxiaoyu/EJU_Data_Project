# -*- coding=utf-8 -*-
#coding=utf-8
import xlrd,os


def convert_type(data_type):
    """Normalize MySQL `data_type`"""
    if 'CHAR' == data_type or 'CLNT' == data_type or 'QUAN' == data_type or 'CUKY' == data_type or 'CURR' == data_type or 'DEC' == data_type or 'INT4' == data_type or 'TIMS' == data_type or 'string' == data_type or 'String' == data_type:
        return 'varchar'
    elif 'NUMC' == data_type:
        return 'numeric'
    elif 'DATS' == data_type:
        return 'timestamp'
    else:
        return data_type
 
# 在mysql中创建表
def mysql_create(fields):
    stg_table_name = fields[0]['table_name']
    columns = []
    primary_key = []
    table_name_cn =  fields[1]['table_name_cn']
    for field in fields:
        table_column_index = ""
        table_column = ""
        if field['primary_key'] == 'Y':
            primary_key.append(field['column_name'])
        if field['column_name'] == 'id':
             table_column = '`' + field['column_name'] + '`    ' + field['type'] + '    NOT NULL AUTO_INCREMENT    ' + 'COMMENT ' + "'" + field['column_exp'] + "'" + ',\n'
        else:
            if field['null_key'] == 'N':
            # if field['primary_key'] == 'Y':
            #     primary_key.append(field['column_name'])
            # print(primary_key)
                if  field['default_value'] is None or field['default_value'] == '':
                    table_column = '`' + field['column_name'] + '`    ' + field['type'] + '    NOT NULL    ' + 'COMMENT ' + "'" + field['column_exp'] + "'" + ',\n'
                else :
                    table_column = '`' + field['column_name'] + '`    ' + field['type'] + '    NOT NULL DEFAULT "'+ str(field['default_value']) +'"    ' + 'COMMENT ' + "'" + field['column_exp'] + "'" + ',\n'
                    print(field['default_value'])
            else:
                table_column = '`' + field['column_name'] + '`    ' + field['type'] + '  DEFAULT NULL  ' + 'COMMENT ' + "'" + field['column_exp'] + "'" + ',\n'
        if field['index_key'] == 'Y':
            table_column_index = 'KEY `idx_dim_' + stg_table_name.replace('.','_') + '_' + field['column_name'] + '` (`' + field['column_name'] + '`) USING BTREE' + ',\n'        
        # print(stg_table_name)
        if table_column_index is None :
            columns.append(table_column)
        else:
            columns.append(table_column)
            columns.append(table_column_index)
    primary_key_str = "PRIMARY KEY ("
    for item in primary_key:
        primary_key_str =primary_key_str + '`' + item + '`, '     
    # print(primary_key_str)
    columns.append(primary_key_str)
    stg_create_columns = ''.join(
        columns)[:-2]
    # print(stg_create_columns)
    create_stg_sql = "drop table if exists {};\ncreate table {} (\n{})) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='{}' ;".format(
    stg_table_name,stg_table_name, stg_create_columns,table_name_cn)
    # print(primary_key)
    print(create_stg_sql)
    return create_stg_sql
 
 
# print(os.getcwd())
par_path = os.getcwd()
paths = [par_path+'\\']
print('---------------paths-------------------')
print(paths)
for path in paths:
    for filename in os.listdir(path):
        print(filename)
        if filename.endswith(".xlsx") or filename.endswith(".xls"):
            result_sql = ''
            print(path)
            print(filename)
            print(path + filename)
            worksheet = xlrd.open_workbook(path + filename)
            table_names = worksheet.sheet_names()
            for table_name in range(len(table_names)):
                sheet = worksheet.sheet_by_index(table_name)
                nrows = sheet.nrows
                fields = []
                for i in range(1,nrows):
                    res = sheet.row_values(i)
                    desc = {
                        'table_name_cn': res[0],
                        'table_name': table_names[table_name].lower(),
                        'column_exp': res[1],
                        'column_name': res[2].lower(),
                        'type': convert_type(res[3]).upper(),
                        'primary_key': res[4],
                        'null_key': res[5],
                        'index_key': res[6],
                        'default_value': res[7],
                    }
                    fields.append(desc)
                #print(fields)
                result_sql += mysql_create(fields) + '\n\n'
            
            with open(path+'\\'+filename[:-5]+'.tab', "w", encoding='utf-8') as f:
                f.write(str(result_sql))
