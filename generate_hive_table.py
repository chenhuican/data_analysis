#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = "Gerry Chan"
"""
   广州天鹏科技
   oracle 数据表转hive表
   大数据平台hive版本3.0
   1) 指标数据表结构依据初始库 实例：192.168.0.103/init_3
   2) 源数据表结构依据 实例：192.168.0.55/GYALLORCL
   
   :Mail: chenhc@tp-data.com
"""
import cx_Oracle
import os
import re
import time
from hive.config import table_list

#二选一 sourcedata
db_info = {
    "host": "192.168.0.253",
    "port": "1521",
    "database": "orcl",
    "username": "sourcedata",
    "password": "passwd112",
}

# warehouse表实例
db_info1 = {
    "host": "192.168.0.55",
    "port": "1521",
    "database": "orcl159",
    "username": "sourcedata",
    "password": "passwd112",
}

schema_list = ['ecom',
               'PRIMARYINDEX',
               'SOURCEDATA',
               'WAREHOUSE']


def get_oracle_conn(**kwargs):
    dsn = cx_Oracle.makedsn(kwargs['host'],
                            kwargs['port'],
                            kwargs['database'])
    try:
        conn = cx_Oracle.connect(kwargs['username'], kwargs['password'], dsn)
        return conn
    except Exception as e:
        print("Oracle连接错误：" + str(e))
        return None


def get_single_oracle_table(conn, schema, flags, table_name):
    """
    依据表配置，获取单表信息
    :param conn:
    :param schema:
    :param table_name:
    :param flags:
    :return:
    """
    column_info = []
    try:
        cursor = conn.cursor()
        sqls = """
                SELECT
                    T1.COLUMN_NAME, 
                    T1.DATA_TYPE,
                    T1.DATA_LENGTH, 
                    T1.DEFAULT_LENGTH, 
                    T1.DATA_DEFAULT,
                    T1.DATA_PRECISION,  -- number数据长度
                    T1.DATA_SCALE
                FROM USER_TAB_COLUMNS T1
                JOIN ALL_TABLES T2 ON T1.TABLE_NAME=T2.TABLE_NAME
                WHERE T1.TABLE_NAME='%s' AND T2.OWNER = '%s'
                ORDER BY T1.TABLE_NAME,T1.COLUMN_ID
        """ % (table_name.upper(), schema.upper())

        response = cursor.execute(sqls)
        column_info = response.fetchall()
    except Exception as e:
        print("获取Oracle表信息失败： " + str(e))
        conn.close()
    # 拼接好的建表sql
    sql = generate_create_table_sql(schema, column_info, flags, table_name)
    return sql


def single_oracle_oracle_table(conn, schema, flags, table_name):
    """
    依据表配置，获取单表信息(用于读取oracle 配置oracle表)
    :param conn:
    :param schema:
    :param table_name:
    :param flags:
    :return:
    """
    column_info = []
    try:
        cursor = conn.cursor()
        sqls = """
                   SELECT DISTINCT
                         B.COLUMN_NAME,
                         A.DATA_TYPE,
                         A.DATA_TYPE||'('||A.CHAR_LENGTH||')',
                         B.COMMENTS,
                         COLUMN_ID
                   FROM   USER_TAB_COLUMNS A
                   JOIN ALL_TABLES T2 ON A.TABLE_NAME=T2.TABLE_NAME
                   INNER JOIN USER_COL_COMMENTS B ON A.TABLE_NAME=B.TABLE_NAME 
                   AND A.COLUMN_NAME=B.COLUMN_NAME
                   WHERE A.TABLE_NAME='%s' AND T2.OWNER = '%s'
                   ORDER BY COLUMN_ID
               """ % (table_name.upper(), schema.upper())

        response = cursor.execute(sqls)
        column_info = response.fetchall()
    except Exception as e:
        print("获取Oracle表信息失败： " + str(e))
        conn.close()
    # 拼接好的建表sql
    sql = generate_oracle_create_table_sql(schema, column_info, flags, table_name)
    return sql


def get_oracle_table_list(conn, schema, flags):
    """
    根据schema获取所有表信息
    :param conn:
    :param schema:
    :param table_name:
    :param flags:
    :return:
    """
    try:
        cursor = conn.cursor()
        sqls = """SELECT
                    TABLE_NAME,
                    COLUMN_NAME, 
                    DATA_TYPE,
                    DATA_LENGTH, 
                    DEFAULT_LENGTH, 
                    DATA_DEFAULT
                FROM USER_TAB_COLUMNS 
                WHERE TABLE_NAME IN ( SELECT TABLE_NAME FROM ALL_TABLES WHERE OWNER = '%s')  
                ORDER BY TABLE_NAME,COLUMN_ID;
              """ % (schema)
        # sqls = """
        #     SELECT DISTINCT
        #           B.COLUMN_NAME,
        #           A.DATA_TYPE,
        #           A.DATA_TYPE||'('||A.CHAR_LENGTH||')',
        #           B.COMMENTS,
        #           COLUMN_ID
        #     FROM   USER_TAB_COLUMNS A
        #     INNER JOIN USER_COL_COMMENTS B ON A.TABLE_NAME=B.TABLE_NAME AND A.COLUMN_NAME=B.COLUMN_NAME
        #     WHERE A.TABLE_NAME='%s'
        #     ORDER BY COLUMN_ID;
        # """ % (table_name.upper())
        response = cursor.execute(sqls)
        column_info = response.fetchall()
    except Exception as e:
        print(e)
        conn.close()
    # 拼接好的建表sql
    sql = generate_create_table_sql(schema, column_info, flags)
    return sql


def generate_create_table_sql(schema, column_info, flags, table_name=None):
    """
    oracle表转hive表
    hive3 原生支持ACID, 可省略transactional'='true'
    tuple: column_name, data_type,data_length, default_length, data_default
    :param table_info:
    :param table_name:
    :param distributed_field:
    :return:
    """
    sql = ''
    if table_name:
        sql = "drop table if exists %s.%s;\n" % (schema, table_name)

    if flags == 1:
        sql = "drop table if exists %s.%s;\n" % ("data_all", table_name)
        sql = sql + "create external table if not exists %s.%s(\n" % ("data_all", table_name)
    else:
        sql = sql + "create table if not exists %s.%s(\n" % (schema, table_name)

    clustered_field = None
    clustered_fields = ''
    clustered_other = ''
    column_list = []

    for i in column_info:
        clustered_other = column_info[0][0]
        columns = i[0]
        column_list.append(columns.lower())

        if columns == 'DESCRIBE':
            columns = 'describe1'

        if columns == 'INPATIENT_NO':
            clustered_field = 'hospital_code,' + columns.lower()
        if columns == 'IN_PATIENT_ID':
            clustered_field = 'hospital_code,' + columns.lower()
        if columns == 'INP_PATIENT_NO':
            clustered_field = 'hospital_code,' + columns.lower()
        if columns == 'OUTP_PATIENT_NO':
            clustered_field = 'hospital_code,' + columns.lower()

        if ('VARCHAR' or 'VARCHAR2') in i[1]:
            sql = sql + columns.lower() + ' string,\n'
        elif 'CHAR' in i[1]:
            sql = sql + columns.lower() + ' string,\n'
        elif i[1] == 'NUMBER':
            decimal_num = ' NUMERIC' + '(' + str(i[5]) + ',' + str(i[6]) + ')' if i[5] is not None and i[6] is not None else ' bigint'
            sql = sql + columns.lower() + decimal_num + ',\n'
        elif 'TIMESTAMP' in i[1] or 'DATE' in i[1]:
            sql = sql + columns.lower() + ' ' + ' string,\n'
        elif 'CLOB' in i[1] or 'XMLTYPE' in i[1] or 'BLOB' in i[1]:
            sql = sql + columns.lower() + ' string,\n'
        else:
            sql = sql + columns.lower() + ' string,\n'

    if ('outp_patient_no' in column_list
            and 'inpatient_no' in column_list
            and 'hospital_code' in column_list):
        clustered_field = 'hospital_code,inpatient_no,outp_patient_no'

    if table_name == 'clinic_orders_detail':
        clustered_fields = 'hospital_code'

    if table_name == 'exam_report_detail':
        clustered_fields = 'hospital_code'

    if table_name == 'dw_t_tmp_drugs':
        clustered_fields = 'code'

    if table_name == 'data_category':
        clustered_fields = 'category_id'

    if table_name == 'data_query_group':
        clustered_fields = 'group_id'

    if table_name == 'data_schema':
        clustered_fields = 'data_id'

    if table_name == 'data_schema_category':
        clustered_fields = 'category_id'

    if table_name == 'data_template':
        clustered_fields = 'template_id'

    if table_name == 'data_value_type':
        clustered_fields = 'type_id'

    if table_name == 'dict_charleson':
        clustered_fields = 'mark'

    if len(clustered_fields) > 0:
         clustered_fields = clustered_fields
    else:
        clustered_fields = clustered_field if clustered_field is not None else clustered_other

    if flags == 1:
        sql = sql[:-2] + "\n)\n" + \
              "clustered by (" + clustered_fields + ")" + \
              " into 16 buckets \nrow format delimited fields terminated by ',' \n" + \
              "stored as orc tblproperties(" \
              "'serialization.encoding'='utf-8'," \
              "'orc.compress'='SNAPPY');\n"
    else:
        sql = sql[:-2] + "\n)\n" + \
              "clustered by (" + clustered_fields + ")" + \
              " into 16 buckets \nrow format delimited fields terminated by ',' \n" + \
              "stored as orc tblproperties(" \
              "'serialization.encoding'='utf-8'," \
              "'orc.compress'='SNAPPY'," \
              "'transactional'='true');\n"
    return sql


def generate_oracle_create_table_sql(schema, column_info, flags, table_name=None):
    """
    根据oracle生成Orale表
    :param schema:
    :param column_info:
    :param flags:
    :param table_name:
    :return:
    """
    sql = ''
    if table_name:
        sql = "drop table if exists %s.%s;\n" % (schema, table_name)

    sql = sql + "create table if not exists %s.%s(\n" % (schema, table_name)

    #print(column_info)

    for i in column_info:
        columns = i[0]
        columns_type = i[2]
        sql = sql + columns + columns_type + ',\n'

    sql = sql[:-2] + "\n);\n"
    return sql


if __name__ == '__main__':
    # flags 赋值1  创建external外部表脚本,0 代表内部表
    flags = 0
    create_table_sql = ""
    # table_list = collections.OrderedDict({'sourcedata':['CLINIC_ORDERS_MASTER',]})
    suffix = time.strftime("%Y%m%d", time.localtime())

    # sourcedata.dict_report 缺失 单独处理
    for schema, tables in table_list.items():
        conn = get_oracle_conn(**db_info1)
        print(schema, tables)
        for table in tables:
            sql = get_single_oracle_table(conn, schema, flags, str(table).lower())
            create_table_sql = create_table_sql + sql + '\n'

    file_name = r".\hive_table\create_sourcedate_hive_%s.sql" % suffix

    if flags == 1:
        file_name = r".\hive_table\create_external_sourcedate_hive_%s.sql" % suffix

    # 保存为sql文件
    if os.path.exists(file_name):
        os.remove(file_name)

    with open(file_name, "w", encoding='utf-8') as f:
        print("file_name: " + file_name)
        f.write(create_table_sql)
    if conn:
        conn.close
    # print(create_table_sql)


