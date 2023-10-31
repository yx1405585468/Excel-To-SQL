import sys
import time

import pandas as pd
import psycopg2

pd.set_option("display.width", None)
pd.set_option("display.max_rows", None)


def get_data():
    for i in range(1000):
        try:
            file_path = input(r"请输入本地结构文件地址，如：C:\Users\yx140\Desktop\测试数据.csv：" + "\n")  # 本地文件地址
            print()
            encode = input("请输入文件编码类型，如：gbk或utf-8\n")  # 本地文件地址
            print()
            data = pd.read_csv(r"{}".format(file_path), encoding=encode)
            break

        except Exception as e:
            print("地址错误，请重新输入：")
            print(e)
            continue
    table_name = input("请输入要创建的表名：\n")  # 要创建的表格
    print()
    return data, table_name


def create_table(data, table_name, host, port, dbname, user, password):
    try:
        # 数据转换
        data["数据类型"] = data["数据类型"].apply(
            lambda x: "VARCHAR" if x == "C" else "NUMERIC" if x == "D" else "DATE" if x == "T" else "INTEGER")
        data["是否必填"] = data["是否必填"].apply(lambda x: "NOT NULL" if x == "是" else " ")

        # 创建字段列表
        字段中文意义 = list(data["字段中文意义"])
        字段名称 = list(data["字段名称"])
        数据类型 = list(data["数据类型"])
        数据宽度 = list(data["数据宽度"])
        是否必填 = list(data["是否必填"])
        # 说明 = list(data["说明"])

        # 合成建表语句
        sql_list = []
        for name_, type_, length_, not_null_ in zip(字段名称, 数据类型, 数据宽度, 是否必填):
            if name_ == "USID":
                sql_temp = f"{name_.lower()} {type_}({length_}) PRIMARY KEY"
                sql_list.append(sql_temp.strip())

            elif type_ == "INTEGER":
                sql_temp = f"{name_.lower()} {type_} {not_null_}"
                sql_list.append(sql_temp.strip())
            elif type_ == "DATE":
                sql_temp = f"{name_.lower()} {type_} {not_null_}"
                sql_list.append(sql_temp.strip())

            else:
                sql_temp = f"{name_.lower()} {type_}({length_}) {not_null_}"
                sql_list.append(sql_temp.strip())

        sql_string = ", ".join(sql_list)

        try:
            # 连接到数据库
            conn = psycopg2.connect(
                host=host,
                port=port,
                dbname=dbname,
                user=user,
                password=password
            )

            # 创建游标对象
            cursor = conn.cursor()

            # 创建表格
            create_table_query = '''
                    CREATE TABLE {table_name} (
                        {sql_code}
                    );
                    '''.format(table_name=table_name, sql_code=sql_string)
            cursor.execute(create_table_query)

            # 添加注释
            for name_, detail_ in zip(字段名称, 字段中文意义):
                add_comment = '''
                            COMMENT ON COLUMN {table_name}.{name} IS '{detail}';
                            '''.format(table_name=table_name, name=name_, detail=detail_)
                cursor.execute(add_comment)

            # 提交事务
            conn.commit()

            print(f"Table '{table_name}' created successfully.")

            # 关闭游标和数据库连接
            cursor.close()
            conn.close()

        except psycopg2.Error as e:
            print("Error connecting to PostgreSQL database:", e)
    except Exception as e:
        print("文件格式有误！")
        print(e)
        time.sleep(3600)


if __name__ == '__main__':
    # 数据库连接信息
    print("----------------------------------------------制作说明------------------------------------------------")
    print("本软件仅为将excel列出的postgresql数据库表结构，傻瓜式一键建表。其他数据库及数据格式请勿乱用！")
    print("文件格式见本地文件")
    print("制作不易，请珍爱生命，拥护和平！")
    print("----------------------------------------------制作说明------------------------------------------------")
    host = input("请输入数据库地址，如：127.0.0.1：\n")  # 地址
    print()
    port = input("请输入数据库端口，如：5432：\n")  # 端口
    print()
    dbname = input("请输入要链接的数据库，如：test_database：\n")  # 要建表的数据库
    print()
    user = input("请输入用户名，如：余总大帅比：\n")  # 用户名
    print()
    password = input("请输入密码，如：123456：\n")  # 密码
    print()

    data, table_name = get_data()

    for i in range(1000):
        keys = input("请输入口令：如未获取口令请联系微信：yx1405585468\n")  # 要创建的表格
        if keys == "口令是啥":
            print(" ")
            print(
                "最后：请注重作者制作权，请勿乱传播，请勿用于商业生产受益等活动，违者必究！如有定制需求，可联系我，微信yx1405585468！")
            print("正在注入中...")
            create_table(data, table_name, host, port, dbname, user, password)
            print()
            while True:
                con = input("是否继续注入：\n")
                print()
                if con == "是":
                    data, table_name = get_data()
                    print("正在注入中...")
                    create_table(data, table_name, host, port, dbname, user, password)
                    print()
                    continue
                else:
                    print("感谢使用！")
                    time.sleep(3)
                    sys.exit()

        else:
            print("口令错误！")
            print()
            continue
