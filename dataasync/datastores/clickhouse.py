#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@Author     :pyang
@Date       :2022/08/10 15:41:12
@version    :1.0
@Description:
'''

import re
import pandas as pd
from uuid import uuid4
from clickhouse_driver import Client
from ..exceptions import DatabaseError
from ..constants import DF_CH_TYPE_MAP


class ClickHouse:
    """
    数据库操作
    """

    def __init__(self, host, user, password, port, database):

        self._ch_user = user
        self._ch_password = password
        self._ch_host = host
        self._ch_port = port
        self._ch_database = database

    def ch_query(self, query):
        """
        clickhouse 只读接口
        """
        client = Client(
            host=self._ch_host,
            port=self._ch_port,
            database=self._ch_database,
            user=self._ch_user,
            password=self._ch_password,
        )
        try:
            query_id = str(uuid4())
            df = client.query_dataframe(query, query_id=query_id)
        except Exception as e:
            print(e)
            raise DatabaseError("Clickhouse query failed")
        finally:
            client.disconnect()
        return df

    def ch_query_sync(self, query):
        """
        clickhouse 读写接口
        """
        client = Client(
            host=self._ch_host,
            port=self._ch_port,
            database=self._ch_database,
            user=self._ch_user,
            password=self._ch_password,
            settings={"mutations_sync": "1"},  # 将clickhouse mutation 操作由异步改为阻塞
        )
        try:
            query_id = str(uuid4())
            df = client.query_dataframe(query, query_id=query_id)
        except Exception as e:
            print(e)
            raise DatabaseError("Clickhouse sync query failed")
        finally:
            client.disconnect()
        return df

    def _ch_insert_data_numpy(self, df, table_name, database, insert_block_size=131072):
        """
        通过 numpy 将数据写入 clickhouse
        """
        client = Client(
            host=self._ch_host,
            port=self._ch_port,
            database=self._ch_database,
            user=self._ch_user,
            password=self._ch_password,
            settings={
                "insert_block_size": insert_block_size,
                "use_numpy": True,
                "send_logs_level": "debug",
            },
        )
        try:
            query_id = str(uuid4())
            query = f"INSERT INTO {database}.{table_name} VALUES"
            res = client.insert_dataframe(
                query, df, transpose=True, query_id=query_id)
        except Exception as e:
            print(e)
            raise DatabaseError("Clickhouse insert data by numpy failed")
        finally:
            client.disconnect()
        return res

    def _ch_insert_data_dict(self, df, table_name, database, insert_block_size=131072):
        """
        通过 dict 将数据写入 clickhouse
        """
        client = Client(
            host=self._ch_host,
            port=self._ch_port,
            database=self._ch_database,
            user=self._ch_user,
            password=self._ch_password,
            settings={"insert_block_size": insert_block_size},
        )
        try:
            query_id = str(uuid4())
            query = f"INSERT INTO {database}.{table_name} VALUES"
            res = client.execute(
                query, df.to_dict("records"), types_check=False, query_id=query_id
            )
        except Exception as e:
            print(e)
            raise DatabaseError("Clickhouse insert data by dict failed")
        finally:
            client.disconnect()
        return res

    def create_ch_table(
        self,
        df,
        table_name,
        database,
        datetime_cols: list = None,
        primary_key: list = None,
    ):
        """
        创建 clickhouse 表
        datetime_cols: 建的表中要指定为DateTime格式的列名列表
        primary_key: 主键列名列表，默认为前两列
        """

        def to_ch_type(array):
            if array["NULLABLE"] == 0 and array["LC"] == 0:
                return f"{array['DATA_TYPE']}"
            if array["NULLABLE"] == 0 and array["LC"] == 1:
                return f"LowCardinality({array['DATA_TYPE']})"
            if array["NULLABLE"] == 1 and array["LC"] == 0:
                return f"Nullable({array['DATA_TYPE']})"
            if array["NULLABLE"] == 1 and array["LC"] == 1:
                return f"LowCardinality(Nullable({array['DATA_TYPE']}))"

        df_meta = self.get_df_meta_info(df)
        df_meta["DATA_TYPE"] = df_meta["DATA_TYPE"].replace(DF_CH_TYPE_MAP)

        # 识别主键
        primary_key = df_meta.iloc[:2, 0].tolist(
        ) if primary_key is None else primary_key
        df_meta["NULLABLE"] = 1
        primary_key_mask = df_meta["COLUMN_NAME"].isin(primary_key)
        df_meta.loc[primary_key_mask, "NULLABLE"] = 0

        # 识别指定的DateTime列
        if datetime_cols is not None:
            datetime_cols_mask = df_meta["COLUMN_NAME"].isin(datetime_cols)
            df_meta.loc[datetime_cols_mask, "DATA_TYPE"] = "DateTime"

        # 大于100万行则采用低基数类型
        if len(df) > 1e6:
            df_meta["C_NUM"] = [len(df[col].unique()) for col in df.columns]
            df_meta["LC"] = df_meta.apply(
                lambda x: 1
                if (x["DATA_TYPE"] in ["String"]) and (x["C_NUM"] < 1e4)
                else 0,
                axis=1,
            )
        else:
            df_meta["C_NUM"] = 0
            df_meta["LC"] = 0

        # 映射到 ch 数据类型
        df_meta["CH_DATA_TYPE"] = df_meta.apply(
            lambda x: to_ch_type(x), axis=1)

        # 构建 sql 语句
        partial_sql = ""
        for _, r in df_meta.iterrows():
            column_name, ch_data_type = r.loc[["COLUMN_NAME", "CH_DATA_TYPE"]]
            partial_sql += f"\n`{column_name}` {ch_data_type},"
        partial_sql = partial_sql[:-1]  # 去掉最后一行的逗号

        primary_list = df_meta[df_meta["NULLABLE"]
                               == 0]["COLUMN_NAME"].tolist()
        order_by_str = ",".join(primary_list)

        sql = f"""
            CREATE TABLE {database}.{table_name}
            ({partial_sql})
            ENGINE = MergeTree ORDER BY ({order_by_str});
        """

        # 建表
        self.ch_query_sync(sql)

    def insert_df_to_ch(
        self,
        df,
        table_name,
        database,
        datetime_cols: list = None,
        primary_key: list = None,
    ):
        """
        DataFrame 写入 clickhouse
        datetime_cols: 建的表中要指定为DateTime格式的列所对应的列顺序编号
        primary_key: 主键列所对应的列顺序编号
        """
        table_exists = self.ch_query(
            f"EXISTS {database}.{table_name}").iloc[0, 0]

        # 如果待插入表已经存在
        if table_exists:
            ch_meta = self.get_ch_table_meta_info(
                table_name, database)  # 获取表元数据
            # 校验列的一致性
            if len(set(df.columns) - set(ch_meta["COLUMN_NAME"])) == 0:
                df = self.df_to_ch_type(df, ch_meta)  # 将 df 的列类型与 ch 做匹配
                self._ch_insert_data_dict(df, table_name, database)
                # try:
                #     self._ch_insert_data_numpy(df, table_name, database)  # 插入数据
                # except DatabaseError:
                #     self._ch_insert_data_dict(df, table_name, database)  # 插入数据

            else:
                raise ValueError("待写入数据与目标表不匹配")

        # 如果待插入表不存在
        else:
            self.create_ch_table(
                df,
                table_name,
                database,
                datetime_cols=datetime_cols,
                primary_key=primary_key,
            )  # 建表
            self._ch_insert_data_dict(df, table_name, database)
            # try:
            #     self._ch_insert_data_numpy(df, table_name, database)  # 插入数据
            # except DatabaseError:
            #     self._ch_insert_data_dict(df, table_name, database)  # 插入数据

    def get_ch_table_all(self, table_name, database):
        """
        获取完整的 clickhouse 表
        """
        return self.ch_query(f"SELECT * FROM {database}.{table_name}")

    def get_ch_table_meta_info(self, table_name, database):
        """
        获取 clickhouse 表的列元数据
        """
        meta_info = self.ch_query(
            f"""
            SELECT name COLUMN_NAME, type DATA_TYPE, is_in_primary_key PRIMARY_KEY
            FROM system.columns 
            WHERE database='{database}' AND table='{table_name}'
        """
        )
        meta_info["PURE_TYPE"] = [
            re.sub("LowCardinality|Nullable|\\(|\\)|", "", text)
            for text in meta_info["DATA_TYPE"]
        ]
        meta_info["NULLABLE"] = meta_info["DATA_TYPE"].apply(
            lambda x: 1 if "Nullable" in x else 0
        )
        meta_info.index.name = "clickhouse"
        return meta_info

    def get_df_meta_info(self, df):
        """
        获取 DataFrame 的列元数据
        """
        column_name = df.columns.tolist()
        data_type = [i.name for i in df.dtypes.to_list()]
        meta_info = pd.DataFrame(
            {"COLUMN_NAME": column_name, "DATA_TYPE": data_type})
        meta_info.index.name = "dataframe"
        return meta_info

    def df_to_ch_type(self, df, meta_info):
        """
        将 df 列转换成 clickhouse 对应的格式
        """
        for _, r in meta_info.iterrows():

            col, col_type = r.loc[["COLUMN_NAME", "PURE_TYPE"]]

            # float, int, uint 类型按 clickhouse 的格式进行转换
            if ("float" in col_type.lower()) or ("int" in col_type.lower()):
                df[col] = df[col].astype(col_type.lower())

            # date 和 datetime 类型转换成 datetime64
            elif "date" in col_type.lower():
                df[col] = pd.to_datetime(df[col])

            # 其他格式包括 string 转换成 str
            else:
                df[col] = df[col].astype(str)

        return df

    def drop_clickhouse_table(self, table_name, database):
        """
        删除 clickhouse 表
        """
        if input("删表操作！请输入 YES 确认：") == "YES":
            self.ch_query_sync(f"DROP TABLE IF EXISTS {database}.{table_name}")
            print(f"drop clickhouse table {database}.{table_name} success")

    def backup_ch_table(
        self, resource_table_name, resource_database, target_table_name, target_database
    ):
        """
        备份 clickhouse 表
        """
        self.ch_query_sync(f"CREATE DATABASE IF NOT EXISTS {target_database}")
        try:
            self.ch_query_sync(
                f"""
                CREATE TABLE {target_database}.{target_table_name} 
                as {resource_database}.{resource_table_name}
            """
            )
            self.ch_query_sync(
                f"""
                INSERT INTO {target_database}.{target_table_name} 
                SELECT * FROM {resource_database}.{resource_table_name}
            """
            )
            print(
                f"""backup ch table {resource_database}.{resource_table_name} to {target_database}.{target_table_name} success"""
            )
        except DatabaseError:
            print(
                f"backup ch table {resource_database}.{resource_table_name} to {target_database}.{target_table_name} failed"
            )

    def backup_ch_table_to_current_db(self, table_name, database):
        """
        备份 clickhouse 表到当前库
        """
        self.backup_ch_table(table_name, database,
                             f"{table_name}_backup", database)

    def backup_ch_database(self, database, target_database=None):
        """
        备份 clickhouse 整个库
        :param: database: 要备份的数据库
        :param: target_database: 备份的目标数据库，如果为None,则默认命名新数据库为原数据库名+backup
        """
        target_database = (
            f"{database}_backup" if target_database is None else target_database
        )
        self.ch_query_sync(f"CREATE DATABASE IF NOT EXISTS {target_database}")
        table_list = self.ch_query_sync(
            f"SELECT name FROM system.tables WHERE database='{database}'"
        )["name"].tolist()

        k = 1
        num = len(table_list)
        for table in table_list:
            self.ch_query_sync(
                f"CREATE TABLE {target_database}.{table} AS {database}.{table}"
            )
            self.ch_query_sync(
                f"INSERT INTO {target_database}.{table} SELECT * FROM {database}.{table}"
            )
            print(f"{k}/{num} {k / num * 100:.2f}% ", end="\r")
            k += 1
        print(f"backup database {database} to {target_database} success")
