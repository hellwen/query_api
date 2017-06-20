#coding=utf-8
from impala.dbapi import connect

class DB:

    def __init__(self, host="", port=""):
        self.conn = connect(host='10.0.12.251', port=21050)

    def execute(self, sql):
        cursor = self.conn.cursor()
        cursor.execute(sql)
        dataset = cursor.fetchall()

        result = {"title": [desc[0] for desc in cursor.description],
                "data": dataset,
                }

        cursor.close()
        return result

    def __del__(self):
        self.conn.close()
