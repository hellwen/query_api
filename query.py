# coding=UTF-8
import sys
import conf
import util
from db import DB

class Query:

    def __init__(self, request):
        self.request = request

    def by_event(self):
        if not "event" in self.request:
            return ""

        event = self.request["event"]
        return " %s = '%s' " % (util.get_event_field(), event)

    def by_from(self):
        '''
        using rowkey
        '''
        if not "event" in self.request:
            return ""
        event = self.request["event"]

        if not "datetime" in self.request:
            return ""
        timestamp = self.request["datetime"]
        timestamp_start_str = self.request["datetime"]["start"]
        timestamp_start = util.datetime2timestamp(timestamp_start_str)
        timestamp_start = 9999999999999 - timestamp_start
        timestamp_end_str = self.request["datetime"]["end"]
        timestamp_end = util.datetime2timestamp(timestamp_end_str)
        timestamp_end = 9999999999999 - timestamp_end

        table = util.get_table()

        rowkey_low = "%s|%s" % (event.rjust(12, '0'), str(timestamp_start).ljust(13, '0'))
        rowkey_upper = "%s|%s" % (event.rjust(12, '0'), str(timestamp_end).ljust(12, '0'))

        rowkeys = """
            select * from %(table)s where rowkey <= '00|%(low)s' and rowkey >= '00|%(upper)s' 
            union all select * from %(table)s where rowkey <= '01|%(low)s' and rowkey >= '01|%(upper)s' 
            union all select * from %(table)s where rowkey <= '02|%(low)s' and rowkey >= '02|%(upper)s' 
            union all select * from %(table)s where rowkey <= '03|%(low)s' and rowkey >= '03|%(upper)s' 
            union all select * from %(table)s where rowkey <= '04|%(low)s' and rowkey >= '04|%(upper)s' 
            union all select * from %(table)s where rowkey <= '05|%(low)s' and rowkey >= '05|%(upper)s' 
            union all select * from %(table)s where rowkey <= '06|%(low)s' and rowkey >= '06|%(upper)s' 
            union all select * from %(table)s where rowkey <= '07|%(low)s' and rowkey >= '07|%(upper)s' 
            union all select * from %(table)s where rowkey <= '08|%(low)s' and rowkey >= '08|%(upper)s' 
            union all select * from %(table)s where rowkey <= '09|%(low)s' and rowkey >= '09|%(upper)s' 
        """ % {"table": table, "low": rowkey_low, "upper": rowkey_upper}
        return rowkeys

    def by_attrs(self):
        if not "attrs" in self.request:
            return ""

        s = ""
        for a in self.request["attrs"]:
            if "as" in a.keys():
                s = (s + "\n," if s != "" else "") + "%s as %s" % (a["attr"], a["as"])
            else:
                s = (s + "\n," if s != "" else "") + a["attr"]

        return s

    def by_dimension(self):
        if not "dimension" in self.request:
            return {}

        dimension = self.request["dimension"]
        s_havef = ""
        s_notf = ""
        for i in dimension:
            if "func" in i:
                attr = util.field2name(str(i["attr"]))
                func = util.field2name(i["func"])
                if func == "distinct":
                    s_havef += ("" if s_havef == "" else ",") + "count(distinct %(attr)s) as %(attr)s_%(func)s" % {"func": func, "attr": attr}
                else:
                    s_havef += ("" if s_havef == "" else ",") + "%(func)s(%(attr)s) as %(attr)s_%(func)s" % {"func": func, "attr": attr}
            else:
                s_notf += ("" if s_notf == "" else ",") + str(i["attr"])

        s_havef = (s_havef[:-1] if s_havef[-1:len(s_havef)] == "," else s_havef)
        s_notf = (s_notf[:-1] if s_notf[-1:len(s_notf)] == "," else s_notf)
        return {"havef": s_havef, "notf": s_notf}

    def by_datetime(self):
        if not "datetime" in self.request:
            return {}

        timestamp = self.request["datetime"]
        s = "%(timestamp)s between \"%(start)s\" and \"%(end)s\"" % {
                 "timestamp": util.get_field("timestamp"),
                 "start": timestamp["start"],
                 "end": timestamp["end"],
                 }

        return (s[:-1] if s[-1:len(s)] == "," else s)

    def by_datetime_aggregate(self):
        if not "datetime" in self.request or not "aggregate" in self.request["datetime"]:
            return {}

        aggregate = self.request["datetime"]["aggregate"]
        s_havef = ""
        s_notf = ""
        if aggregate == "day":
            s_notf = "day(%(time)s)" % {"time": util.get_field("timestamp")}
            s_havef = "day(%(time)s) as %(time)s_day" % {"time": util.get_field("timestamp")}

        s_havef = (s_havef[:-1] if s_havef[-1:len(s_notf)] == "," else s_havef)
        s_notf = (s_notf[:-1] if s_notf[-1:len(s_notf)] == "," else s_notf)
        return {"notf": s_notf, "havef": s_havef}

    def by_where(self):
        if not "where" in self.request:
            return ""

        where = self.request["where"]
        s = ""
        t = where["type"]
        d = where["data"]
        for w in d:
            vs = ""
            for v in w["value"]:
               vs += ("" if vs == "" else ",") + "\"%s\"" % v
            op = w["op"]
            if op == "in":
                s += " %s " % ("" if s == "" else t) + \
                        "%(attr)s %(op)s (%(value)s)" % {"attr": w["attr"],"op": w["op"], "value": vs}
            else:
                s += " %s " % ("" if s == "" else t) + \
                        "%(attr)s %(op)s %(value)s" % {"attr": w["attr"],"op": w["op"], "value": vs}
        return (s[:-1] if s[-1:len(s)] == "," else s)

    def by_case(self):
        if not "case" in self.request:
            return ""

        case = self.request["case"]
        s = ""
        for c in case:
            ci = ""
            attr = c["attr"]
            value_attr = c.get("value_attr")
            #if value_attr == "":
            #    value_attr = util.get_event_field()
            func = c.get("func")
            if func == "sum":
                pass
            else:
                for inc in c["includes"]:
                    ci += "\nsum(case %(attr)s when \"%(inc)s\" then 1 else 0 end) as %(attr)s_%(inc)s," % {"attr": attr, "value_attr": value_attr, "func": func, "inc": util.field2name(inc)}
            s += ci

        return (s[:-1] if s[-1:len(s)] == "," else s)

    def by_limit(self):
        if not "limit" in self.request:
            return ""

        return "limit %s" % self.request["limit"]

    def parse(self):
        event = self.by_event()
        fromt = self.by_from()
        attrs = self.by_attrs()
        dimension = self.by_dimension()
        datetime = self.by_datetime()
        aggregate = self.by_datetime_aggregate()
        where = self.by_where()
        case = self.by_case()
        limit = self.by_limit()

        s = "select"
        if attrs != "":
            s += " %(attrs)s,"
        if "notf" in dimension.keys() and dimension["notf"] != "":
            s += " %(dimension_notf)s,"
        if "havef" in dimension.keys() and dimension["havef"] != "":
            s += " %(dimension_havef)s,"
        if "havef" in aggregate.keys() and aggregate["havef"] != "":
            s += " %(aggregate_havef)s,"
        if s[-1:len(s)] == ",":
            s = s[:-1]

        if case != "":
            s += ",%(case)s"

        s += "\nfrom (%(from)s) as ta"
        s += "\nwhere %(event)s"
        if datetime != "":
            s += " \nand %(datetimes)s"
        if where != "":
            s += " \nand %(wheres)s"
        if "notf" in dimension.keys() or "notf" in aggregate.keys():
            s += "\ngroup by"
        if "notf" in dimension.keys() and dimension["notf"] != "":
            s += " %(dimension_notf)s,"
        if "notf" in aggregate.keys() and aggregate["notf"] != "":
            s += " %(aggregate_notf)s,"
        if s[-1:len(s)] == ",":
            s = s[:-1]

        if limit != "":
            s += "\n%(limit)s"

        # print(s)
        # print("")

        sql = s % {
            "event": event,
            "from": fromt,
            "attrs": attrs,
            "dimension_notf": dimension.get("notf"),
            "dimension_havef": dimension.get("havef"),
            "datetimes": self.by_datetime(),
            "aggregate_havef": aggregate.get("havef"),
            "aggregate_notf": aggregate.get("notf"),
            "wheres": where,
            "case": case,
            "limit": limit,
            }

        return sql

    def execute(self):
        db = DB()
        rst = db.execute(self.parse())
        return rst
