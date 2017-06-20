#coding=utf-8
import time

def field2name(field):
    return field.lower().replace(" ", "_").replace("-", "_")

def get_field(name):
    fields = {
            "version": "c_version",
            "timestamp": "c_timestamp",
            }

    return fields[name]

def get_table():
    return "hevents_expand"

def get_event_field():
    return "c_eveid"

def datetime2timestamp(dt):
    if len(dt) <= 10:
        dt = dt + " 00:00:00"
    f = '%Y-%m-%d %H:%M:%S'
    time.strptime(dt, f)
    ## time.struct_time(tm_year=2012, tm_mon=3, tm_mday=28, tm_hour=6, tm_min=53, tm_sec=40, tm_wday=2, tm_yday=88, tm_isdst=-1)
    s = time.mktime(time.strptime(dt, f))
    return int(s)*1000
