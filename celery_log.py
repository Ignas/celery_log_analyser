#!/usr/bin/env python
import sys
import os
import json
import time
from collections import defaultdict
import sqlite3


def load(log_filenames):
    cur = sqlite3.connect(':memory:', detect_types=sqlite3.PARSE_DECLTYPES).cursor()
    schema = """CREATE TABLE items (dt float,
                                    dt1 integer,
                                    dt5 integer,
                                    dt10 integer,
                                    dt30 integer,
                                    type, task_name, task_id, seconds float)"""
    sql = 'INSERT INTO items VALUES ({0})'.format(','.join('?' for _ in range(9)))
    cur.execute(schema)
    cur.connection.commit()
    for log_filename in log_filenames:
        log_lines = open(log_filename).readlines()
        log_lines = filter(lambda l: l.startswith('['), log_lines)
        log_lines = [line.split(' ') for line in log_lines]

        for line in log_lines:
            if len(line) < 3:
                continue
            if line[2] == "ERROR/MainProcess]":
                record_type = 'failed'
                task_name = line[4].split('[')[0]
                task_id = line[4].split('[')[1][:-1]
                seconds = 3000
            elif line[3] == 'Task':
                record_type = 'finished'
                task_name = line[4].split('[')[0]
                task_id = line[4].split('[')[1][:-1]
                seconds = float(line[7][:-2])
            elif line[3] == 'Got':
                record_type = 'started'
                task_name = line[7].strip().split('[')[0]
                task_id = line[7].strip().split('[')[1][:-1]
                seconds = 0
            else:
                continue
            timestamp = ' '.join(line[0:2])[1:-1]
            ptime = time.strptime(timestamp, '%Y-%m-%d %H:%M:%S,%f')
            ts = time.mktime(ptime)
            dt = ts
            dt1 = int(ts / 60) * 60
            dt5 = int(ts / 300) * 300
            dt10 = int(ts / 600) * 600
            dt30 = int(ts / 1800) * 1800
            cur.execute(sql, [dt, dt1, dt5, dt10, dt30, record_type, task_name, task_id, seconds])
            cur.connection.commit()
    return cur


def dump_series(current_interval,
                series,
                file_list,
                force=False):
    out_file_name = "%s.js" % current_interval
    if force or not os.path.exists("output/data/" + out_file_name):
        with open("output/data/" + out_file_name, "w") as out_file:
            for name, data in series.items():
                out_file.write('%s = %s.concat(%s);\n' % (name, name, json.dumps(data)))
    file_list.write("""$('head').append('<script src="data/%s"></script>');\n""" % out_file_name)


def analyze(log_filenames):
    cur = load(log_filenames)
    data = {}
    keys = [k[0] for k in cur.execute("select dt1 from items group by dt1 order by dt1")]

    data = defaultdict(lambda:defaultdict(int))
    data['started'].update(cur.execute("select dt1, count(*) from items where type = 'started' group by dt1 order by dt1"))
    data['finished'].update(cur.execute("select dt1, count(*) from items where type = 'finished' group by dt1 order by dt1"))
    data['failed'].update(cur.execute("select dt1, count(*) from items where type = 'failed' group by dt1 order by dt1"))
    data['difference'].update([(stamp, data['started'][stamp] - data['finished'][stamp] - data['failed'][stamp])
                               for stamp in keys])
    data['total'] = []
    total = 0
    for key in keys:
        total = max(0, total + data['difference'][key])
        data['total'].append((key, total))
    data['total'] = dict(data['total'])

    lag_subquery = ("(select max(dt1) dt1,"
                    "        max(0, max(dt) - min(dt) - max(seconds)) lag,"
                    "        max(seconds) seconds"
                    " from items group by task_id order by dt)")

    data['time_spent_avg'].update(cur.execute(
            "select dt1, avg(seconds)"
            " from items where type = 'finished' group by dt1 order by dt1"))
    data['lag_avg'].update(cur.execute(
            "select dt1, avg(lag) from " +
            lag_subquery + " group by dt1"))

    data['time_spent_max'].update(cur.execute(
            "select dt1, max(seconds)"
            " from items where type = 'finished' group by dt1 order by dt1"))
    data['lag_max'].update(cur.execute(
            "select dt1, max(lag) from " +
            lag_subquery + " group by dt1"))

    data['time_spent'].update(cur.execute("select dt1, sum(seconds)"
                                          " from items where type = 'finished' group by dt1 order by dt1"))
    data['lag'].update(cur.execute("select dt1, sum(lag) from " +
                                   lag_subquery + " group by dt1"))

    series = {}
    for name in data.keys():
        series[name] = []

    total = 0
    current_interval = None
    file_list = open('output/data/timestamps.js', 'w')
    file_list.write("\n".join(["var %s = [];" % name for name in data.keys()]))

    for key in keys:
        dt1 = key
        interval_name = str(dt1 / 86400)
        if interval_name != current_interval:
            if current_interval:
                dump_series(current_interval,
                            series, file_list)
            current_interval = interval_name
            # Reset series
            for name in data.keys():
                series[name] = []

        stamp = dt1 * 1000
        for key in series:
            series[key].append([stamp, data[key][dt1]])

    dump_series('remainder',
                series,
                file_list,
                force=True)
    file_list.close()

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Pass the path to the log"
    analyze(sys.argv[1:])
