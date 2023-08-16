import sys
import os
import time
import json
import pandas as pd
import numpy as np
from datetime import datetime, date

path= "/psaz_data_groups1"

cpu= ["ctx_switches", "idle", "interrupts", "iowait", "irq", "syscalls", "system", "total", "user", "cpucore"]
percpu= ['cpu_number', 'idle', 'iowait', 'irq', 'system', 'total', 'user']
mem=['active', 'available', 'buffers', 'cached', 'free', 'inactive', 'percent', 'shared', 'total', 'used']
memswap=['total', 'used', 'free', 'percent', 'sin', 'sout']
processcount=['pid_max', 'running', 'sleeping', 'thread', 'total']
load=['min1', 'min5', 'min15', 'cpu_core']
diskio=['disk_name', 'read_bytes', 'read_count', 'write_bytes', 'write_count']
fs=['fs_type', 'mnt_point', 'size','free', 'used', 'percent']
sensors=['label', 'type', 'unit']
processlist=["cmdline", "cpu_percent",'user', 'system', 'children_user', 'children_system', "readio", "writeio",'rss', 'vms', 'shared', 'text', 'data', "memory_percent", 'name', 'num_threads', 'status']

list_para=[cpu, percpu, mem, memswap, processlist, processcount, load, diskio, fs, sensors]
arg_list= str(sys.argv)
def read_cat():
    if "--process" in arg_list or "--pid" in arg_list:
        x= "processlist"
    elif "--cpu" in arg_list:
        x = "percpu"
    else:
        x= sys.argv[1]
        cat = ["cpu", "percpu", "mem", "memswap", "processcount", "processlist", "load", "diskio", "fs", "sensors"]
        if x in cat:
            return(x)
    return(x)
def read_time():
    if len(sys.argv) >=2:

        if "--start" in arg_list:
            x= 0
        elif "--last" in arg_list:
            i = arg_list[arg_list.index("--last") + 1]
            if "m" in i:
                m = i[:i.index("m")]
                x = int(m) * 60
            elif "h" in i:
                h = i[:i.index("h")]
                x = int(h) * 60 * 60
            elif "d" in i:
                d = i[:i.index("d")]
                x = int(d) * 24 * 60 * 60
        else:
            x= 30*60
        if "--granularity" in arg_list:
            i = arg_list[arg_list.index("--granularity") + 1]
            if "m" in i:
                m = i[:i.index("m")]
                g = int(m) * 60
            elif "h" in i:
                h = i[:i.index("h")]
                g = int(h) * 60 * 60
            elif "d" in i:
                d = i[:i.index("d")]
                g = int(d) * 24 * 60 * 60
        else:
            g= 5*60
    else:
        x = 1800
        g= 300
    return x, g
def start_end():
    if len(sys.argv) >=2:
        arg_list= str(sys.argv)
        if "--start" in arg_list:
            i = arg_list[arg_list.index("--start") + 1]
            s = datetime.strptime(sys.argv[i], '%Y-%m-%d:%H:%M')
            se= s.timestamp()
        if "--end" in arg_list:
            i = arg_list[arg_list.index("--end") + 1]
            e = datetime.strptime(sys.argv[i], '%Y-%m-%d:%H:%M')
            ee = e.timestamp()
    return se, ee
def agg_df():
    #retrieve indexes from time and compile a csv
    time = t_now
    path = "psaz_data_groups1"
    df_agg = pd.DataFrame()
    file_paths = []
    t, g = read_time()

    if os.path.exists("psaz_map1.json"):
        data = json.loads(open("psaz_map1.json", "r").read())
        grps = sorted(data.keys())
        tstamps = sorted(data.values())

    if t == 0:
        se, ee= start_end()
        t= ee-se
        find = se
        find_end= ee
    else:
        if time-t<tstamps[-1]-t:
            find= tstamps[-1]-t
        else:
            find = time - t
        find_end=0

    for x in reversed(range(len(tstamps))):
        if find > tstamps[x]:
            grp_ind_s = x
            break
        else:
            return 0
    for x in reversed(range(len(tstamps))):
        if find_end<tstamps[x]:
            grp_ind_e = x+1
            break
        else:
            return 0
    if tstamps[grp_ind_e]-tstamps[grp_ind_s]>=t:
        for x in range(grp_ind_s, grp_ind_e):
            cat = read_cat()
            csv_path = path + "/psaz_data." + str(x) + "/" + cat + ".csv"
            file_paths.append(csv_path)

        df_agg = pd.concat(map(pd.read_csv, file_paths))
        return df_agg
    else:
        return 0
def time_gran():
    time= t_now
    df= avg_df()
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], format='%Y-%m-%d %H:%M:%S')
    t, g= read_time()
    if t == 0:
        se, ee = start_end()
        t = ee - se
        find = se
        find_end = ee
    else:
        find = time - t
        find_end = time

    for x in reversed(range(df.shape[0])):
        if find <= df['Timestamp'][x].timestamp():
            ind_s= x
            break
    for x in reversed(range(df.shape[0])):
        if find_end >= df['Timestamp'][x].timestamp():
            ind_e = x
            break

    ind_list = [-1]
    for x in reversed(range(ind_s,ind_e+1)):
        i = int(ind_list[-1])
        ts = df['Timestamp'].iloc[i] - df['Timestamp'].iloc[x]
        ts = ts.total_seconds()
        if ts >= g:
            ind_list.append(x)
    return ind_s, ind_e, ind_list
def avg_df():
    cat=read_cat()
    df = agg_df.copy()
    for x in range(len(list_para)):
        v = [i for i, a in locals().items() if a == list_para[x]][0]
        i = x
        if cat == v:
            break
    spec_cols = []
    for c in arg_list:
        ind = arg_list.index(c) + 1
        if "-" in c:
            c = c.replace("-", "")
            print(c)
        if c == "cpu":
            c = "cpu_number"
        if c in list_para[i]:
            spec_cols.append(c)
            col = c
            if arg_list[ind].isdigit():
                type_col = int(arg_list[ind])
            else:
                type_col = arg_list[ind]
            break

    if spec_cols:
        df = df.groupby(col)
        df= df.get_group(type_col).reset_index().drop("index", axis=1)

    for para in columns:
        if para not in list_para[i]:
            df.drop([para], axis=1)

    return df




    # a, b= time_gran()
    # for x in to_be_analysed:
    #     tba=np.array_equal(df[x], df[x].astype(float))
def stats_df():
    df = avg_df()
    ind_s, ind_e, ind_list= time_gran()
    t, g= read_time()
    g, t= g/60, t/60

    not_avg = ["cpu_number"]
    dict = {'Type': [f'{t} mins']}
    columns = list(df.columns.values)
    for i in columns:
        try:
            df[i].mean()
        except:
            continue
        if i not in not_avg:
            x = df[i][ind_s:ind_e + 1].mean()
            dict[i] = [x]
        # else:
        #     dict[i]=['-']
        for ind in range(len(ind_list)):
            if i not in not_avg:
                if ind == 0:
                    s = 0
                else:
                    s = ind_list[ind - 1] + 1
                e = ind_list[ind] + 1
                x = df[i][s:e + 1].mean()
                dict[i].append(x)
            # else:
            #     dict[i].append('-')
            if len(dict['Type']) <= (len(ind_list)):
                dict["Type"].append(f'{ind * g}:{(ind + 1) * g} min')

    stat_df = pd.DataFrame(dict)
    return(stat_df)

t_now= time.time()
if agg_df():
    df= agg_df()
else:
    raise SystemExit('Not enough data available')
columns= list(df.columns.values)
stat_df= stats_df()
print(stat_df)