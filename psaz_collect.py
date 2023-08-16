import requests, sys, os, csv, atexit, json, shutil, configparser
import time as t
import string
from json import loads, dumps
from datetime import datetime

config = configparser.ConfigParser()
try:
    config.read(sys.argv[1])
except:
    with open(sys.argv[1], 'r+') as configfile:
        conf = configfile.read()
        configfile.seek(0)
        configfile.write("[sec]\n"+conf)
        configfile.close()
        config.read(sys.argv[1])

sec = config.sections()[0]
data_dir = config[sec]['data_dir']
isize = int(config[sec]['data_directory_isize'])
interval = int(config[sec]['data_collection_interval'])
reten = int(config[sec]['data_retention'])

# with open(sys.argv[1]) as config_data:
#     configjson=json.load(config_data)
#     data_dir=configjson['data_dir']
#     isize=configjson['data_directory_isize']
#     interval=configjson['data_collection_interval']
#     reten=configjson['data_retention']

def read_counter():
    if os.path.exists("psaz_map.json"):
        data= loads(open("psaz_map.json", "r").read())
        count= int(sorted(data.keys())[-1])+1
        # print(count)
        return count
    else:
        return 1
def make_dir():
    x = read_counter()
    dir= "psaz_data."+str(x)
    path = data_dir+"/"+dir
    if not os.path.exists(path):
        os.makedirs(path)
    return path
def collect_data():

    collect= ["cpu", "percpu", "mem", "memswap", "processcount", "processlist", "load", "diskio", "fs", "sensors"]

    for para in collect:
        API_url = 'http://localhost:61208/api/3/'+para
        response= requests.get(API_url)
        data_json= response.json()
        if type(data_json)!= list:
            data_json= [data_json]

        path= make_dir()
        fname= para+".csv"

        for x in range(len(data_json)):
            csv_h = []
            csv_v = []
            now = datetime.now()
            csv_v.append(now)
            csv_h.append("Timestamp")
            for key, value in data_json[x].items():
                if para == "processlist" and type(value)==list:
                    if key=="cpu_times":
                        csv_v.extend(value)
                        if len(value) == 5:
                            csv_h.extend(["user", "system", "children_user", "children_system", "iowait"])
                        else:
                            csv_h.extend(["user", "system", "children_user", "children_system"])
                    elif key == "io_counters":
                        csv_h.extend(["readio", "writeio"])
                        csv_v.extend([value[0]-value[2], value[1]-value[3]])
                    elif key == "memory_info":
                        # csv_h.extend(["rss", "vms", "shared", "text", "lib", "data", "dirty"])
                        csv_h.extend(["rss", "vms", "shared", "text", "data"])
                        for x in range(5):
                            csv_v.append(value[x])
                    elif key == "gids":
                        csv_v.extend(value)
                        csv_h.extend(["real", "effective", 'saved'])
                    else:
                        csv_h.append(key)
                        csv_v.append(value)
                else:
                    csv_h.append(key)
                    csv_v.append(value)

            if os.path.exists(path+"/"+fname):
                with open(path+"/"+fname, 'a', encoding="UTF8", newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(csv_v)
            else:
                with open(path+"/"+fname, 'w', encoding="UTF8", newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(csv_h)
                    writer.writerow(csv_v)
    print ("done")
def data_coll (int, isize):
    count=0
    count_reached= True
    while (count_reached):
        collect_data()
        count += 1
        t.sleep(int)
        if count== isize:
            count_reached=False
    return 1
def write_counter(time):
    counter = read_counter()
    if os.path.exists("psaz_map.json"):
        with open("psaz_map.json", 'r+') as f:
            data = json.loads(f.read()) #data becomes a dictionary
            data[counter] = time
            f.seek(0)
            json.dump(data,f,indent=4)
            f.truncate()
    else:
        with open("psaz_map.json", "w") as f:
            data= {int(counter):time}
            f.write(dumps(data))
def data_reten():
    count = read_counter()-1
    path=data_dir+"/psaz_data."
    if count-reten>0:
        for i in range(1,(count-reten+1)):
            if os.path.exists(path+str(i)):
                #os.rmdir(path+str(i))
                shutil.rmtree(path+str(i))
def exit():
    count=read_counter()
    path = data_dir + "/psaz_data."
    if os.path.exists(path+str(count)):
        #os.rmdir(path+str(count))
        shutil.rmtree(path+str(count))

atexit.register(exit)

while(True):
    exit()
    time= t.time() #beginning of a group
    data_coll(interval, isize)
    write_counter(time) #appending time of creation to psaz_map after data has been fully collected for the specified group
    data_reten()
