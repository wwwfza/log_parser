#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
log_parser_register.py
parse logs and update to db
    
author fanzhengang@umeng.com 2013-10-11
---------------------------------------
1.revised 2013-11-07
decode the log by string_escape before match
and encode the record from db into utf8
all the string use utf8

"""
import pdb,traceback
import time,sys,os
import re
import ConfigParser 
import MySQLdb

def getExcInfo():
    type,value,tb = sys.exc_info()
    return str(traceback.format_exception(type,value,tb))

log_path = ""		
def log(info,level = 'trace'):
    fd = open(log_path,"a")
    mes = "[%s][%s][%s]\n" % (time.asctime(),level,info)  
    fd.write(mes)
    fd.close()

def log_parse(pattens,string):
    result = {}
    for pattern in patterns:
        match = patterns[pattern].search(string)        
        if match:
            result[pattern] = match.group(1)
    return result

def result_parse(info,result):
    key = info.get("appkey")
    if key is None:
        return
    tag = info.get("tag","")
    version = info.get("app_version","")
    channel = info.get("channel","")
    col_names = ["tag","version","channel"]   
 
    if result.has_key(key):
        cols = result[key]
        for name in col_names:
            value_list = cols[name].split(",")
            value_list = list(set(value_list).union(set(eval(name).split(","))))
            value_list = [ i for i in value_list if i != '']
            cols[name] = ",".join(sorted(value_list))
       # tags = cols["tag"].split(",")
       # if tag not in tags:
       #     tags = [i for i in tags if i != ""]
       #     tags.append(tag)
       #     cols["tag"] = ",".join(sorted(tags))

    else:
        cols = {} 
        for name in col_names:
            cols[name] = eval(name)
       # cols['tag'] = tag
       # cols["version"] = version
       # cols["channel"] = channel
        result[key] = cols

def update_db(db,result):
    try:
        conn = MySQLdb.connect(host=db["host"],user=db["user"],passwd=db["passwd"],db=db["db"],charset='utf8')
        cursor = conn.cursor()

        for key in result:
            cols = result[key]
            r = cursor.execute("select * from %s where appkey = '%s'" % (db["table"],key))
            if r == 0:
                #new row
                cursor.execute("insert into %s values('%s','%s','%s','%s')" % (db["table"],key,cols["tag"],cols["version"],cols["channel"]))
                log("insert %s{appkey:'%s',tags:'%s',versions:'%s',channels:'%s'})" % (db["table"],key,cols["tag"],cols["version"],cols["channel"]))
            elif r == 1:
                #update row
                db_row = cursor.fetchone()
                
                old_tags = db_row[1].encode("utf8").split(",")
                new_tags = list(set(old_tags).union(set(cols["tag"].split(","))))
                tag = ",".join([ i for i in new_tags if i != ''])            
                old_versions = db_row[2].encode("utf8").split(",")
                new_versions = list(set(old_versions).union(set(cols["version"].split(","))))
                version = ",".join([ i for i in new_versions if i != ''])
                old_channels = db_row[3].encode("utf8").split(",")
                new_channels = list(set(old_channels).union(set(cols["channel"].split(","))))
                channel = ",".join([ i for i in new_channels if i != '']) 
                
                cursor.execute("update %s set tags='%s',versions='%s',channels='%s' where appkey= '%s'" % (db["table"],tag,version,channel,key))
                log("update %s{appkey:'%s',tags:'%s',versions:'%s',channels:'%s'}" % (db["table"],key,tag,version,channel))
       
        conn.commit()
        cursor.close()
        conn.close()
    except MySQLdb.Error,e:
        log("Mysql Error %d: %s" % (e.args[0], e.args[1]))

if __name__ == "__main__":
    if len(sys.argv) < 2:
        mes = "error, missing log path parameter!"
        log(mes)
        print mes    
        sys.exit(-1)
    log_file = sys.argv[1]
    conf_path = sys.argv[0][:].replace("log_parser.py","log_parser.conf")
    log_path = sys.argv[0][:].replace("log_parser.py","log_parser.log")
    if not os.path.exists(conf_path):
        mes = "error, missing regular configuration files!"
        log(mes)
        print mes
        sys.exit(-1);
    cf = ConfigParser.ConfigParser()
    cf.read(conf_path)
    db = {}
    for name,value in cf.items("db"):
        db[name] = value
    patterns = {}
    for name,value in cf.items("re"):
        patterns[name] = re.compile(value)

    try:
        reload(sys)
        sys.setdefaultencoding('utf-8')
        start_time = time.clock()
        log("start parsing log %s"%(log_file))
        result = {}        
        fd = open(log_file,"r")
        try:
            for line in fd.readlines():
                info = log_parse(patterns,line.decode('string_escape'))
                result_parse(info,result)
            #pdb.set_trace()
            update_db(db,result)
        finally:
            fd.close()
        log("end parsing, time: %fs" % (time.clock()-start_time))        
    except:
        log(getExcInfo())

 
