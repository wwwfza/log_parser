#!/usr/bin/env:: python
# -*- coding: utf-8 -*-
"""
log_parser.py
parse access_register_logs & access_tag_logs ad update table "app_dimension" in db "message" 
    
author fanzhengang@umeng.com 2013-10-11
---------------------------------------
"""
import pdb,traceback
import time,sys,os
import re
import ConfigParser 
import MySQLdb

col_names = ["tag","app_version","channel","time_zone"]

def get_exc_info():
    type,value,tb = sys.exc_info()
    return str(traceback.format_exception(type,value,tb))

#get needed information
#log line => info {'appkey': '5228291356240bec3b098acc', 'versions': '1.2.1', 'channels': 'xxx'}
def parse_log_line(pattens,string):
    info = {}
    for pattern in patterns:
        match = patterns[pattern].search(string)        
        if match:
            info[pattern] = match.group(1)
    return info

#format information according to conf file [re] names and sequence
#info => result {'5228291356240bec3b098acc': {'tags': '', 'versions': '1.2.1', 'channels': 'xxx'}}
def merge_log_line(info,col_names,result):
    key = info.get("appkey")
    if key is None:
        return
 
    if result.has_key(key):
        for name in col_names:
            old_values = result[key][name].split(",")
            new_values = info.get(name,"").split(",")
            final_values = list(set(old_values).union(set(new_values)))
            final_values = [ i for i in final_values if i != '']
            result[key][name] = ",".join(sorted(final_values))
    else:
        cols = {} 
        for name in col_names:
            cols[name] = info.get(name,"") 
        result[key] = cols

def update_db(db,col_names,result):
    try:
        conn = MySQLdb.connect(host=db["host"],user=db["user"],passwd=db["passwd"],db=db["db"],charset='utf8')
        cursor = conn.cursor()

        for key in result:
            cols = result[key]
            select_cols = ",".join(col_names)
            r = cursor.execute("select %s from %s where appkey = '%s'" % (select_cols,db["table"],key))
            if r == 0:
                #new row
                value = "'%s',%s" % (key,",".join(["'%s'"%cols[i] for i in col_names]))
                cmd = "insert into %s values(%s)" % (db["table"],value)
                cursor.execute(cmd)
                print cmd
            elif r == 1:
                #update row
                db_row = cursor.fetchone()
                value_ls = [] 
                for i in range(0,len(col_names)):
                #i+1 means ig
                    old_value = db_row[i].encode("utf8").split(",")
                    new_value = list(set(old_value).union(set(cols[col_names[i]].split(","))))
                    final_value = ",".join([ v for v in new_value if v != '']) 
                    if final_value != '':           
                        value_ls.append("%s='%s'"%(col_names[i],final_value))
                
                cmd = "update %s set %s where appkey='%s'" % (db["table"],",".join(value_ls),key)
                cursor.execute(cmd)
                print cmd 
        conn.commit()
        cursor.close()
        conn.close()
    except MySQLdb.Error,e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print "error, must specified target log file!"
        sys.exit(-1)
    log_file = sys.argv[1]
    conf_path = sys.argv[0][:].replace("log_parser.py","log_parser.conf")
    if not os.path.exists(conf_path):
        print "error, need configuration file!"
        sys.exit(-1);
    cf = ConfigParser.ConfigParser()
    cf.read(conf_path)
    db = {}
    for name,value in cf.items("db"):
        db[name] = value
    patterns = {}
    col_names = []
    for name,value in cf.items("re"):
        patterns[name] = re.compile(value)
        if(name != 'appkey'):
            col_names.append(name)

    try:
        reload(sys)
        sys.setdefaultencoding('utf-8')
        start_time = time.clock()
        print "start parsing log %s"%(log_file)
        result = {}        
        fd = open(log_file,"r")
        try:
            for line in fd.readlines():
                info = parse_log_line(patterns,line.decode('string_escape'))
                merge_log_line(info,col_names,result)
            pdb.set_trace()
            update_db(db,col_names,result)
        finally:
            fd.close()
        print "end parsing, time: %fs\n" % (time.clock()-start_time)       
    except:
        print get_exc_info()

 
