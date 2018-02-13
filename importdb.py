# -*- coding: utf-8 -*-
#!python

import pymysql
import glob
from conn import config
import sys

'''
    获取文件列表
'''
def get_file_list(path):
    csvx_list = glob.glob(path+'/*.csv')
    return csvx_list

'''
    分割字符串

'''
def parse_line(line):
    row = line.split(',')
    return row

'''
    连接数据库
'''
def dbconn(config):
    db = pymysql.connect(**config)
    return db

'''
    拼凑SQL语句
'''
def build_sql(rows):
    rowcount = len(rows)
    basesql =  '''insert into `profile_traveler`(`name`,`card_no`,`descriot`,`ctftp`,`ctfid`,`gender`,`birthday`,`address`,`zip`,`dirty`,`district1`,`district2`,`district3`,`district4`,`district5`,`district6`,`firstnm`,`lastnm`,`duty`,`mobile`,`tel`,`fax`,`email`,`nation`,`taste`,`education`,`company`,`ctel`,`caddress`,`czip`) values'''
    sql = basesql
    
    for i in range(0,rowcount):
        row = rows[i]
        r = parse_line(row)
        #print(len(r))
        k = 0
        if len(r) ==33:
            sql += "("
            for val in r[0:30]:
                sql += "\""+val+"\""
                if k < 29:
                    sql += ","
                    k += 1
            sql += ")"
            if i < (rowcount-1):
                sql += ","
        else:
            continue

    
    return sql

'''
    拆分列表
'''
def chunks(arr, n):
    return [arr[i:i+n] for i in range(0, len(arr), n)]

def logd(filename,content):
    with open(filename,'w') as fh:
        fh.write(content)
        fh.close()

'''
    主程序入口
'''
if __name__=='__main__':
    flist = get_file_list('./csv')
    per_time_insert_count = 100
    db = dbconn(config)
    cursor = db.cursor()
    #print(db.charset)
    for csfile in flist:
        print('read file :'+csfile)
        with open(csfile,'r') as fh:
            rows = fh.readlines()
            rows = rows[1:]
            rowcount = len(rows)
            try:
                if rowcount > per_time_insert_count:
                    cks = chunks(rows,per_time_insert_count)
                    for ck in cks:
                        #print(ck)
                        sql = build_sql(ck)
                        cursor.execute(sql)
                        db.commit()
                    
                else:
                    sql = build_sql(rows)
                    #print(sql)
                    cursor.execute(sql)
                    db.commit()
                
            except Exception as e:
                print(sql)
                #print('Got error {!r}, errno is {}'.format(e, e.args[0]))
                # 发生错误时回滚
                #logd('./debug.log',sql)
                s = sys.exc_info()
                print "Error '%s' happened on line %d" % (s[1],s[2].tb_lineno)
        
