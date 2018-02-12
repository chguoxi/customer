# -*- coding: utf-8 -*-
import pymysql
import glob
from conn import config


def chunks(arr, n):
    return [arr[i:i+n] for i in range(0, len(arr), n)]

if __name__=='__main__':
    print(config)
