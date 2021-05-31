#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2
import math
DATABASE_NAME = 'dds_assgn1'


def getopenconnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(ratingstablename, ratingsfilepath, openconnection):
    cur = openconnection.cursor()
    cur.execute("create table " + ratingstablename + "(userid int, colon1 char, movieid int, colon2 char, rating float, colon3 char, timestamp bigint)")
    filepath = open(ratingsfilepath,'r')
    cur.copy_from(filepath, ratingstablename, sep=':')
    cur.execute("alter table " + ratingstablename + " drop column colon1, drop column colon2, drop column colon3, drop column timestamp")
    cur.close()
    openconnection.commit()



def rangepartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    max_rating = 5
    interval = float(max_rating)/float(numberofpartitions)
    for i in range(0,numberofpartitions):
        lower_bound = interval*i
        upper_bound = lower_bound+interval
        table = 'range_part' + str(i)
        if i == 0:
            cur.execute("create table " + table + " as select * from " + ratingstablename + " where rating>=" + str(lower_bound) + " and rating<=" + str(upper_bound))
        else:
            cur.execute("create table " + table + " as select * from " + ratingstablename + " where rating>" + str(lower_bound) + " and rating<=" + str(upper_bound))
    cur.close()
    openconnection.commit()



def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    for i in range(0,numberofpartitions):
        table = 'rrobin_part' + str(i)
        cur.execute("create table " + table + "(userid int, movieid int, rating float)")
    cur.execute("select * from " + ratingstablename)
    res = cur.fetchall()
    i = 0
    for rows in res:
        table = 'rrobin_part' + str(i)
        cur.execute("insert into " + table + " values(%d,%d,%f)"%(rows[0],rows[1],rows[2]))
        i+=1
        i = i % numberofpartitions
    cur.close()
    openconnection.commit()


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    cur.execute("select count(*) from information_schema.tables where table_name like 'rrobin_part%'")
    res = cur.fetchone()
    partitions = res[0]
    cur.execute("select count(*) from " + ratingstablename)
    res1 = cur.fetchone()
    number_of_rows = res1[0]
    last_inserted_row = number_of_rows % partitions
    table = 'rrobin_part' + str(last_inserted_row)
    cur.execute("insert into " + table +" values (%d,%d,%f)"%(userid,itemid,rating))
    cur.execute("insert into " + ratingstablename + " values(%d,%d,%f)"%(userid,itemid,rating))
    cur.close()
    openconnection.commit()



def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    max_rating = 5
    cur.execute("select count(*) from information_schema.tables where table_name like 'range_part%'")
    res = cur.fetchone()
    partitions = res[0]
    interval = float(max_rating)/float(partitions)
    i = math.floor(rating/interval)
    i = int(i)
    if rating % interval == 0:
        i-=1
    table = 'range_part' + str(i)
    cur.execute("insert into " + table + " values (%d,%d,%f)"%(userid,itemid,rating))
    cur.close()
    openconnection.commit()



def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.close()