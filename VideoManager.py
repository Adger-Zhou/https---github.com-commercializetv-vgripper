#! /usr/bin/env python
# encoding:utf-8

import urllib, urllib2
import feedparser
import threadpool
import paramiko
import MySQLdb
import os
import uuid
from subprocess import Popen, PIPE
import json

try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET


class MysqlConnect(object):
    def __init__(self, host, user, passwd, db_name):
        self.host = host
        self.user = user
        self.passwd = passwd
        self.db_name = db_name

    def get_conn(self):
        conn = MySQLdb.connect(host=self.host, user=self.user, passwd=self.passwd, db=self.db_name)
        return conn


def get_list_info(db_host, db_user, db_passwd, db_name):
    """
    获取扫描列表
    """
    mysql_conn = MysqlConnect(db_host, db_user, db_passwd, db_name)
    cursor = mysql_conn.get_conn()
    sftp_sources = cursor.execute("select (host,port,username,password,filedir) from sftpsources")
    rss_sources = cursor.execute("select rss_url from rsssources")
    local_sources = cursor.execute("select localdir from localsources")
    list_info = []
    if sftp_sources:
        for sftp_s in sftp_sources:
            sftp_data = {
                "source": "sftp",
                "host": sftp_s[0],
                "port": sftp_s[1],
                "username": sftp_s[2],
                "password": sftp_s[3],
                "filedir": sftp_s[4]
            }
            list_info.append(sftp_data)
    if rss_sources:
        for rss_s in rss_sources:
            rss_url = rss_s[0]
            xml_doc = feedparser.parse(rss_url)
            for media_item in xml_doc.entries:
                vidible_id = media_item.vidible_id
                media_content = media_item.media_content
                title = media_item.title
                media_max = media_content[0]
                type_container = media_max.get("type").split("/")  # video/mp4
                # source,type,slug,title,description,description_plan,duration,author_name,author_email
                rss_data = {
                    "source": "rss",
                    "type": type_container[0],
                    "slug": title,
                    "title": title,
                    "description": "",
                    "description_plan": "",
                    "duration": media_max.get("duration", ""),
                    "author_name": "",
                    "author_email": "",
                    "container": type_container[1],
                    "vidible_id": vidible_id,
                    "href": media_max.get("url", "")
                }
                list_info.append(rss_data)
    if local_sources:
        for local_s in local_sources:
            local_data = {
                "source": "local",
                "local_dir": local_s[0]
            }
            list_info.append(local_data)
    cursor.close()
    mysql_conn.close()
    return list_info


class ParamikoConnect(object):
    def __init__(self, hostname, port, username, passwd):
        self.hostname = hostname
        self.port = port
        self.username = username
        self.passwd = passwd

    def para_connect(self):
        conn_p = paramiko.Transport(self.hostname, self.port)
        conn_p.connect(self.username, self.passwd)
        return conn_p


def analytical_dict(stream):
    return stream.get("codec_type", "") + "?" + "codec_name=" + stream.get(
        "codec_name") + "&" + "profile=" + stream.get("profile", "") + "&" + "codec_tag_string=" + stream.get(
        "codec_tag_string", "") + "&" + "width=" + stream.get("width", "") + "&" + "height=" + stream.get("height", "")


def return_sql(file_path, insert_id=None):
    result = Popen("ffprobe -v quiet -print_format json -show_format -show_streams -i %s" % file_path, shell=True,
                   stdout=PIPE).stdout.read()
    result = json.loads(result)
    streams = result.get("streams", [])
    text = ""
    if streams:
        for stream in streams:
            text += analytical_dict(stream)
    format_info = result.get("format", {})
    format_tags = format_info.get("tags", {})
    duration = format_info.get("duration", "")
    media_size = format_info.get("size", "")
    bit_rate = format_info.get("bit_rate", "")
    major_brand = format_tags.get("major_brand", "")
    creation_time = format_tags.get("creation_time", "")
    sql = "insert into media_file_info (media_id,duration,media_size,bit_rate,major_brand,creation_time) VALUES (%s,%s,%s,%s,%s,%s)" % (
        insert_id, duration, media_size, bit_rate, major_brand, creation_time)
    return sql

def sftp_xml(local_path,cur):
    for file_name in os.listdir(local_path):
        if file_name.split(".")[-1] == "xml":
            file_path = local_path + "/" + file_name
            per = ET.parse(file_path)
            p = per.findall('meta')
            data = {}
            for i in p:
                for j in i.getchildren():
                    data[j.get("name", "")] = j.get("value", "")
        guid = uuid.uuid1()
        container = data.get("container", "mp4")
        new_name = str(guid) + "." + container
        cur.execute(
            "insert into media (type,slug,title,description,description_plan,duration,author_name,author_email,old_name,new_name) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)" %
            (data.get("type", "video"), data.get("title", ""), data.get("title", ""), data.get("description", ""),
             data.get("description_plan", ""), data.get("duration", ""), data.get("author_name", ""),
             data.get("author_email", ""), data.get("old_name", ""),
             new_name))
        insert_id = cur.execute("select LAST_INSERT_ID()")
        file_path = local_path + "." + container
        sql_sentence = return_sql(file_path, insert_id)
        cur.execute(sql_sentence)

def task_choice(data_info):
    """
    根据参数内容选择下载方式
    """
    mysql_conn = MysqlConnect(db_host, db_user, db_passwd, db_name).get_conn()
    cur = mysql_conn.cursor()
    if data_info.get("source", "") == "rss":
        media_type = data_info.get("type", "")
        slug = data_info.get("slug", "")
        title = data_info.get("title", "")
        description = data_info.get("description", "")
        description_plan = data_info.get("description_plan", "")
        duration = data_info.get("duration", "")
        author_name = data_info.get("author_name", "")
        author_email = data_info.get("author_email", "")
        container = data_info.get("container", "")
        vidible_id = data_info.get("vidible_id", "")
        href = data_info.get("vidible_id", "")
        old_name = vidible_id + "." + container
        guid = uuid.uuid1()
        new_name = str(guid) + "." + container
        local_file_path = localdir + "/" + new_name
        urllib.urlretrieve(href, local_file_path)
        cur.execute(
            "insert into media (type,slug,title,description,description_plan,duration,author_name,author_email,old_name,new_name) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)" %
            (media_type, slug, title, description, description_plan, duration, author_name, author_email, old_name,
             new_name))
        insert_id = cur.execute("select LAST_INSERT_ID()")
        sql_sentence = return_sql(local_file_path, insert_id)
        cur.execute(sql_sentence)
    elif data_info.get("source", "") == "sftp":
        host = data_info.get("host", "")
        port = data_info.get("port", "")
        username = data_info.get("username", "")
        password = data_info.get("password", "")
        file_dir = data_info.get("file_dir", [])
        para_conn = paramiko.Transport((host, port))
        para_conn.connect(username=username, password=password)
        sftp_p = paramiko.SFTPClient.from_transport(para_conn)
        for f_dir in file_dir:
            sftp_p.get(f_dir, localdir)
        para_conn.close()
        sftp_xml(localdir,cur)
    elif data_info.get("source", "") == "local":
        local_dir = data_info.get("local_dir", "")
        sftp_xml(local_dir,cur)
        # for file_name in os.listdir(local_dir):
        #     if file_name.split(".")[-1] == "xml":
        #         file_path = local_dir + "/" + file_name
        #         per = ET.parse(file_path)
        #         p = per.findall('meta')
        #         data = {}
        #         for i in p:
        #             for j in i.getchildren():
        #                 data[j.get("name", "")] = j.get("value", "")
        #     guid = uuid.uuid1()
        #     container = data.get("container", "mp4")
        #     new_name = str(guid) + "." + container
        #     cur.execute(
        #         "insert into media (type,slug,title,description,description_plan,duration,author_name,author_email,old_name,new_name) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)" %
        #         (data.get("type", "video"), data.get("title", ""), data.get("title", ""), data.get("description", ""),
        #          data.get("description_plan", ""), data.get("duration", ""), data.get("author_name", ""),
        #          data.get("author_email", ""), data.get("old_name", ""),
        #          new_name))
        #     insert_id = cur.execute("select LAST_INSERT_ID()")
        #     file_path = local_dir + "." + container
        #     sql_sentence = return_sql(file_path, insert_id)
        #     cur.execute(sql_sentence)
    cur.close()
    mysql_conn.close()

if __name__ == "__main__":
    localdir = ""
    db_host = ""
    db_user = ""
    db_passwd = ""
    db_name = ""
    thread_max = 5
    info_list = get_list_info(db_host, db_user, db_passwd, db_name)
    pool = threadpool.ThreadPool(thread_max)
    requests = threadpool.makeRequests(task_choice, info_list)
    [pool.putRequest(req) for req in requests]
    pool.wait()
