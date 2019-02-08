import mysql.connector
from mysql.connector import errorcode
from mysql.connector import pooling
import httplib
import hashlib
import time

class BaseDbManager:
    DB_NAME = 'mfw_pro_crawl'
    SERVER_IP = '127.0.0.1'
    TABLES = {}
    def __init__(self, max_num_thread):
        try:
            cnx = mysql.connector.connect(host=self.SERVER_IP, user='root', password='123456')
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print 'Create Error ' + err.msg
            exit(1)

        cursor = cnx.cursor()
        try:
            cnx.database = self.DB_NAME
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_BAD_DB_ERROR:
                self.create_database(cursor)
                cnx.database = self.DB_NAME
            else:
                print(err)
                exit(1)
        finally:
            self.create_tables(cursor)
            cursor.close()
            cnx.close()

        dbconfig = {
        "database": self.DB_NAME,
        "user": "root",
        "host": self.SERVER_IP,
        "password": "123456",
        }
        self.cnxpool = mysql.connector.pooling.MySQLConnectionPool(pool_name = "mypool",
        pool_size = max_num_thread,
        **dbconfig)

    def create_database(self, cursor):
        try:
            cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(self.DB_NAME))
        except mysql.connector.Error as err:
            print("Failed creating database: {}".format(err))
            exit(1)

    def create_tables(self, cursor):
        for name, ddl in self.TABLES.iteritems():
            try:
                cursor.execute(ddl)
            except mysql.connector.Error as err:
                if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                    print 'create tables error ALREADY EXISTS'
                else:
                    print 'create tables error ' + err.msg
            else:
                print 'Tables created'
    def GetTableName(self):
        return self.m_Table
    def ClearTable(self):
        con = self.cnxpool.get_connection()
        cursor = con.cursor()
        try:
            add_url = ("truncate table %s") % (self.m_Table)
            cursor.execute(add_url)
            con.commit()
        except mysql.connector.Error as err:
            print 'ClearTable() ' + err.msg
            return
        finally:
            cursor.close()
            con.close()


class ChapterDbManager(BaseDbManager):
    def __init__(self, novelIndex, max_num_thread):
        self.m_Table = novelIndex
        self.TABLES[self.m_Table] = (
        "CREATE TABLE `%s` ("
        " `index` int(11) NOT NULL AUTO_INCREMENT,"
        " `chapterUrl` varchar(128) NOT NULL,"
        " `chapterName` varchar(128) NOT NULL,"
        " `chapterContent` text(65536),"
        " `status` varchar(11) NOT NULL DEFAULT 'new',"
        " `queue_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,"
        " `done_time` timestamp NOT NULL DEFAULT 0,"
        " PRIMARY KEY (`index`),"
        " UNIQUE KEY `chapterUrl` (`chapterUrl`)"
        ") ENGINE=InnoDB") % (self.m_Table)
        BaseDbManager.__init__(self, max_num_thread)
    def ExistChapter(self, chapterUrl):
        con = self.cnxpool.get_connection()
        cursor = con.cursor()
        try:
            query = ("SELECT * FROM %s WHERE chapterUrl='%s' ") % (self.m_Table, chapterUrl)
            cursor.execute(query)
            if cursor.rowcount is 0:
                return False
            row = cursor.fetchone()
            if row is None:
                return False
            return True
        except mysql.connector.Error as err:
            print 'ExistChapter() ' + err.msg
            return False
        finally:
            cursor.close()
            con.close()
    def AddChapter(self, chapterUrl, chapterName):
        con = self.cnxpool.get_connection()
        cursor = con.cursor()
        try:
            add_url = ("INSERT INTO %s (chapterUrl, chapterName) VALUES ('%s', '%s')") % (self.m_Table, chapterUrl, chapterName)
            cursor.execute(add_url)
            con.commit()
        except mysql.connector.Error as err:
            print 'AddChapter() ' + err.msg
            return
        finally:
            cursor.close()
            con.close()
    def UpdateChapterContent(self, chapterUrl, chapterContent):
        con = self.cnxpool.get_connection()
        cursor = con.cursor(dictionary=True)
        try:
            query = ("SELECT `index` FROM %s WHERE chapterUrl='%s'") % (self.m_Table, chapterUrl)
            cursor.execute(query)
            if cursor.rowcount is 0:
                return False

            row = cursor.fetchone()
            if row is None:
                return False
            update_query = ("UPDATE %s SET `chapterContent`='%s', `done_time`='%s' WHERE `index`=%d") % (self.m_Table, chapterContent, time.strftime('%Y-%m-%d %H:%M:%S'), row['index'])
            cursor.execute(update_query)
            con.commit()
            return True
        except mysql.connector.Error as err:
            print 'UpdateChapterContent() ' + err.msg
            return False
        finally:
            cursor.close()
            con.close()
    def UpdateChapterByUrl(self, chapterUrl, status):
        con = self.cnxpool.get_connection()
        cursor = con.cursor(dictionary=True)
        try:
            query = ("SELECT `index` FROM %s WHERE chapterUrl='%s'") % (self.m_Table, chapterUrl)
            cursor.execute(query)
            if cursor.rowcount is 0:
                return False

            row = cursor.fetchone()
            if row is None:
                return False
            update_query = ("UPDATE %s SET `status`='%s', `done_time`='%s' WHERE `index`=%d") % (self.m_Table, status, time.strftime('%Y-%m-%d %H:%M:%S'), row['index'])
            cursor.execute(update_query)
            con.commit()
            return True
        except mysql.connector.Error as err:
            #print 'UpdateChapterByUrl() ' + err.msg
            return False
        finally:
            cursor.close()
            con.close()
    def UpdateChapterByIndex(self, index, status):
        con = self.cnxpool.get_connection()
        cursor = con.cursor(dictionary=True)
        try:
            update_query = ("UPDATE %s SET `status`='%s', `done_time`='%s' WHERE `index`=%d") % (self.m_Table, status, time.strftime('%Y-%m-%d %H:%M:%S'), index)
            cursor.execute(update_query)
            con.commit()
            return True
        except mysql.connector.Error as err:
            #print 'UpdateChapterByIndex() ' + err.msg
            return False
        finally:
            cursor.close()
            con.close()
    def AutoGetChapter(self):
        con = self.cnxpool.get_connection()
        cursor = con.cursor(dictionary=True)
        try:
            query = ("SELECT * FROM %s WHERE status='new' ORDER BY `index` ASC LIMIT 1 FOR UPDATE") % (self.m_Table)
            cursor.execute(query)
            if cursor.rowcount is 0:
                return None
            row = cursor.fetchone()
            if row is None:
                return row
            update_query = ("UPDATE %s SET `status`='downloading' WHERE `index`=%d") % (self.m_Table, row['index'])
            cursor.execute(update_query)
            con.commit()
            return row
        except mysql.connector.Error as err:
            print 'AutoGetChapter() ' + err.msg
            return None
        finally:
            cursor.close()
            con.close()
    def QueryAllIndex(self):
        con = self.cnxpool.get_connection()
        cursor = con.cursor(dictionary=True)
        try:
            query = ("SELECT `index` FROM %s ORDER BY `index` ASC") % (self.m_Table)
            cursor.execute(query)
            if cursor.rowcount is 0:
                return None
            rows = cursor.fetchall()
            if rows is None:
                return rows
            con.commit()
            return rows
        except mysql.connector.Error as err:
            print 'QueryAllIndex() ' + err.msg
            return None
        finally:
            cursor.close()
            con.close()
    def CleanWrongStatus(self):
        con = self.cnxpool.get_connection()
        cursor = con.cursor(dictionary=True)
        try:
            query = ("SELECT `index` FROM %s WHERE status='downloading' ORDER BY `index` ASC") % (self.m_Table)
            cursor.execute(query)
            if cursor.rowcount is 0:
                return True

            rows = cursor.fetchall()
            if rows is None:
                return True
            for item in rows:
                if item is None:
                    continue
                update_query = ("UPDATE %s SET `status`='new' WHERE `index`=%d") % (self.m_Table, item['index'])
                cursor.execute(update_query)
            con.commit()
            return True
        except mysql.connector.Error as err:
            #print 'CleanWrongStatus() ' + err.msg
            return False
        finally:
            cursor.close()
            con.close()
class NovelDbManager(BaseDbManager):
    def __init__(self, max_num_thread):
        self.m_Table = 'NovelList'
        self.TABLES[self.m_Table] = (
        "CREATE TABLE `%s` ("
        " `index` int(11) NOT NULL AUTO_INCREMENT,"
        " `novelUrl` varchar(128) NOT NULL,"
        " `novelName` varchar(128) NOT NULL,"
        " `record_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,"
        " PRIMARY KEY (`index`),"
        " UNIQUE KEY `novelUrl` (`novelUrl`)"
        ") ENGINE=InnoDB") % (self.m_Table)
        BaseDbManager.__init__(self, max_num_thread)

    def AddNovel(self, novelUrl, novelName):
        con = self.cnxpool.get_connection()
        cursor = con.cursor()
        try:
            add_url = ("INSERT INTO %s (novelUrl, novelName) VALUES ('%s', '%s')") % (self.m_Table, novelUrl, novelName)
            cursor.execute(add_url)
            con.commit()
        except mysql.connector.Error as err:
            print 'AddNovel() ' + err.msg
            return
        finally:
            cursor.close()
            con.close()
    def ExistNovel(self, novelUrl):
        con = self.cnxpool.get_connection()
        cursor = con.cursor()
        try:
            query = ("SELECT * FROM %s WHERE novelUrl='%s' ") % (self.m_Table, novelUrl)
            cursor.execute(query)
            if cursor.rowcount is 0:
                return False
            row = cursor.fetchone()
            if row is None:
                return False
            return True
        except mysql.connector.Error as err:
            print 'ExistNovel() ' + err.msg
            return False
        finally:
            cursor.close()
            con.close()
    def QueryNovelByUrl(self, novelUrl):
        con = self.cnxpool.get_connection()
        cursor = con.cursor(dictionary=True)
        row = None
        try:
            query = ("SELECT * FROM %s WHERE novelUrl='%s' ORDER BY `index` ASC LIMIT 1 FOR UPDATE") % (self.m_Table, novelUrl)
            cursor.execute(query)
            if cursor.rowcount is 0:
                return None

            row = cursor.fetchone()
            if row is None:
                return row
            con.commit()
            return row
        except mysql.connector.Error as err:
            #print 'QueryRule() ' + err.msg
            return None
        finally:
            cursor.close()
            con.close()
    def QueryNovelByIndex(self, index):
        con = self.cnxpool.get_connection()
        cursor = con.cursor(dictionary=True)
        row = None
        try:
            query = ("SELECT * FROM %s WHERE `index`=%d") % (self.m_Table, index)

            cursor.execute(query)
            if cursor.rowcount is 0:
                return None

            row = cursor.fetchone()
            if row is None:
                return row
            con.commit()
            return row
        except mysql.connector.Error as err:
            print 'QueryNovelByIndex() ' + err.msg
            return None
        finally:
            cursor.close()
            con.close()

class RuleDbManager(BaseDbManager):
    def __init__(self, max_num_thread):
        self.m_Table = 'ParseRule'
        self.TABLES[self.m_Table] = (
        "CREATE TABLE `%s` ("
        " `index` int(11) NOT NULL AUTO_INCREMENT,"
        " `webUrl` varchar(128) NOT NULL,"
        " `parseUrl` varchar(128) NOT NULL,"
        " `novelnameRule` varchar(128) NOT NULL,"
        " `chapterRule` varchar(128) NOT NULL,"
        " `titleRule` varchar(128) NOT NULL,"
        " `ContentRule` varchar(128) NOT NULL,"
        " `record_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,"
        " PRIMARY KEY (`index`),"
        " UNIQUE KEY `webUrl` (`webUrl`)"
        ") ENGINE=InnoDB") % (self.m_Table)
        BaseDbManager.__init__(self, max_num_thread)
    def AddRule(self, newRule):
        con = self.cnxpool.get_connection()
        cursor = con.cursor()
        try:
            add_url = ("INSERT INTO %s (webUrl, parseUrl, novelnameRule, chapterRule, titleRule, ContentRule) VALUES ('%s', '%s','%s','%s', '%s', '%s')") % (self.m_Table, newRule['webUrl'], newRule['parseUrl'], newRule['novelnameRule'], newRule['chapterRule'],newRule['titleRule'],newRule['ContentRule'])
            cursor.execute(add_url)
            con.commit()
        except mysql.connector.Error as err:
            print 'AddRule() ' + err.msg
            return
        finally:
            cursor.close()
            con.close()
    def QueryRule(self, novelUrl):
        con = self.cnxpool.get_connection()
        cursor = con.cursor(dictionary=True)
        row = None
        try:
            query = ("SELECT `webUrl` FROM %s ORDER BY `index` ASC") % (self.m_Table)
            cursor.execute(query)
            if cursor.rowcount is 0:
                return None

            rows = cursor.fetchall()
            if rows is None:
                return row
            for item in rows:
                if item is None:
                    continue
                webUrl = item['webUrl']
                if webUrl in novelUrl:
                    Extquery = ("SELECT * FROM %s WHERE webUrl='%s' ORDER BY `index` ASC LIMIT 1 FOR UPDATE") % (self.m_Table, webUrl)
                    cursor.execute(Extquery)
                    row = cursor.fetchone()
                    con.commit()
                    return row
        except mysql.connector.Error as err:
            #print 'QueryRule() ' + err.msg
            return None
        finally:
            cursor.close()
            con.close()


