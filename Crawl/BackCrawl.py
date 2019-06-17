# -*- coding: utf-8 -*-
import urllib2


from lxml import etree
import os

import httplib
import time
import thread
import threading
import cookielib
import traceback
import zlib

import sys
#import chardet

from collections import deque
import xml.dom.minidom as xmldom

from BaseDbManager import RuleDbManager

from BaseDbManager import NovelDbManager

from BaseDbManager import ChapterDbManager

from LogMgr import LogType
from LogMgr import LogInfo

CRAWL_DELAY = 0.05

class MyHTTPRedirectHandler(urllib2.HTTPRedirectHandler):
    def http_error_302(self, req, fp, code, msg, headers):
        print "Cookie Manip Right Here"
        return urllib2.HTTPRedirectHandler.http_error_302(self, req, fp, code, msg, headers)

    http_error_301 = http_error_303 = http_error_307 = http_error_302


class BaseCrawl:
    def __init__(self, url):
        self.bCreate = False
        self.m_NovelUrl = None
        self.max_num_thread = 30
        self.novelDbMgr = None
        self.chapterDbMgr = None
        self.ruleDbMgr = None
        self.threads = []
        self.m_NovelName = None
        self.m_bLoop = False
        self.ruleDbMgr = RuleDbManager(self.max_num_thread)
        rule = self.ruleDbMgr.QueryRule(url)
        if rule is None:
            LogInfo(LogType.LOG_ERROR, "query rule failed %s" % url)
            return
        self.ResetNovelUrl(url)
        self.ResetRequestHeaders(rule['webUrl'])
        self.ResetParseRootUrl(rule['parseUrl'])
        self.ResetNovelNameRule(rule['novelnameRule'])
        self.ResetMenuChapterRule(rule['chapterRule'] )
        self.ResetTitleRule(rule['titleRule'])
        self.ResetContentRule(rule['ContentRule'])
        self.bCreate = True
    def GetStatus(self):
        return self.bCreate
    def GetLoop(self):
        return self.m_bLoop
    def SetLopp(self, value):
        self.m_bLoop = value
    def ResetRequestHeaders(self, host):
        newHost = None
        if type(host) == unicode:
            newHost = host.encode('gbk')
        else:
            newHost = host
        self.request_headers = {
        'host': newHost,
        'connection': "keep-alive",
        'cache-control': "no-cache",
        'upgrade-insecure-requests': "1",
        'user-agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.95 Safari/537.36",
        'accept': "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        'accept-language': "zh-CN,en-US;q=0.8,en;q=0.6"
        }
        print self.request_headers
    def ResetNovelUrl(self, novelUrl):
        self.m_NovelUrl = novelUrl
        self.novelDbMgr = NovelDbManager(self.max_num_thread)
    def ResetParseRootUrl(self, ParseRootUrl):
        self.m_ParseRottUrl = ParseRootUrl
    def ResetNovelNameRule(self, NovelNameRule):
        self.m_NovelNameRule = NovelNameRule
    def ResetMenuChapterRule(self, ChapterRule):
        self.m_ChapterRule = ChapterRule
    def ResetTitleRule(self, TitleRule):
        self.m_TitleRule = TitleRule
    def ResetContentRule(self, ContentRule):
        self.m_ContentRule = ContentRule
    def ReadHtmlPage(self, url):
        html_page = None
        retryTimes = 0
        while True:
            try:
                if retryTimes > 10:
                    LogInfo(LogType.LOG_ERROR, "too many retries %s" % url)
                    return html_page
                    #request = urllib2.Request(url, headers = self.request_headers)
                cj = cookielib.CookieJar()
                opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj))
                opener.addheaders = [('User-agent', 'Mozilla/5.0')]
                opener.addheaders = [('Accept-encoding', 'gzip')]

                response = opener.open(url, timeout= 2)
                gizziped = response.headers.get('Content-Encoding')
                html_page = response.read()
                if gizziped:
                    html_page = zlib.decompress(html_page, 16+zlib.MAX_WBITS)



                #chardInfo = chardet.detect(html_page)
                #print 'Html is encoding by : %', chardInfo

                response.close()
                break
            except urllib2.HTTPError, Arguments:
                LogInfo(LogType.LOG_ERROR, "urllib2.HTTPError: %s || sourec: %s" % (Arguments, url))
                continue
            except httplib.BadStatusLine, Arguments:
                LogInfo(LogType.LOG_ERROR, "httplib.BadStatusLine: %s || sourec: %s" % (Arguments, url))
                continue
            except IOError, Arguments:
                LogInfo(LogType.LOG_ERROR, "IOError: %s || sourec: %s" % (Arguments, url))
                continue
            except Exception, Arguments:
                LogInfo(LogType.LOG_ERROR, "Exception: %s || sourec: %s" % (Arguments, url))
                continue
            finally:
                retryTimes = retryTimes + 1
        return html_page
        
    def ParseNovelName(self, html_page):
        novelName = None
        try:
            html = etree.HTML(html_page.lower())
            novelNameInfo = html.xpath(self.m_NovelNameRule)
            print self.m_NovelNameRule
            novelName = novelNameInfo[-1:][0]
        except Exception, Arguments:
            LogInfo(LogType.LOG_ERROR, "ParseNovelName:Exception: %s || novelUrl: %s" % (Arguments, self.m_NovelUrl))
        return novelName
            
    def MakeChapterStore(self, html_page):
        try:
            html = etree.HTML(html_page.lower())
            hrefs = html.xpath(self.m_ChapterRule)
            for href in hrefs:
                if 'href' in href.attrib:
                    nexUrl = href.attrib['href']
                    title = href.text
                    if not self.chapterDbMgr.ExistChapter(nexUrl):
                        self.chapterDbMgr.AddChapter(nexUrl, title)
            return True
        except Exception, Arguments:
            LogInfo(LogType.LOG_ERROR, "MakeChapterStore:Exception: %s || novelUrl: %s" % (Arguments, self.m_NovelUrl))
            return False
    def DownloadTask(self):
        html_page = self.ReadHtmlPage(self.m_NovelUrl)
        if html_page == None:
            LogInfo(LogType.LOG_ERROR, "Read html page failed! source:%s" % (self.m_NovelUrl))
            return
        html = etree.HTML(html_page.lower())
        novelName = self.ParseNovelName(html_page)
        if novelName == None:
            LogInfo(LogType.LOG_ERROR, "Read html page failed! source:%s" % (self.m_NovelUrl))
            return
        self.m_NovelName = novelName
  
        if not self.novelDbMgr.ExistNovel(self.m_NovelUrl):
            self.novelDbMgr.AddNovel(self.m_NovelUrl, self.m_NovelName)
        novelItem = self.novelDbMgr.QueryNovelByUrl(self.m_NovelUrl)
        if novelItem is None:
            LogInfo(LogType.LOG_ERROR, "QueryNovelByUrl failed! source:%s" % (self.m_NovelUrl))
            return None
        novelIndexName = "novelName_" + str(novelItem['index'])
        self.chapterDbMgr = ChapterDbManager(novelIndexName, self.max_num_thread)
        if self.chapterDbMgr == None:
            LogInfo(LogType.LOG_ERROR, "chapterDbMgr init failed! source:%s novelIndex:%s" % (self.m_NovelUrl, novelIndexName))
            return None
        self.chapterDbMgr.CleanWrongStatus()
        if not self.MakeChapterStore(html_page):
            LogInfo(LogType.LOG_ERROR, "MakeChapterStore failed! source:%s" % (self.m_NovelUrl))
            return None
        while True:
            curtask = self.chapterDbMgr.AutoGetChapter()
            if curtask is None:
                LogInfo(LogType.LOG_ERROR, "AutoGetChapter failed! need to clean .source:%s" % (self.m_NovelUrl))
                self.chapterDbMgr.CleanWrongStatus()
                curtask = self.chapterDbMgr.AutoGetChapter()
                if curtask is None:
                    LogInfo(LogType.LOG_ERROR, "AutoGetChapter failed! all is over .source:%s" % (self.m_NovelUrl))
                    for t in self.threads:
                        t.join()
                    break
            index = curtask['index']
            for t in self.threads:
                if not t.is_alive():
                    self.threads.remove(t)
            if len(self.threads) >= self.max_num_thread:
                time.sleep(CRAWL_DELAY)
                continue
            try:
                t = threading.Thread(target=self.HandleUrl, name=None, args=(curtask,))
                self.threads.append(t)
                # set daemon so main thread can exit when receives ctrl-c
                t.setDaemon(True)
                t.start()
                time.sleep(CRAWL_DELAY)
            except Exception:
                print "Error: unable to start thread"
        self.HandleFullContent()
        print "download all over"
        
    def HandleUrl(self, curtask):
        if curtask is None:
            LogInfo(LogType.LOG_ERROR, "HandleUrl failed!.source:%s " % (self.m_NovelUrl))
            return None
        index = curtask['index']
        if self.FetchSingleUrl(curtask):
            self.chapterDbMgr.UpdateChapterByIndex(index, 'done')
            
    def FetchSingleUrl(self, curtask):
        if curtask is None:
            LogInfo(LogType.LOG_ERROR, "FetchSingleUrl failed!.source:%s " % (self.m_NovelUrl))
            return False
        nexUrl = curtask['chapterUrl']
        index = curtask['index']

        fullUrl = self.m_ParseRottUrl + nexUrl
        if self.m_ParseRottUrl == "novel_name":
            fullUrl = self.m_NovelUrl + "/" + nexUrl
        
        try:
            html_page = self.ReadHtmlPage(fullUrl)
            if html_page == None:
                LogInfo(LogType.LOG_ERROR, "FetchSingleUrl ReadHtmlPage failed!.source:%s " % (fullUrl))
                return False
            html = etree.HTML(html_page.lower())
            title = curtask['chapterName']
            if title is None:
                LogInfo(LogType.LOG_ERROR, "FetchSingleUrl title failed!.source:%s " % (fullUrl))
                return False

            content = html.xpath(self.m_ContentRule)

            chapterDbName = self.chapterDbMgr.GetTableName()
            novelNameArray = chapterDbName.split("novelName_")
            novelIndex = int(novelNameArray[-1:][0])
            novelInfo = self.novelDbMgr.QueryNovelByIndex(novelIndex)
            if novelInfo is None:
                LogInfo(LogType.LOG_ERROR, "QueryNovelByIndex failed!.source:%s novelIndex = %d " % (self.m_NovelUrl, novelIndex))
                return False
            downloadValue = [title, content, index, novelInfo['novelName'] + str(novelIndex)]
            if self.HandleDownloadContent(downloadValue):
                fullContent = ''.encode('utf-8')
                for item in content:
                    fullContent = fullContent + item.encode('utf-8') + '\n'.encode('utf-8')

                self.chapterDbMgr.UpdateChapterContent(nexUrl, fullContent)
                print "downloaded %s" % (title)
                return True
        except urllib2.HTTPError, Arguments:
            LogInfo(LogType.LOG_ERROR, "FetchSingleUrl urllib2.HTTPError: %s .source:%s " % (Arguments, fullUrl))
            return False
        except httplib.BadStatusLine, Arguments:
            LogInfo(LogType.LOG_ERROR, "FetchSingleUrl httplib.BadStatusLine: %s .source:%s " % (Arguments, fullUrl))
            return False
        except IOError, Arguments:
            LogInfo(LogType.LOG_ERROR, "FetchSingleUrl IOError: %s .source:%s " % (Arguments, fullUrl))
            return False
        except Exception, Arguments:
            LogInfo(LogType.LOG_ERROR, "FetchSingleUrl Exception: %s .source:%s " % (Arguments, fullUrl))
            traceback.print_exc()
            return False
                
    def HandleDownloadContent(self, downloadValue):
        LogInfo(LogType.LOG_INFO, "BASE HandleDownloadContent was called")
        return True
    def HandleFullContent(self):
        print "ready to rewrite"


class SpCrawl(BaseCrawl):
    def __init__(self, url):
        BaseCrawl.__init__(self, url)
        print "SpCrawl"
    def HandleDownloadContent(self, downloadValue):
        length = len(downloadValue)
        if length < 4:
            LogInfo(LogType.LOG_ERROR, "HandleDownloadContent: not enough value cur len = %d" % (length))
            return False
        title = downloadValue[0]
        content = downloadValue[1]
        index = downloadValue[2]
        novelName = downloadValue[3]
        targetPath = ('./%s/') % (novelName)
        if not os.path.isdir(targetPath):
            os.mkdir(targetPath)
        fp1 = open('%s/%d.txt' % (targetPath, index),'wb+')

        fp1.write(title.encode('utf-8'))
        fp1.write('\n')
        for item in content:
            fp1.write(item.encode('utf-8'))
            fp1.write('\n')
        fp1.close()
        return True
    def HandleFullContent(self):
        novelItem = self.novelDbMgr.QueryNovelByUrl(self.m_NovelUrl)
        if novelItem is None:
            LogInfo(LogType.LOG_ERROR, "HandleFullContent: QueryNovelByUrl: source:%s" % (self.m_NovelUrl))
            return None
        newPath = self.m_NovelName + str(novelItem['index'])

        start_index = 0
        fpNew = open('%s_%s.txt' % (newPath, start_index),'wb+')
        IndexArry = self.chapterDbMgr.QueryAllIndex()
        total_count = 0
        for item in IndexArry:
            filePath = ('%s/%d.txt') % (newPath, item['index'])
            if os.path.exists(filePath):
                if total_count > (1024 * 1024 * 20):
                    total_count = 0
                    fpNew.close()
                    start_index = start_index + 1
                    fpNew = open('%s_%s.txt' % (newPath, start_index),'wb+')
                                
                fp = open(filePath,'r')
                content = fp.read()
                fpNew.write(content)
                fp.close()
                file_byte_size = os.path.getsize(filePath)
                total_count = total_count + file_byte_size
        fpNew.close()

def InitRule():
    print "init rule"
    ruleDb = RuleDbManager(5)
    ruleDb.ClearTable()
    rulePath = os.path.abspath('WebRule.xml')
    domobj = xmldom.parse(rulePath)
    elementobj = domobj.documentElement

    # get all elements
    elementArray = elementobj.getElementsByTagName("Property")
    print elementArray
    for index in range(len(elementArray)):
        # get all node list in some one element
        print index
        nodeAttrMap = elementArray.item(index).attributes
        
        newRule = {}
        for i in range(nodeAttrMap.length):
            newRule[nodeAttrMap.item(i).name] = nodeAttrMap.item(i).value
        
        ruleDb.AddRule(newRule)
    
def UpdateNovelList():
    novelPath = os.path.abspath('NovelList.xml')
    domobj = xmldom.parse(novelPath)
    elementobj = domobj.documentElement

    elementArray = elementobj.getElementsByTagName("Property")
    for index in range(len(elementArray)):
        url = elementArray[index].getAttribute('url')
        print url
        crawl = SpCrawl(url)
        crawl.DownloadTask()


if __name__ == "__main__":
    InitRule()
    UpdateNovelList()











