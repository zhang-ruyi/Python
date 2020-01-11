import os
import time
import requests
from glob import iglob
from natsort import natsorted
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
 
 
@dataclass
class DownLoad_M3U8(object):
    m3u8_url  : str
    file_name : str
    m3u8_url_prefix = ''
    ts_list = []
    m3u8_root = ''

    def __post_init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 '
                          '(KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36', }
        self.threadpool = ThreadPoolExecutor(max_workers=20)
        #prefix
        pos = self.m3u8_url.rfind('/') + 1
        self.m3u8_url_prefix = self.m3u8_url[:pos]

        #root
        pos = self.m3u8_url.find('/',self.m3u8_url.find('//') + 2) + 1
        self.m3u8_root = self.m3u8_url[:pos]
        print("prefix = ", self.m3u8_url_prefix ,'root = ', self.m3u8_root)

        #output
        if not self.file_name:
            self.file_name = 'new.mp4'

    def get_ts_url(self):
        res = requests.get(self.m3u8_url, headers=self.headers)
        if res.status_code != 200:
            print("resp not 200")
            return
        ts_content = res.text.split("\n")
        self.ts_list = []
        for tmp in ts_content:
            # print(tmp)
            if tmp.find('.m3u8') != -1 :
                if tmp[0] == '/' :
                    tmp = tmp[1:]
                    self.m3u8_url = self.m3u8_root + tmp
                    self.m3u8_url_prefix = self.m3u8_root
                else :
                    pos = tmp.rfind('/') + 1
                    self.m3u8_url = self.m3u8_url_prefix + tmp
                    self.m3u8_url_prefix = self.m3u8_url_prefix + tmp[:pos]
                print("update url = ", self.m3u8_url, "prefix = ",self.m3u8_url_prefix)
                self.get_ts_url()

            if tmp.find(".ts") != -1 :
                if tmp.find('https://') != -1 or tmp.find('http://') != -1:
                    self.ts_list.append(tmp)
                    self.m3u8_url_prefix = ''
                else :
                    if tmp[0] == '/' :
                        tmp = tmp[1:]
                        self.m3u8_url_prefix = self.m3u8_root
                    self.ts_list.append(tmp)

        print(len(self.ts_list))
            
    def download_single_ts(self,ts_url):
        url = self.m3u8_url_prefix + ts_url
        pos = ts_url.rfind('/')
        ts_name = ''
        if pos != -1 :
            ts_name = ts_url[pos+1:]
        else :
            ts_name = ts_url
        i=0
        while i<3 :
            try :
                #print(url)
                res = requests.get(url,headers = self.headers,timeout=30)
                with open(ts_name,'wb') as fp:
                    fp.write(res.content)
                return
            except requests.exceptions.RequestException:
                print(url," : timeout retry")
                i += 1

 
    def download_all_ts(self):
        self.get_ts_url()
        print("download all start")
        for ts_url in self.ts_list:
            #print (ts_url)
            self.threadpool.submit(self.download_single_ts,ts_url)
        self.threadpool.shutdown()
        print("download all end")
 
    def run(self):
        self.download_all_ts()
        ts_path = '*.ts'
        with open(self.file_name,'wb') as fn:
            for ts in natsorted(iglob(ts_path)):
                with open(ts,'rb') as ft:
                    scline = ft.read()
                    fn.write(scline)
        for ts in iglob(ts_path):
            os.remove(ts)
 
if __name__ == '__main__':
    
    m3u8_url  = 'https://xxx/index.m3u8'
    file_name = '视频' + '.mp4'
 
    start = time.time()
 
    M3U8 = DownLoad_M3U8(m3u8_url,file_name)
    M3U8.run()
 
    end = time.time()
    print ('耗时:',end - start)
