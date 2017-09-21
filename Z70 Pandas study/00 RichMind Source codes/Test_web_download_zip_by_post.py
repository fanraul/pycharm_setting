#!/usr/bin/env python
import sys,os
import urllib

def run():
    params=urllib.urlencode({'name':'zipform'})
    headers={'Host':'imgloop.com',
             'User-Agent':'Mozilla/5.0 (X11; Linux i686; rv:6.0) Gecko/20100101 Firefox/6.0',
             'Accept':'*/*',
             'Accept-Language':'zh-cn,zh;q=0.5',
             'Accept-Encoding':'gzip, deflate',
             'Accept-Charset':'GB2312,utf-8;q=0.7,*;q=0.7',
             'Connection':'keep-alive',
             'Content-Type':'application/x-www-form-urlencoded; charset=UTF-8',
             'X-Requested-With':'XMLHttpRequest',
             'Referer':'http://imgloop.com/Home/article/'+self.number,
             'Content-Length':'12'
             }
    conn=httplib.HTTPConnection('imgloop.com')
    conn.request('POST','http://www.cninfo.com.cn/cninfo-new/data/download'+self.number,params,headers)
    response=conn.getresponse()
    if response.status==200:
        fileName=self.number+'.zip'
        #Check file has been downloaded.
        if os.path.exists(fileName) and long(response.getheader('Content-Length'))==os.path.getsize(fileName):
            print '%s %s has been downloaded.' % (getTime(),self.number)
            conn.close()
            return
        while(True):
            try:
                f=open(fileName,'w')
                f.write(response.read())
                f.close()
            except Exception:
                print '%s %s has a error, in %s redownload' % (getTime(),self.number,self.sleepTime)
                time.sleep(self.sleepTime)
                #Redownload.
                continue
            finally:
                print '%s %s download completed.' % (getTime(),self.number)
                #End of the download.
                break
    else:
        print '%s %s download failed.' % (getTime(),self.number)
    conn.close()

if __name__ == '__main__':
    #If only 1 argument.
    if len(sys.argv)==2:
        download(int(sys.argv[1])).start()
        exit()
    for i in range(int(sys.argv[1]),int(sys.argv[2])):
        download(i).start()