# def sendmail(subject, content):
#     email_host = 'smtp.163.com'     # 发送者是163邮箱
#     email_user = 'fanraul@163.com'  # 发送者账号
#     email_pwd = '121357468@'       # 发送者密码
#     maillist ='fanraul@icloud.com'    # 接收者账号，本来想写成[]list的，但是报错，还没解决！
#     me = email_user
#     # 三个参数：第一个为文本内容，第二个 html 设置文本格式，第三个 utf-8 设置编码
#     msg = MIMEText(content, 'html', 'utf-8')    # 邮件内容
#     msg['Subject'] = subject    # 邮件主题
#     msg['From'] = me    # 发送者账号
#     msg['To'] = maillist    # 接收者账号列表（列表没实现）
#
#     smtp = smtplib.SMTP(email_host) # 如上变量定义的，是163邮箱
#     smtp.login(email_user, email_pwd)   # 发送者的邮箱账号，密码
#     smtp.sendmail(me, maillist, msg.as_string())    # 参数分别是发送者，接收者，第三个不知道
#     smtp.quit() # 发送完毕后退出smtp
#     print ('email send success.')
#
# content = '''''
#     你好，
#             这是一封自动发送的邮件的内容。
# '''
#
# sendmail('subject for test', content)    # 调用发送邮箱的函数


import smtplib
import email.mime.multipart as multipart# import MIMEMultipart
from email.mime.text import MIMEText # import MIMEText
from email.mime.base import MIMEBase# import MIMEBase
import os.path
import mimetypes
import email

def send_email(receiver,title,content,attachment):
    From = "fanraul@163.com"
    To = receiver
    file_name = attachment #附件名 :"c:/1.png"
  
    server = smtplib.SMTP("smtp.163.com")
    server.login("fanraul@163.com","121357468@") #仅smtp服务器需要验证时
  
    # 构造MIMEMultipart对象做为根容器
    main_msg = multipart.MIMEMultipart()
  
    # 构造MIMEText对象做为邮件显示内容并附加到根容器
    text_msg = MIMEText(content,_charset="utf-8")
    main_msg.attach(text_msg)
  
    # 构造MIMEBase对象做为文件附件内容并附加到根容器
  
    # 读入文件内容并格式化 [方式1]－－－－－－－－－－－－－－－－－－－－－－－－－－－－－－
    data = open(file_name, 'rb')
    ctype,encoding = mimetypes.guess_type(file_name)
    if ctype is None or encoding is not None:
        ctype = 'application/octet-stream'
    maintype,subtype = ctype.split('/',1)
    file_msg = MIMEBase(maintype, subtype)
    file_msg.set_payload(data.read())
    data.close( )
    email.encoders.encode_base64(file_msg)#把附件编码
    ''''' 
     测试识别文件类型：mimetypes.guess_type(file_name) 
     rar 文件             ctype,encoding值：None None（ini文件、csv文件、apk文件） 
     txt text/plain None 
     py  text/x-python None 
     gif image/gif None 
     png image/x-png None 
     jpg image/pjpeg None 
     pdf application/pdf None 
     doc application/msword None 
     zip application/x-zip-compressed None 
     
    encoding值在什么情况下不是None呢？以后有结果补充。 
    '''
    #－－－－－－－－－－－－－－－－－－－－－－－－－－－－－－－－－－－－－－－－－－－－－
  
    ## 设置附件头
    basename = os.path.basename(file_name)
    file_msg.add_header('Content-Disposition','attachment', filename = basename)#修改邮件头
    main_msg.attach(file_msg)

    # 设置根容器属性
    main_msg['From'] = From
    main_msg['To'] = To
    main_msg['Subject'] = title
    main_msg['Date'] = email.utils.formatdate( )

    # 得到格式化后的完整文本
    fullText = main_msg.as_string( )

    # 用smtp发送邮件
    try:
        server.sendmail(From, To, fullText)
    except:
        raise
    finally:
        server.quit()

send_email('fanraul@icloud.com;terry.fan@sparkleconsulting.com','email test',
           'this is a test for email attachement','C:/00 RichMinds/Github/RichMinds/R10_sensor/tmp.xls')