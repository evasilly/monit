# -*- coding: utf-8 -*-

import sys
import os
import time
from email.mime.text import MIMEText
import smtplib
from config import *
from libs.database import connectDataBaseByInfo
from libs.library import *
from libs.dirtyCrypt import *
from PyQt4 import QtCore
from prettytable import PrettyTable
import threading
from signal import SIGINT

message = u""
str_format = 'yyyy-MM-dd hh:mm:ss'
timePoint = QtCore.QDateTime.currentDateTime()

reload(sys)
sys.setdefaultencoding('utf-8')

db = connectDataBaseByInfo(connectionInfo)
tblfsSize = db.table('fsSize')
tblUsers = db.table('users')
tblMYC = db.table('mysqlclientscnt')
recUser = db.getRecordEx(tblUsers, '*',
                              tblUsers['login'].eq(forceString(msg['From']))
                              )
password = decryptPassword(forceString(recUser.value('password')))

def send_mail(message_body):
    """
        Класс для отправки сформированного сообщения по почте
    """
    msg.attach(MIMEText(message_body.encode('utf-8'), 'plain'))
    server = smtplib.SMTP('smtp.gmail.com: 587')
    server.starttls()
    server.login(msg['From'], password)
    server.sendmail(msg['From'], msg['To'], msg.as_string())
    server.quit()


def fsSizeCheck(step):
    while 1:
        proc = os.popen("source /home/megatron/workspace/monit/dirSizeCheck.sh")
        fsSize = proc.read()
        proc.close()
        newRec = tblfsSize.newRecord()
        newRec.setValue('chkDateTime', toVariant(QtCore.QDateTime.currentDateTime()))
        newRec.setValue('fsSize', fsSize)
        db.insertOrUpdate(tblfsSize, newRec)
        time.sleep(step)

        prevRecs = db.getRecordList(tblfsSize, '*', "chkDateTime >= '%s'" % (timePoint.addSecs(-3600)).toString(str_format))
        for rec in prevRecs:
            if float(fsSize[:-1]) - float(forceString(rec.value('fsSize'))) >= 5:
                msg = u'FILE SYSTEM ALLERT!\nFile System Size changed for more than 5 GB' + "\n"
                send_mail(msg)


def myc():
    while 1:
        proc = os.popen("source /home/megatron/workspace/monit/myc.sh")
        fline = proc.read()
        proc.close()
        newRec = tblMYC.newRecord()
        newRec.setValue('chkDateTime', toVariant(QtCore.QDateTime.currentDateTime()))
        newRec.setValue('myc', fline)
        db.insertOrUpdate(tblMYC, newRec)
        if int(fline) >= 100:
            msg = u"MySQL ALLERT!\nClients count is " + int(fline) + "\n"
            send_mail(msg)
        time.sleep(30)


def printMYC():
    outlist = PrettyTable()
    outlist.field_names = [u"dateTime", u"clientsCount"]
    prevmycRecs = db.getRecordList(tblMYC, '*', "chkDateTime >= '%s'" % (timePoint.addSecs(-300)).toString(str_format))
    for recm in prevmycRecs:
        outlist.add_row([formatDateTime(recm.value('chkDateTime')), forceString(recm.value('myc'))])
    print(u"MySQL clients count:")
    print(outlist)


def printFSSize():
    outlist = PrettyTable()
    outlist.field_names = [u"dateTime", u"fileSystemSize"]
    prevlssRecs = db.getRecordList(tblfsSize, '*', "chkDateTime >= '%s'" % (timePoint.addSecs(-3600)).toString(str_format))
    for recfss in prevlssRecs:
        outlist.add_row([formatDateTime(recfss.value('chkDateTime')), forceString(recfss.value('fsSize'))])
    print(u"File System Size History:")
    print(outlist)


def main():
    if len(sys.argv) == 2:
        if str(sys.argv[1]) == '-all':
            threading.Thread(name='mycThread', target=myc).start()
            threading.Thread(name='fssThread', target=fsSizeCheck(60)).start()
        elif str(sys.argv[1]) == '-lut':
            print u"CPU usage sorted:\n    PID  CPU processName"
            os.system("ps -e -o pid,pcpu,comm= | sort -n -k 2 | tail")
        elif str(sys.argv[1]) == '-mpt':
            printMYC()
        elif str(sys.argv[1]) == '-m':
            printFSSize()
        elif str(sys.argv[1]) == '-hta':
            os.system("source /home/megatron/workspace/monit/hta.sh")
    else:
        print(u'Введите ключ операции, чтобы узнать:'
              u'\n -all  =  Запустить сервиса в фоновую работу, пока не произойдёт останов;'
              u'\n -lut  =  Top пользователей, лидирующих по потребляемым ресурсам CPU за последние 5 минут;'
              u'\n -hta  =  Top 10 сайтов по количеству запросов и top 10 ip адресов для каждого из сайтов за последние N-минут;'
              u'\n -mpt  =  Количество запросов mysql за последние 5 минут (только количество тредов);'
              u'\n -m    =  Изменение свободного места за последний час для раздела /home;')


if __name__ == '__main__':
    main()