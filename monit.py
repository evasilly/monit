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

message = u""
str_format = 'yyyy-MM-dd hh:mm:ss'
timePoint = QtCore.QDateTime.currentDateTime()

reload(sys)
sys.setdefaultencoding('utf-8')

class CMonit(object):
    """
        Класс для проведения мониторинга
    """
    def __init__(self):
        self.db = connectDataBaseByInfo(connectionInfo)

        self.tblfsSize = self.db.table('fsSize')
        self.tblUsers = self.db.table('users')
        self.tblMYC = self.db.table('mysqlclientscnt')

        recUser = self.db.getRecordEx(self.tblUsers, '*',
                                      self.tblUsers['login'].eq(forceString(msg['From']))
                                      )
        self.password = decryptPassword(forceString(recUser.value('password')))
        # print self.password

    def send_mail(self, message_body):
        """
            Класс для отправки сформированного сообщения по почте
        """
        msg.attach(MIMEText(message_body.encode('utf-8'), 'plain'))
        server = smtplib.SMTP('smtp.gmail.com: 587')
        server.starttls()
        server.login(msg['From'], self.password)
        server.sendmail(msg['From'], msg['To'], msg.as_string())
        server.quit()

    def fsSizeCheck(self, step):
        while True:
            proc = os.popen("source /home/megatron/workspace/monit/dirSizeCheck.sh")
            fsSize = proc.read()
            proc.close()
            newRec = self.tblfsSize.newRecord()
            newRec.setValue('chkDateTime', toVariant(QtCore.QDateTime.currentDateTime()))
            newRec.setValue('fsSize', fsSize)
            self.db.insertOrUpdate(self.tblfsSize, newRec)
            time.sleep(step)

            prevRecs = self.db.getRecordList(self.tblfsSize, '*', "chkDateTime >= '%s'" % (timePoint.addSecs(-3600)).toString(str_format))
            for rec in prevRecs:
                if float(fsSize[:-1]) - float(forceString(rec.value('fsSize'))) >= 5:
                    msg = u'FILE SYSTEM ALLERT!\nFile System Size changed for more than 5 GB' + "\n"
                    self.send_mail(msg)

    def myc(self):
        proc = os.popen("source /home/megatron/workspace/monit/myc.sh")
        fline = proc.read()
        proc.close()
        newRec = self.tblMYC.newRecord()
        newRec.setValue('chkDateTime', toVariant(QtCore.QDateTime.currentDateTime()))
        newRec.setValue('myc', fline)
        self.db.insertOrUpdate(self.tblMYC, newRec)
        if int(fline) >= 100:
            msg = u"MySQL ALLERT!\nClients count is " + int(fline) + "\n"
            self.send_mail(msg)
        time.sleep(30)

    def printMYC(self):
        outlist = PrettyTable()
        outlist.field_names = [u"dateTime", u"clientsCount"]
        prevRecs = self.db.getRecordList(self.tblfsSize, '*', "chkDateTime <= '%s'" % (timePoint.addSecs(-300)).toString(str_format))
        for rec in prevRecs:
            outlist.add_row([formatDateTime(rec.value('chkDateTime')), forceString(rec.value('myc'))])
        print(u"MySQL clients count:")
        print(outlist)

    def printFSSize(self):
        outlist = PrettyTable()
        outlist.field_names = [u"dateTime", u"fileSystemSize"]
        prevRecs = self.db.getRecordList(self.tblMYC, '*', "chkDateTime <= '%s'" % (timePoint.addSecs(-3600)).toString(str_format))
        for rec in prevRecs:
            outlist.add_row([formatDateTime(rec.value('chkDateTime')), forceString(rec.value('fsSize'))])
        print(u"File System Size History:")
        print(outlist)


def main():
    ex = CMonit()
    if len(sys.argv) == 2:
        if str(sys.argv[1]) == '-all':
            while True:
                ex.fsSizeCheck(60)
                ex.myc()
        elif str(sys.argv[1]) == '-lut':
            os.system("ps -e -o pid,pcpu,comm= | sort -n -k 2 | tail")
        # elif str(sys.argv[1]) == '-al':
        #     # print average CPU load during last 5 mins
        # elif str(sys.argv[1]) == '-hta -':
        #     # list top 10 website addresses and its ip-s
        elif str(sys.argv[1]) == '-mpt':
            ex.printMYC()
        elif str(sys.argv[1]) == '-m':
            ex.printFSSize()
    else:
        print(u'Введите ключ операции, чтобы узнать:'
              u'\n -all  =  Запуск сервиса в фоновую работу, пока не произойдёт останов;'
              u'\n -lut  =  Top пользователей, лидирующих по потребляемым ресурсам CPU за последние 5 минут;'
              u'\n -al   =  Среднюю нагрузку на диск за последние 10 минут в разделе /home;'
              u'\n -hta  =  Top 10 сайтов по количеству запросов и top 10 ip адресов для каждого из сайтов за последние N-минут;'
              u'\n -mpt  =  Количество запросов mysql за последние 5 минут (только количество тредов);'
              u'\n -m    =  Изменение свободного места за последний час для раздела /home;')


if __name__ == '__main__':
    main()