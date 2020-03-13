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

message = u""
str_format = 'yyyy-MM-dd hh:mm:ss'
timePoint = QtCore.QDateTime.currentDateTime()
beg = (timePoint.addSecs(-120)).toString(str_format)

class CMonit(object):
    """
        Класс для проведения мониторинга
    """
    def __init__(self):
        self.db = connectDataBaseByInfo(connectionInfo)

        self.tblfsSize = self.db.table('fsSize')
        self.tblUsers = self.db.table('users')

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

            prevRecs = self.db.getRecordList(self.tblfsSize, '*', "chkDateTime >= '%s'" % beg)
            for rec in prevRecs:
                if float(fsSize[:-1]) - float(forceString(rec.value('fsSize'))) >= 5:
                    msg = u'ALLERT! > ' + forceString(fsSize)
                    self.send_mail(msg)



def main():
    ex = CMonit()
    if len(sys.argv) == 2:
        if str(sys.argv[1]) == '-lut':
            os.system("ps -e -o pid,pcpu,comm= | sort -n -k 2 | tail")
        # elif str(sys.argv[1]) == '-al':
        #     # print average CPU load during last 5 mins
        # elif str(sys.argv[1]) == '-hta -':
        #     # list top 10 website addresses and its ip-s
        # elif str(sys.argv[1]) == '-mt':
        #     # list mysql threads count during last 5 mins
        elif str(sys.argv[1]) == '-m':
            ex.fsSizeCheck(10)
    else:
        print(u'Введите ключ операции, чтобы узнать:'
              u'\n -lut  =  Top пользователей, лидирующих по потребляемым ресурсам CPU за последние 5 минут;'
              u'\n -al   =  Среднюю нагрузку на диск за последние 10 минут в разделе /home;'
              u'\n -hta  =  Top 10 сайтов по количеству запросов и top 10 ip адресов для каждого из сайтов за последние N-минут;'
              u'\n -mt   =  Количество запросов mysql за последние 5 минут (только количество тредов);'
              u'\n -m    =  Изменение свободного места за последний час для раздела /home;')


if __name__ == '__main__':
    main()