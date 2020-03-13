# -*- coding: utf-8 -*-

import datetime
from email.mime.multipart import MIMEMultipart

currDate = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')

connectionInfo = {
    'driverName'      : 'mysql',
    'host'            : 'localhost',
    'port'            : 3306,
    'database'        : 'monitSrv',
    'user'            : 'dbuser',
    'password'        : 'dbpassword'
}

msg = MIMEMultipart()

msg['From'] = u"szpvmonitoring@gmail.com"
msg['To'] = u"apfelrobbe@gmail.com"
msg['Subject'] = u"Monit Allert_" + str(currDate)