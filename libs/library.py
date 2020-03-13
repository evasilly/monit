# -*- coding: utf-8 -*-

import datetime
from decimal import Decimal
from PyQt4.QtCore import *

def decorateString(s):
    u = unicode(s)
    return '\'' + u.replace('\\', '\\\\').replace('\'', '\\\'') + '\''


class CException(Exception):
    def __str__(self):
        if isinstance(self.message, unicode):
            return self.message.encode('utf8')
        else:
            return self.message

    def __unicode__(self):
        if isinstance(self.message, unicode):
            return self.message
        else:
            return super(Exception, self).__unicode__()


class CDatabaseException(CException):
    def __init__(self, message, sqlError=None):
        if sqlError:
            message = message + '\n' + unicode(sqlError.driverText()) + '\n' + unicode(sqlError.databaseText())
        CException.__init__(self, message)
        self.sqlError = sqlError

def formatDate(val, toString = True):
    formatString = 'dd.MM.yyyy'
    if toString:
        if isinstance(val, QVariant):
            val = val.toDate()
        return unicode(val.toString(formatString))
    else:
        if isinstance(val, QVariant):
            val = val.toString()
        return QDate.fromString(val, formatString)


def formatTime(val):
    if isinstance(val, QVariant):
        val = val.toDate()
    return unicode(val.toString('H:mm'))


def formatDateTime(val):
    if isinstance(val, QVariant):
        val = val.toDateTime()
    return unicode(val.toString('dd.MM.yyyy H:mm'))


def forceString(val):
    if isinstance(val, QVariant):
        valType = val.type()
        if  valType == QVariant.Date:
            return formatDate(val.toDate())
        elif valType == QVariant.DateTime:
            return formatDateTime(val.toDateTime())
        elif valType == QVariant.Time:
            return formatTime(val.toTime())
        else:
            val = val.toString()
    if isinstance(val, QDate):
        return formatDate(val)
    if isinstance(val, QDateTime):
        return formatDateTime(val)
    if isinstance(val, QTime):
        return formatTime(val)
    if val is None:
        return u''
    if isinstance(val, QStringRef):
        val = val.toString()
    return unicode(val)


def forceRef(val):
    if isinstance(val, QVariant):
        if val.isNull():
            val = None
        else:
            val = int(val.toULongLong()[0])
            if val == 0:
                val = None
    return val


def forceInt(val):
    if isinstance(val, QVariant):
        return val.toInt()[0]
    elif ((val is None) # Если пустое значение
          or (isinstance(val, QString) and not val.toInt()[1]) # Если нечисловой QString
          or isinstance(val, basestring) and not val.isdigit()): # Если нечисловая строка
        return 0
    return int(val)


def toVariant(v):
    if v is None:
        return QVariant()
    t = type(v)
    if t == QVariant:
        return v
    elif t == datetime.time:
        return QVariant(QTime(v))
    elif t == datetime.datetime:
        return QVariant(QDateTime(v))
    elif t == datetime.date:
        return QVariant(QDate(v))
    elif t == Decimal:
        return QVariant(unicode(v))
    else:
        return QVariant(v)