    # -*- coding: utf-8 -*-

import collections
import itertools
import re
import copy
from library import *
from PyQt4 import QtCore, QtGui, QtSql
from PyQt4.QtCore import *


DocumentTables = []


def registerDocumentTable(tableName):
    DocumentTables.append(tableName)


class CField(object):
    def __init__(self, database, tableName, field, fieldType=None):
        self.database = database  # type: CDatabase
        self.tableName = tableName
        if isinstance(field, QtSql.QSqlField):
            self.fieldName = database.escapeFieldName(field.name())
            self.field = field
            self.isSurrogate = False
        elif isinstance(field, (basestring, QString)):
            self.fieldName = forceString(field)
            self.field = QtSql.QSqlField(field, fieldType if isinstance(fieldType, QVariant.Type) else QVariant.String)
            self.isSurrogate = True

    def __str__(self):
        return self.name()

    def name(self):
        prefix = (self.tableName + '.') if self.tableName else ''
        return prefix + self.fieldName

    def fieldType(self):
        return self.field.type()

    def alias(self, name=''):
        return ' '.join([self.name(),
                         self.database.aliasSymbol,
                         self.database.escapeFieldName(name) if name else self.fieldName])

    def asc(self):
        return u'{0} ASC'.format(self.name())

    def desc(self):
        return u'{0} DESC'.format(self.name())

    def toTable(self, tableName):
        return CField(self.database, tableName, self.field)

    def signEx(self, sign, expr=None, modifierTemplate=None):
        exprPart = [expr if modifierTemplate is None else (modifierTemplate % expr)] if expr is not None else []
        return ' '.join([self.name() if modifierTemplate is None else (modifierTemplate % self.name()),
                         sign]
                        + exprPart)

    def formatValue(self, value):
        if isinstance(value, CField):
            return value.name()
        else:
            return self.database.formatValueEx(self.fieldType(), value)

    def sign(self, sign, val, modifierTemplate=None):
        return self.signEx(sign, self.formatValue(val), modifierTemplate)

    def eq(self, val):
        return self.isNull() if val is None else self.sign('=', val)

    def eqEx(self, val):
        return self.isNull() if val is None else self.signEx('=', val)

    def __eq__(self, val):
        return CSqlExpression(self.database, self.eq(self.database.forceField(val)))

    def lt(self, val):
        return self.sign('<', val)

    def __lt__(self, val):
        return CSqlExpression(self.database, self.lt(self.database.forceField(val)))

    def le(self, val):
        return self.sign('<=', val)

    def __le__(self, val):
        return CSqlExpression(self.database, self.le(self.database.forceField(val)))

    def gt(self, val):
        return self.sign('>', val)

    def __gt__(self, val):
        return CSqlExpression(self.database, self.gt(self.database.forceField(val)))

    def ge(self, val):
        return self.sign('>=', val)

    def ne(self, val):
        return self.isNotNull() if val is None else self.sign('!=', val)

    def isNull(self):
        return self.signEx('IS NULL', None)

    def isNotNull(self):
        return self.signEx('IS NOT NULL', None)

    def setNull(self):
        return self.signEx('=', 'NULL')

    def isZeroDate(self):
        return self.eq(self.database.valueField('0000-00-00'))

    def isNullDate(self):
        return self.database.joinOr([self.isNull(),
                                     self.isZeroDate()])

    def __not__(self):
        return CSqlExpression(self.database, u'NOT {0}'.format(self))


    def __div__(self, other):
        return CSqlExpression(self.database, u'{0} / {1}'.format(self.database.formatArg(self),
                                                                 self.database.formatArg(other)))

    def decoratedlist(self, itemList):
        if not itemList:
            return '()'
        else:
            decoratedList = [self.formatValue(value) for value in itemList]
            return unicode('(' + (','.join(decoratedList)) + ')')

    def inlist(self, itemList, *args):
        if not isinstance(itemList, (list, tuple, set)):
            itemList = args + (itemList,)
        return '0' if not itemList else self.signEx('IN', self.decoratedlist(itemList))

    def notInlist(self, itemList):
        if not itemList:
            return '1'  # true
        else:
            return self.signEx('NOT IN', self.decoratedlist(itemList))

    def inInnerStmt(self, stmt):
        if not stmt:
            return '0'
        else:
            return self.signEx('IN', u'(%s)' % stmt)

    def notInInnerStmt(self, stmt):
        if not stmt:
            return '0'
        else:
            return self.signEx('NOT IN', u'(%s)' % stmt)

    def eqStmt(self, stmt):
        if not stmt:
            return '0'
        else:
            return self.signEx('=', u'(%s)' % stmt)


    def between(self, low, high):
        return u'(%s BETWEEN %s AND %s)' % (self.name(),
                                            self.database.formatArg(low),
                                            self.database.formatArg(high))

    def compareDatetime(self, otherDatetime, compareOperator, onlyDate=True):
        if otherDatetime is None:
            return self.isNull()

        return self.signEx(compareOperator, self.formatValue(otherDatetime),
                           u'DATE(%s)' if onlyDate else u'TIMESTAMP(%s)')

    def dateEq(self, val):
        return self.compareDatetime(val, u'=')

    def dateLe(self, val):
        return self.compareDatetime(val, u'<=')

    def dateLt(self, val):
        return self.compareDatetime(val, u'<')

    def dateGe(self, val):
        return self.compareDatetime(val, u'>=')

    def dateGt(self, val):
        return self.compareDatetime(val, u'>')

    def datetimeEq(self, val):
        return self.compareDatetime(val, u'=', onlyDate=False)

    def datetimeLe(self, val):
        return self.compareDatetime(val, u'<=', onlyDate=False)

    def datetimeLt(self, val):
        return self.compareDatetime(val, u'<', onlyDate=False)

    def datetimeGe(self, val):
        return self.compareDatetime(val, u'>=', onlyDate=False)

    def datetimeGt(self, val):
        return self.compareDatetime(val, u'>', onlyDate=False)

    def datetimeBetween(self, low, high):
        return 'TIMESTAMP(%s) BETWEEN TIMESTAMP(%s) AND TIMESTAMP(%s)' % (
            self.name(), self.formatValue(low), self.formatValue(high)
        )

    def dateBetween(self, low, high):
        return u'(DATE(%s) BETWEEN %s AND %s)' % (self.name(), self.formatValue(low), self.formatValue(high))

    def daysDiffLe(self, date, val):
        return u'DATEDIFF(%s, %s) <= %s' % (self.name(), self.formatValue(date), val)

    def monthGe(self, val):
        if val is None:
            return self.isNull()
        else:
            return 'MONTH(' + self.name() + ')>=MONTH(' + unicode(self.formatValue(val) + ')')

    def yearGe(self, val):
        if val is None:
            return self.isNull()
        else:
            return 'YEAR(' + self.name() + ')>=YEAR(' + unicode(self.formatValue(val) + ')')

    def yearEq(self, val):
        if val is None:
            return self.isNull()
        else:
            return 'YEAR(' + self.name() + ')=YEAR(' + unicode(self.formatValue(val) + ')')

    def lenGt(self, val):
        if val is None:
            return self.isNull()
        else:
            return 'LENGTH(' + self.name() + ') > ' + forceString(val)

    def lenEq(self, val):
        if val is None:
            return self.isNull()
        else:
            return 'LENGTH(' + self.name() + ') = ' + forceString(val)

    def __repr__(self):
        return u'<CField {0}>'.format(self.name())


class CTable(object):
    def __init__(self, tableName, database, alias=''):
        self.fields = []  # type: list[CField]
        self.fieldsDict = {}  # type: dict[str,CField]
        self.database = database  # type: CDatabase
        self.tableName = unicode(tableName)
        self.aliasName = alias
        self.isQueryTable = True if self.tableName.strip().lower().startswith('select') else False
        # если имя таблицы начинается с 'SELECT ', то предположить, что это таблица-подзапрос, а она должна иметь псевдоним
        if self.isQueryTable and not alias:
            self.aliasName = 'someSubQueryTable'
            if not self.tableName.endswith(' '):
                self.tableName += u' '
        self._idField = None
        self._idFieldName = None
        record = database.record(self.tableName)
        for i in xrange(record.count()):
            qtfield = record.field(i)
            field = CField(self.database, self.tableName, qtfield)
            self.fields.append(field)
            fieldName = str(qtfield.name())
            if not self.fieldsDict.has_key(fieldName):
                self.fieldsDict[fieldName] = field

    def name(self, alias=''):
        alias = alias or self.aliasName
        if alias:
            return ' '.join([self.tableName if not self.isQueryTable else '(%s)' % self.tableName,
                             self.database.aliasSymbol,
                             unicode(alias)])
        else:
            return self.tableName

    @property
    def aliasOrName(self):
        return self.aliasName if self.aliasName else self.tableName

    def alias(self, name):
        newTable = copy.copy(self)
        newTable.aliasName = unicode(name)
        return newTable

    def setIdFieldName(self, name):
        self._idField = self.__getitem__(name)
        self._idFieldName = name

    def idField(self):
        if not self._idField:
            self.setIdFieldName('id')
        ##            raise CDatabaseException(CDatabase.errNoIdField % (self.tableName))
        return self._idField

    def idFieldName(self):
        if not self._idField:
            if self.hasField('id'):
                self.setIdFieldName('id')
            else:
                self.setIdFieldName(self.fields[0].fieldName.replace('`', ''))
        ##            raise CDatabaseException(CDatabase.errNoIdField % (self.tableName))
        return self._idFieldName

    def __getitem__(self, key):
        key = str(key)
        result = self.fieldsDict.get(key, None)
        if result:
            return result if not self.aliasName else result.toTable(self.aliasName)
        elif key == '*':
            return CSqlExpression(self.database, '{0}.*'.format(self.tableName))
        else:
            raise CDatabaseException(CDatabase.errFieldNotFound % (self.tableName, key))

    def hasField(self, fieldName):
        return self.fieldsDict.has_key(fieldName)

    def newRecord(self, fields=None, otherRecord=None):
        record = QtSql.QSqlRecord()
        for field in self.fields:
            if fields and field.field.name() not in fields:
                continue
            record.append(QtSql.QSqlField(field.field))
            if otherRecord and otherRecord.contains(field.field.name()) and str(field) != str(self._idField):
                record.setValue(field.field.name(), otherRecord.value(field.field.name()))
        return record

    def beforeInsert(self, record):
        return

    def beforeUpdate(self, record):
        return

    def beforeDelete(self, record):
        return

    def join(self, table, onCond):
        return self.database.join(self, table, onCond)

    def leftJoin(self, table, onCond):
        return self.database.leftJoin(self, table, onCond)

    def innerJoin(self, table, onCond):
        return self.database.innerJoin(self, table, onCond)

    def __repr__(self):
        return u'<CTable "%s">' % self.name()


class CJoin(object):
    def __init__(self, firstTable, secondTable, onCond, stmt='JOIN'):
        self.firstTable = firstTable
        self.secondTable = secondTable
        self.onCond = onCond
        self.stmt = stmt
        self.database = firstTable.database
        assert firstTable.database == secondTable.database

    def name(self):
        return u'%s %s %s ON %s' % (self.firstTable.name(), self.stmt, self.secondTable.name(), self.onCond)

    def join(self, table, onCond):
        u""" :rtype: CJoin """
        return self.database.join(self, table, onCond)

    def leftJoin(self, table, onCond):
        u""" :rtype: CJoin """
        return self.database.leftJoin(self, table, onCond)

    def innerJoin(self, table, onCond):
        u""" :rtype: CJoin """
        return self.database.innerJoin(self, table, onCond)

    def getMainTable(self):
        if isinstance(self.firstTable, CJoin):
            return self.firstTable.getMainTable()
        else:
            return self.firstTable

    def getAllTables(self):
        if isinstance(self.firstTable, CJoin):
            return self.firstTable.getAllTables() + [self.secondTable]
        else:
            return [self.secondTable]

    def isTableJoin(self, table):
        return table in self.getAllTables()

    def idField(self):
        return self.firstTable.idField()

    def beforeUpdate(self, record):
        return

    def __repr__(self):
        return u'<CJoin "%s">' % self.name()


class CUnionTable(object):
    def __init__(self, db, firstStmt, secondStmt, alias='', distinct=True):
        def getColsCount(stmt):
            match = re.search('(<=SELECT ).*(?= FROM)', stmt)
            if match:
                return len(match.group(0).split(','))
            return 0

        self.firstStmt = firstStmt
        self.secondStmt = secondStmt
        self.aliasName = alias
        self.database = db
        self.distinct = distinct
        assert getColsCount(firstStmt) == getColsCount(secondStmt)

    def __getitem__(self, fieldName):
        return CSqlExpression(self.database, ((self.aliasName + '.') if self.aliasName else '') + self.database.escapeFieldName(fieldName))

    def name(self):
        return u'((%s) %s (%s)) AS %s' % (self.firstStmt,
                                          'UNION' if self.distinct else 'UNION ALL',
                                          self.secondStmt,
                                          self.aliasName)

    def join(self, table, onCond):
        return self.database.join(self, table, onCond)

    def leftJoin(self, table, onCond):
        return self.database.leftJoin(self, table, onCond)

    def innerJoin(self, table, onCond):
        return self.database.innerJoin(self, table, onCond)


class CSqlExpression(CField):
    def __init__(self, db, name, fieldType=QVariant.String):
        super(CField, self).__init__()
        self.database = db
        self.tableName = ''
        self.fieldName = name
        self._fieldType = fieldType

    def convertUtf8(self):
        return CSqlExpression(self.database,
                              u'CONVERT({0}, CHAR CHARACTER SET utf8)'.format(self.name()))

    def __str__(self):
        return self.fieldName

    def name(self):
        return self.fieldName

    def fieldType(self):
        return self._fieldType

    def setType(self, fieldType):
        self._fieldType = fieldType
        return self

    def alias(self, name=''):
        return ' '.join([self.name(),
                         self.database.aliasSymbol,
                         self.database.escapeFieldName(name) if name else self.fieldName])


class CDatabase(QObject):
    aliasSymbol = 'AS'

    errUndefinedDriver = u'Драйвер базы данных "%s" не зарегистрирован'
    errCannotConnectToDatabase = u'Невозможно подключиться к базе данных "%s"'
    errCannotOpenDatabase = u'Невозможно открыть базу данных "%s"'
    errDatabaseIsNotOpen = u'База данных не открыта'

    # добавлено для formatQVariant
    convMethod = {
        QVariant.Int      : lambda val: unicode(val.toInt()[0]),
        QVariant.UInt     : lambda val: unicode(val.toUInt()[0]),
        QVariant.LongLong : lambda val: unicode(val.toLongLong()[0]),
        QVariant.ULongLong: lambda val: unicode(val.toULongLong()[0]),
        QVariant.Double   : lambda val: unicode(val.toDouble()[0]),
        QVariant.Bool     : lambda val: u'1' if val.toBool() else u'0',
        QVariant.Char     : lambda val: decorateString(val.toString()),
        QVariant.String   : lambda val: decorateString(val.toString()),
        QVariant.Date     : lambda val: decorateString(val.toDate().toString(Qt.ISODate)),
        QVariant.Time     : lambda val: decorateString(val.toTime().toString(Qt.ISODate)),
        QVariant.DateTime : lambda val: decorateString(val.toDateTime().toString(Qt.ISODate)),
        QVariant.ByteArray: lambda val: 'x\'' + str(val.toByteArray().toHex()) + '\'',
        QVariant.Color    : lambda val: unicode(QtGui.QColor(val).name()),
    }

    connected = pyqtSignal()
    disconnected = pyqtSignal()

    def __init__(self, afterConnectFunc=None):
        QObject.__init__(self)
        self.deadLockRepeat = 3
        self.db = None  # type: QtSql.QSqlDatabase
        self.tables = {}
        # restoreConnectState:  0 - соединение не было утеряно, 1 - соединение утеряно, пытаться переподключиться
        #                       2 - соединение утеряно, переподключение не требуется.
        self.restoreConnectState = 0

        self._transactionCallStackByLevel = []
        self._openTransactionsCount = 0
        self._func = None
        self._proc = None
        self._connectionInfo = dict()

    @property
    def inTransaction(self):
        return self._openTransactionsCount > 0

    @property
    def fn(self):
        return self._fn

    def hasTable(self, tableName):
        raise NotImplementedError

    def getConnectionId(self):
        return None

    def makeField(self, fromString):
        return CSqlExpression(self, fromString)

    def valueField(self, value):
        return self.makeField(self.formatArg(value))

    def forceField(self, value):
        if isinstance(value, CField): return value
        elif isinstance(value, (str, unicode)): return self.makeField(value)
        return self.valueField(value)

    def unionTable(self, stmt1, stmt2, alias, distinct=True):
        return CUnionTable(self, stmt1, stmt2, alias, distinct)

    def escapeIdentifier(self, name, identifierType):
        return unicode(self.driver().escapeIdentifier(name, identifierType))

    def escapeFieldName(self, name):
        return unicode(self.driver().escapeIdentifier(name, QtSql.QSqlDriver.FieldName))

    def escapeTableName(self, name):
        return unicode(self.driver().escapeIdentifier(name, QtSql.QSqlDriver.TableName))

    escapeSchemaName = escapeTableName

    @staticmethod
    def dummyRecord():
        return QtSql.QSqlRecord()

    @classmethod
    def formatQVariant(cls, fieldType, val):
        if val.isNull():
            return 'NULL'
        return cls.convMethod[fieldType](val)

    @classmethod
    def formatValue(cls, field):
        return cls.formatQVariant(field.type(), field.value())

    @classmethod
    def formatValueEx(cls, fieldType, value):
        if isinstance(value, QVariant):
            return cls.formatQVariant(fieldType, value)
        else:
            return cls.formatQVariant(fieldType, toVariant(value))

    @classmethod
    def formatArg(cls, value):
        if isinstance(value, CField):
            return value.name()
        else:
            qValue = toVariant(value)
            return cls.formatValueEx(qValue.type(), qValue)

    def createConnection(self, driverName, connectionName, serverName, serverPort, databaseName, userName, password):
        if connectionName:
            if not QtSql.QSqlDatabase.contains(connectionName):
                db = QtSql.QSqlDatabase.addDatabase(driverName, connectionName)
            else:
                db = QtSql.QSqlDatabase.database(connectionName)
        else:
            db = QtSql.QSqlDatabase.addDatabase(driverName)
        if not db.isValid():
            raise CDatabaseException(CDatabase.errCannotConnectToDatabase % driverName, db.lastError())
        db.setHostName(serverName)
        if serverPort:
            db.setPort(serverPort)
        db.setDatabaseName(databaseName)
        db.setUserName(userName)
        db.setPassword(password)
        self.db = db
        # self._connectionInfo = {
        #     'driverName': driverName,
        #     'connectionName': connectionName,
        #     'serverName': serverName,
        #     'serverPort': serverPort,
        #     'databaseName': databaseName,
        #     'userName': userName,
        #     'password': password
        # }

    def isConnectionLostError(self, sqlError):
        driverText = forceString(sqlError.driverText()).lower()
        if 'lost connection' in driverText:
            return True
        if 'server has gone away' in driverText:
            return True
        return False

    def connectUp(self):
        if not self.db.open():
            raise CDatabaseException(CDatabase.errCannotOpenDatabase % self.db.databaseName(), self.db.lastError())
        self.restoreConnectState = 0
        self._transactionCallStackByLevel = []
        self._openTransactionsCount = 0
        self.connected.emit()

    def reconnect(self):
        if not (self.db and self.db.isValid):
            return False
        if self.db.isOpen():
            self.db.close()
        if not self.db.open():
            # self.connectDown()
            return False
        self.connected.emit()
        self._schemaTables = None
        return True

    def connectDown(self):
        self.db.close()
        self._transactionCallStackByLevel = []
        self._openTransactionsCount = 0
        self.disconnected.emit()

    def close(self):
        if self.db:
            connectionName = self.db.connectionName()
            self.connectDown()
            self.driver = None
            self.db = None
            QtSql.QSqlDatabase.removeDatabase(connectionName)
        self.tables = {}

    def getTestConnectionStmt(self):
        return u'select \'test connection query\';'

    def checkdb(self):
        if not self.db or not self.db.isValid() or not self.db.isOpen():
            raise CDatabaseException(CDatabase.errDatabaseIsNotOpen)

    def isValid(self):
        return self.db is not None and self.db.isValid() and self.db.isOpen()

    def checkConnect(self, quietRestoreConnection=False):
        self.checkdb()
        sqlError = None

        testQuery = QtSql.QSqlQuery(self.db)
        stmt = self.getTestConnectionStmt()
        if testQuery.exec_(stmt):
            return
        else:
            sqlError = testQuery.lastError()

        if self.isConnectionLostError(sqlError):
            if self.restoreConnection(quietRestoreConnection):
                return
            else:
                self.connectDown()
                # raise CDatabaseException(CDatabase.errConnectionLost)

        raise self.onError(stmt, sqlError)

    def driver(self):
        return self.db.driver()

    def forceTable(self, table, idFieldName='id'):
        if isinstance(table, (CTable, CJoin, CUnionTable)):
            return table
        elif isinstance(table, basestring):
            return self.table(table, idFieldName=idFieldName)
        else:
            raise AssertionError, u'Недопустимый тип'

    def mainTable(self, tableExpr, idFieldName='id'):
        if isinstance(tableExpr, (CTable, CJoin)):
            return tableExpr
        elif isinstance(tableExpr, basestring):
            name = tableExpr.split(None, 1)[0] if ' ' in tableExpr else tableExpr
            return self.table(name, idFieldName)
        else:
            raise AssertionError, u'Недопустимый тип'

    def getTableName(self, table):
        if isinstance(table, basestring):
            if table.strip().lower().startswith('select '):
                return ' '.join([u'(%s)' % table,
                                 self.aliasSymbol,
                                 u'someQueryTable'])
            return table
        return self.forceTable(table).name()

    def formatDate(self, val):
        return '\'' + str(val.toString(Qt.ISODate)) + '\''

    def formatTime(self, val):
        return '\'' + str(val.toString(Qt.ISODate)) + '\''

    def coalesce(self, *args):
        return CSqlExpression(self, u'COALESCE({0})'.format(u', '.join(map(self.formatArg, args))))

    def ifnull(self, exp1, exp2):
        return CSqlExpression(self, u'IFNULL({0}, {1})'.format(self.forceField(exp1), self.forceField(exp2)))

    def ifnullstr(self,exp1,exp2):
        return CSqlExpression(self, u'IFNULL({0}, {1})'.format(self.forceField(exp1), exp2))

    def joinAnd(cls, itemList):
        return (('((' if len(itemList) > 1 else '(') +
                u') AND ('.join(u'%s' % item for item in itemList) +
                ('))' if len(itemList) > 1 else ')')) if itemList else ''

    def joinOr(cls, itemList):
        return (('((' if len(itemList) > 1 else '(') +
                u') OR ('.join(u'%s' % item for item in itemList) +
                ('))' if len(itemList) > 1 else ')')) if itemList else ''

    def not_(self, expr):
        return u'NOT ({0})'.format(expr)

    def if_(self, cond, thenPart, elsePart):
        return CSqlExpression(self, u'IF({0}, {1}, {2})'.format(cond,
                                                                self.forceField(thenPart),
                                                                self.forceField(elsePart)), QVariant.Bool)

    def case(self, field, caseDict, elseValue=None):
        parts = [
            u'WHEN {0} THEN {1}'.format(self.forceField(cond), self.forceField(value))
            for cond, value in caseDict.iteritems()
        ]
        if elseValue:
            parts.append(u'ELSE {0}'.format(self.forceField(elseValue)))
        return CSqlExpression(self, u'CASE {0} {1} END'.format(self.forceField(field), u' '.join(parts)))

    def concat(self, *args):
        return CSqlExpression(self, u'CONCAT({0})'.format(u', '.join(map(self.formatArg, args))))

    def concat_ws(self, sep, *args):
        return CSqlExpression(self, u'CONCAT_WS({0}, {1})'.format(self.formatArg(sep), u', '.join(map(self.formatArg, args))))

    def group_concat(self, item, distinct=False):
        return CSqlExpression(self, u'GROUP_CONCAT({0}{1})'.format(u'DISTINCT ' if distinct else u'', item))

    def count(self, item, distinct=False):
        return CSqlExpression(self, u'COUNT({0}{1})'.format(u'DISTINCT ' if distinct else u'', item if item else '*'), QVariant.Int)

    def countIf(self, cond, item, distinct=False):
        return self.count(self.if_(cond, item, 'NULL'), distinct)

    def datediff(self, dateTo, dateFrom):
        return CSqlExpression(self, u'DATEDIFF({0}, {1})'.format(dateTo, dateFrom), QVariant.Int)

    def addDate(self, date, count, type='DAY'):
        return CSqlExpression(self, u'ADDDATE({0}, INTERVAL {1} {2})'.format(self.formatArg(date),
                                                                             self.formatArg(count),
                                                                             type),
                              QVariant.Date)

    def subDate(self, date, count, type='DAY'):
        return CSqlExpression(self, u'SUBDATE({0}, INTERVAL {1} {2})'.format(self.formatArg(date),
                                                                             self.formatArg(count),
                                                                             type),
                              QVariant.Date)

    def date(self, date):
        return CSqlExpression(self, u'DATE({0})'.format(self.forceField(date)))

    def dateYear(self, date):
        return CSqlExpression(self, u'YEAR({0})'.format(date), QVariant.Int)

    def dateQuarter(self, date):
        return CSqlExpression(self, u'QUARTER({0})'.format(date), QVariant.Int)

    def dateMonth(self, date):
        return CSqlExpression(self, u'MONTH({0})'.format(date), QVariant.Int)

    def dateDay(self, date):
        return CSqlExpression(self, u'DAY({0})'.format(date), QVariant.Int)

    def isZeroDate(self, date):
        return self.forceField(date).eq(self.valueField('0000-00-00'))

    def isNullDate(self, date):
        return self.joinOr([self.forceField(date).isNull(),
                            self.isZeroDate(date)])

    def sum(self, item):
        return CSqlExpression(self, u'SUM({0})'.format(item), item.fieldType() if isinstance(item, CField) else toVariant(item).type())

    def max(self, item):
        return CSqlExpression(self, u'MAX({0})'.format(item), QVariant.Int)

    def min(self, item):
        return CSqlExpression(self, u'MIN({0})'.format(item), QVariant.Int)

    def least(self, *args):
        return CSqlExpression(self, u'LEAST({0})'.format(u', '.join(map(self.formatArg, args))))

    def greatest(self, *args):
        return CSqlExpression(self, u'GREATEST({0})'.format(u', '.join(map(self.formatArg, args))))

    def left(self, str, len):
        return CSqlExpression(self, u'LEFT({0}, {1})'.format(str, len), QVariant.String)

    def right(self, str, len):
        return CSqlExpression(self, u'RIGHT({0}, {1})'.format(str, len), QVariant.String)

    def curdate(self):
        return CSqlExpression(self, u'CURDATE()', QVariant.Date)

    def now(self):
        return CSqlExpression(self, u'NOW()', QVariant.DateTime)

    def joinOp(self, op, *args):
        return CSqlExpression(self, u'({0})'.format(op.join(map(self.formatArg, args))), QVariant.Int)

    def prepareFieldList(cls, fields):
        if isinstance(fields, (list, tuple)):
            return ', '.join([field.name() if isinstance(field, CField) else field for field in fields])
        return fields.name() if isinstance(fields, CField) else fields

    def CONCAT_WS(fields, alias='', separator=' '):
        result = 'CONCAT_WS('
        result += ('\'' + separator + '\'')
        result += ', '
        if isinstance(fields, (list, tuple)):
            for field in fields:
                result += field.name() if isinstance(field, CField) else field
                result += ', '
        else:
            result += fields.name() if isinstance(fields, CField) else fields
            result += ', '
        result = result[:len(result) - 2]
        result += ')'
        if alias: result += (' AS ' + '`' + alias + '`')

        return result

    def dateTimeIntersection(cls, fieldBegDateTime, fieldEndDateTime, begDateTime, endDateTime):
        if fieldBegDateTime is not None and fieldEndDateTime is not None and begDateTime is not None and endDateTime is not None:
            return cls.joinAnd([
                cls.joinOr([fieldBegDateTime.datetimeGe(begDateTime), fieldEndDateTime.datetimeGe(begDateTime),
                            fieldEndDateTime.isNull()]),
                cls.joinOr([fieldBegDateTime.datetimeLe(endDateTime), fieldEndDateTime.datetimeLe(endDateTime),
                            fieldBegDateTime.isNull()])
            ])
        else:
            return ''

    def prepareWhere(cls, cond):
        if isinstance(cond, (list, tuple)):
            cond = cls.joinAnd(cond)
        return u'WHERE %s' % cond if cond else u''

    def prepareOrder(cls, orderFields):
        if isinstance(orderFields, (list, tuple)):
            orderFields = ', '.join(
                [orderField.name() if isinstance(orderField, CField) else orderField for orderField in orderFields])
        if orderFields:
            return ' ORDER BY ' + (orderFields.name() if isinstance(orderFields, CField) else orderFields)
        else:
            return ''

    def prepareGroup(cls, groupFields):
        if isinstance(groupFields, (list, tuple)):
            groupFields = ', '.join(
                [groupField.name() if isinstance(groupField, CField) else groupField for groupField in groupFields])
        if groupFields:
            return ' GROUP BY ' + (groupFields.name() if isinstance(groupFields, CField) else groupFields)
        else:
            return ''

    def prepareHaving(cls, havingFields):
        if isinstance(havingFields, (list, tuple)):
            havingFields = cls.joinAnd(
                [havingField.name() if isinstance(havingField, CField) else havingField for havingField in
                 havingFields])
        if havingFields:
            return ' HAVING ' + (havingFields.name() if isinstance(havingFields, CField) else havingFields)
        else:
            return ''

    def prepareSelect(self, fields=None, table=None, rowNumberFieldName=None, isDistinct=False):
        parts = ['SELECT']
        if isDistinct:
            parts.append('DISTINCT')

        tableName = self.getTableName(table) if table is not None else ''
        if tableName and rowNumberFieldName and isinstance(fields, list):
            fields.insert(0, '@__rowNumber := @__rowNumber + 1 AS %s' % rowNumberFieldName)
            tableName += ', (select @__rowNumber := 0) as __rowNumberInit'

        parts.append(self.prepareFieldList(fields))

        if tableName:
            parts.extend(['FROM', tableName])

        return ' '.join(parts)

    def selectStmt(self, table=None, fields='*', where=None, group=None, order=None, limit=None, isDistinct=False,
                   rowNumberFieldName=None, having=None):
        return ' '.join(it for it in (
            self.prepareSelect(fields, table, rowNumberFieldName, isDistinct),
            self.prepareWhere(where),
            self.prepareGroup(group),
            self.prepareHaving(having),
            self.prepareOrder(order)
        ) if it)

    def selectMax(self, table, col='id', where=''):
        return self.selectStmt(table, self.max(col), where)

    def selectMin(self, table, col='id', where=''):
        return self.selectStmt(table, self.min(col), where)

    def selectExpr(self, fields):
        query = self.query(self.selectStmt(fields=fields))
        if query.first():
            record = query.record()
            return record
        return None

    def notExistsStmt(self, table, where):
        return 'NOT %s' % self.existsStmt(table, where)

    def table(self, tableName, idFieldName='id'):
        if self.tables.has_key(tableName):
            return self.tables[tableName]
        else:
            table = CTable(tableName, self)
            if u'id' in [v.fieldName for v in table.fields]:
                table.setIdFieldName(idFieldName)
            self.tables[tableName] = table
            return table

    def join(self, firstTable, secondTable, onCond, stmt='JOIN'):
        if isinstance(onCond, (list, tuple)):
            onCond = self.joinAnd(onCond)
        return CJoin(self.forceTable(firstTable), self.forceTable(secondTable), onCond, stmt)

    def leftJoin(self, firstTable, secondTable, onCond):
        return self.join(firstTable, secondTable, onCond, 'LEFT JOIN')

    def innerJoin(self, firstTable, secondTable, onCond):
        return self.join(firstTable, secondTable, onCond, 'INNER JOIN')

    def record(self, tableName):
        self.checkdb()
        if tableName.strip().lower().startswith('select'):
            res = self.query(tableName + self.prepareLimit(1)).record()
        else:
            parts = tableName.split('.', 1)
            if len(parts) <= 1:
                res = self.db.record(tableName)
                if not res:  # роверка соединения и повторная попытка в случае потери подключения
                    self.checkConnect()
                    res = self.db.record(tableName)
            else:
                currentDatabaseName = self.db.databaseName()
                databaseName = parts[0]
                # проверка подключения производится внутри query
                self.query('USE %s' % self.escapeSchemaName(databaseName))
                res = self.db.record(parts[1])
                self.query('USE %s' % self.escapeSchemaName(currentDatabaseName))

        if res.isEmpty():
            raise CDatabaseException(CDatabase.errTableNotFound % tableName)
        return res

    def recordFromDict(self, tableName, dct):
        table = self.forceTable(tableName)
        rec = table.newRecord(fields=dct.keys())
        for fieldName, value in dct.iteritems():
            rec.setValue(fieldName, toVariant(value))
        return rec

    def insertFromDict(self, tableName, dct):
        return self.insertRecord(tableName, self.recordFromDict(tableName, dct))

    def insertMultipleFromDict(self, tableName, lst):
        for dct in lst:
            self.insertRecord(tableName, self.recordFromDict(tableName, dct))

    def query(self, stmt, quietReconnect=False):
        self.checkdb()
        result = QtSql.QSqlQuery(self.db)
        result.setForwardOnly(True)
        result.setNumericalPrecisionPolicy(QtSql.QSql.LowPrecisionDouble)
        repeatCounter = 0
        needRepeat = True
        while needRepeat:
            needRepeat = False
            if not result.exec_(stmt):
                lastError = result.lastError()
                if lastError.databaseText().contains(self.returnedDeadlockErrorText):
                    needRepeat = repeatCounter <= self.deadLockRepeat
                elif self.isConnectionLostError(lastError):
                    if self.restoreConnection(quietReconnect or self.restoreConnectState == 1):
                        needRepeat = True
                    else:
                        self.connectDown()
                else:
                    needRepeat = False
                    self.onError(stmt, lastError)
            repeatCounter += 1
        return result

    def onError(self, stmt, sqlError):
        raise CDatabaseException(stmt + u'\n' + CDatabase.errQueryError % stmt, sqlError)

    @staticmethod
    def checkDatabaseError(lastError, stmt=None):
        if lastError.isValid() and lastError.type() != QtSql.QSqlError.NoError:
            message = u'Неизвестная ошибка базы данных'
            if lastError.type() == QtSql.QSqlError.ConnectionError:
                message = u'Ошибка подключения к базе данных'
            elif lastError.type() == QtSql.QSqlError.StatementError:
                message = u'Ошибка SQL-запроса'
            elif lastError.type() == QtSql.QSqlError.TransactionError:
                message = u'Ошибка SQL-запроса'
            if stmt:
                message += u'\n(%s)\n' % stmt
            raise CDatabaseException(message, lastError)

    def getRecordEx(self, table=None, cols=None, where='', order='', stmt=None):
        if stmt is None: stmt = self.selectStmt(table, cols, where, order=order, limit=1)
        query = self.query(stmt)
        return query.record() if query.first() else None

    def getRecord(self, table, cols, itemId):
        idCol = self.mainTable(table).idField()
        return self.getRecordEx(table, cols, idCol.eq(itemId))

    def updateRecord(self, table, record):
        table = self.forceTable(table)
        table.beforeUpdate(record)
        fieldsCount = record.count()
        idFieldName = table.idFieldName()
        values = []
        cond = ''
        itemId = None
        for i in range(fieldsCount):

            # My insertion for 'rbImageMap' table
            if table.name() == 'rbImageMap':
                pair = self.escapeFieldName(record.fieldName(i)) + '=' + self.formatValue(record.field(i))
                if record.fieldName(i) == idFieldName:
                    cond = pair
                    itemId = record.value(i).toInt()[0]
                elif record.fieldName(i) == 'image':
                    pass
                else:
                    values.append(pair)
            else:
                pair = self.escapeFieldName(record.fieldName(i)) + '=' + self.formatValue(record.field(i))
                if record.fieldName(i) == idFieldName:
                    cond = pair
                    itemId = record.value(i).toInt()[0]
                else:
                    values.append(pair)
        stmt = 'UPDATE ' + table.name() + ' SET ' + (', '.join(values)) + ' WHERE ' + cond
        self.query(stmt)
        return itemId

    def insertRecord(self, table, record):
        table = self.forceTable(table)
        table.beforeInsert(record)
        fieldsCount = record.count()
        fields = []
        values = []
        for i in xrange(fieldsCount):
            if not record.value(i).isNull():
                fields.append(self.escapeFieldName(record.fieldName(i)))
                values.append(self.formatValue(record.field(i)))
        stmt = ('INSERT INTO ' + table.name() +
                '(' + (', '.join(fields)) + ') ' +
                'VALUES (' + (', '.join(values)) + ')')
        itemId = self.query(stmt).lastInsertId().toInt()[0]
        idFieldName = table.idFieldName()
        record.setValue(idFieldName, QVariant(itemId))
        return itemId

    def insertMultipleRecords(self, table, records, keepOldFields=None, updateFields=None):
        if len(records) == 0:
            return

        firstRecord = records[0]
        fields = [forceString(firstRecord.fieldName(i)) for i in xrange(firstRecord.count())]

        values = u', '.join((u'(' + u', '.join(self.formatValue(record.field(idx))
                                               for idx in xrange(record.count())) + u')')
                            for record in records)
        parts = [
            self.prepareInsertInto(table, fields),
            'VALUES',
            values
        ]

        if keepOldFields or updateFields:
            parts.append(self.prepareOnDuplicateKeyUpdate(fields, updateFields, keepOldFields))
        self.query(u' '.join(parts))

    def insertMultipleRecordsByChunks(self, table, records, chunkSize=None):
        if len(records) == 0: return
        if chunkSize is None: chunkSize = len(records)
        table = self.forceTable(table)
        firstRecord = records[0]
        fields = [self.escapeFieldName(firstRecord.fieldName(i)) for i in xrange(firstRecord.count())]
        stmtInsert = u'INSERT INTO ' + table.name() + (u'(' + u', '.join(fields)) + u') VALUES '
        recordsIterator = iter(records)
        for _ in xrange(len(records) / chunkSize + 1):
            values = []
            for record in itertools.islice(recordsIterator, 0, chunkSize):
                values.append([self.formatValue(record.field(i)) for i in xrange(record.count())])
            if values:
                rows = [u'(' + u', '.join(value) + u')' for value in values]
                stmt = stmtInsert + ','.join(rows)
                self.query(stmt)

    def prepareInsertInto(self, table, fields):
        table = self.forceTable(table)
        return u'INSERT INTO {tableName} ({fields})'.format(
            tableName=table.name(),
            fields=u','.join(map(self.escapeFieldName, fields))
        )

    def prepareOnDuplicateKeyUpdate(self, fields, updateFields=None, keepOldFields=None):
        updateMap = {}
        if keepOldFields is not None:
            if updateFields is None:
                updateFields = list(set(fields).difference(set(keepOldFields)))
            else:
                for field in keepOldFields:
                    updateMap[field] = u'{field}={field}'.format(field=self.escapeFieldName(field))
        if updateFields is not None:
            for field in updateFields:
                updateMap[field] = u'{field}=VALUES({field})'.format(field=self.escapeFieldName(field))

        if updateMap:
            return u'ON DUPLICATE KEY UPDATE {0}'.format(u', '.join(sorted(updateMap.itervalues())))

        return u''

    def insertFromSelect(self, tableDest, tableSrc, fieldDict, cond=None, group=None, updateFields=None, excludeFields=None):
        if not fieldDict: return

        fields = fieldDict.keys()

        parts = [
            self.prepareInsertInto(tableDest, fields),
            self.selectStmt(tableSrc,
                            [field.alias(fieldName) if isinstance(field, CField) else CSqlExpression(self, field).alias(fieldName)
                             for fieldName, field in fieldDict.iteritems()],
                            cond,
                            group=group),
            self.prepareOnDuplicateKeyUpdate(fields, updateFields=updateFields, keepOldFields=excludeFields)
        ]

        self.query(u' '.join(parts))

    def insertValues(self, table, fields, values=None, keepOldFields=None, updateFields=None):
        if not (fields and values): return

        parts = [
            self.prepareInsertInto(table, fields),
            u'VALUES {0}'.format(u','.join(u'(%s)' % u','.join(map(self.formatArg, v)) for v in values)),
            self.prepareOnDuplicateKeyUpdate(fields, updateFields, keepOldFields)
        ]
        query = self.query(u' '.join(parts))
        lastInsertId = query.lastInsertId().toInt()[0]
        return lastInsertId

    def insertItem(self, table, dct, fields=None, keepOldFields=None, updateFields=None):
        if not fields:
            fields = dct.keys()
        values = [tuple(dct.get(field) for field in fields)]
        return self.insertValues(table, fields, values, keepOldFields=keepOldFields, updateFields=updateFields)

    def insertFromDictList(self, table, dctList, fields=None, keepOldFields=None, updateFields=None, chunkSize=None):
        if not dctList: return
        if not fields:
            fields = set(itertools.chain.from_iterable(dctList))
        if not chunkSize:
            chunkSize = len(dctList)

        listIterator = iter(dctList)
        for _ in xrange(0, len(dctList), chunkSize):
            values = [
                tuple(dct.get(field) for field in fields)
                for dct in itertools.islice(listIterator, 0, chunkSize)
            ]
            self.insertValues(table, fields, values, keepOldFields=keepOldFields, updateFields=updateFields)

    def insertOrUpdate(self, table, record, wasExceptionAndFirst=None):
        table = self.forceTable(table)
        idFieldName = table.idFieldName()
        if record.isNull(idFieldName):
            return self.insertRecord(table, record)
        elif wasExceptionAndFirst: # был некий эксепшн, и при этом это первая вставка в базу
            return self.insertRecord(table, record)
        else:
            return self.updateRecord(table, record)

    def deleteRecord(self, table, where):
        table = self.forceTable(table)  # type: CTable
        self.query(' '.join(it for it in ('DELETE FROM',
                                          table.name(),
                                          self.prepareWhere(where)) if it))

    def markRecordsDeleted(self, table, where):
        table = self.forceTable(table)
        stmt = 'UPDATE ' + table.name() + ' SET deleted=1 '
        if isinstance(where, tuple):
            where = list(where)
        if isinstance(where, list):
            where.append('deleted = 0')
        else:
            where = '(' + where + ') AND deleted = 0'
        stmt += self.prepareWhere(where)
        self.query(stmt)

    def updateRecords(self, table, expr, where=None):
        recQuery = None
        if table and expr:
            table = self.forceTable(table)
            if isinstance(expr, QtSql.QSqlRecord):
                tmpRecord = QtSql.QSqlRecord(expr)
                sets = []
            else:
                tmpRecord = QtSql.QSqlRecord()
                sets = []
                if not isinstance(expr, (list, tuple)):
                    sets = [expr]
                else:
                    sets.extend(expr)
            table.beforeUpdate(tmpRecord)
            for i in xrange(tmpRecord.count()):
                sets.append(table[tmpRecord.fieldName(i)].eq(tmpRecord.value(i)))
            stmt = ' '.join(it for it in ('UPDATE',
                                          table.name(),
                                          'SET',
                                          ', '.join(sets),
                                          self.prepareWhere(where)) if it)
            recQuery = self.query(stmt)
        return recQuery

    def getSum(self, table, sumCol='*', where=''):
        stmt = self.selectStmt(table, 'SUM(%s)' % sumCol, where)
        query = self.query(stmt)
        if query.first():
            return query.value(0)
        else:
            return 0

    def getCount(self, table=None, countCol='1', where='', stmt=None):
        if stmt is None:
            stmt = self.selectStmt(table, 'COUNT(%s)' % countCol, where)
        query = self.query(stmt)
        if query.first():
            return query.value(0).toInt()[0]
        else:
            return 0

    def getDistinctCount(self, table, countCol='*', where=''):
        stmt = self.selectStmt(table, 'COUNT(DISTINCT %s)' % countCol, where)
        query = self.query(stmt)
        if query.first():
            return query.value(0).toInt()[0]
        else:
            return 0

    def getColumnValues(self, table, column='id', where='', order='', limit=None, isDistinct=False,
                        handler=forceString):
        result = [
            handler(record.value(0))
            for record in self.iterRecordList(table, column, where, order, limit=limit, isDistinct=isDistinct)
        ]
        return result

    def getColumnValueMap(self, table, keyColumn='', valueColumn='', where='', order='', limit=None, isDistinct=False,
                          keyHandler=forceRef, valueHandler=forceRef):
        return dict((keyHandler(rec.value(0)), valueHandler(rec.value(1)))
                    for rec in self.iterRecordList(table, [keyColumn, valueColumn], where,
                                                   order=order, limit=limit, isDistinct=isDistinct))

    def getIdList(self, table=None, idCol='id', where='', order='', limit=None, stmt=None, isDistinct=False):
        return list(self.iterIdList(table, idCol, where, order, limit, stmt, isDistinct))

    def iterIdList(self, table=None, idCol='id', where='', order='', limit=None, stmt=None, isDistinct=False):
        if stmt is None:
            stmt = self.selectStmt(table, idCol, where, order=order, limit=limit, isDistinct=isDistinct)
        query = self.query(stmt)
        while query.next():
            yield query.value(0).toInt()[0]

    def getRecordList(self, table=None, cols='*', where='', order='', isDistinct=False, limit=None, rowNumberFieldName=None,
                      group='', having='', stmt=None):
        return list(self.iterRecordList(table, cols, where, order, isDistinct, limit, rowNumberFieldName, group, having, stmt))

    def iterRecordList(self, table=None, cols='*', where='', order='', isDistinct=False, limit=None, rowNumberFieldName=None,
                       group='', having='', stmt=None):
        if stmt is None:
            stmt = self.selectStmt(table, cols, where, group=group, order=order, isDistinct=isDistinct, limit=limit,
                                   rowNumberFieldName=rowNumberFieldName, having=having)
        query = self.query(stmt)
        while query.next():
            yield query.record()

    def getRecordListGroupBy(self, table, cols='*', where='', group='', order=''):
        return self.getRecordList(table, cols=cols, where=where, group=group, order=order)

    def translate(self, table, keyCol, keyVal, valCol, idFieldName='id', order=''):
        if keyCol == 'id' and keyVal is None: return None

        table = self.forceTable(table, idFieldName)
        if not isinstance(keyCol, CField): keyCol = table[keyCol]

        cond = [keyCol.eq(keyVal)]
        if isinstance(table, CTable) and table.hasField('deleted'): cond.append(table['deleted'].eq(0))

        record = self.getRecordEx(table, valCol, cond, order)
        if record:
            return record.value(0)
        else:
            return None


class CMySqlDatabase(CDatabase):
    limit1 = 'LIMIT 0, %d'
    limit2 = 'LIMIT %d, %d'
    CR_SERVER_GONE_ERROR = 2006
    CR_SERVER_LOST = 2013

    returnedDeadlockErrorText          = u'Deadlock found when trying to get lock;'

    def __init__(self, serverName, serverPort, databaseName, userName, password, connectionName=None, compressData=False, afterConnectFunc=None, **kwargs):
        CDatabase.__init__(self, afterConnectFunc)
        self.createConnection('QMYSQL', connectionName, serverName, serverPort, databaseName, userName, password)
        options = []
        if compressData:
            options.append('CLIENT_COMPRESS=1')
        if options:
            self.db.setConnectOptions(';'.join(options))
        self.connectUp()
        self.query('SET NAMES \'utf8\' COLLATE \'utf8_general_ci\';')
        self.query('SET SQL_AUTO_IS_NULL=0;')
        self.query('SET SQL_MODE=\'\';')

        self._func = None
        self._proc = None
        self._schemaTables = None  # type: set[unicode] | None

    NULL = property(lambda self: CSqlExpression(self, 'NULL'))
    func = property(lambda self: self.loadFunctions()._func)
    proc = property(lambda self: self.loadFunctions()._proc)
    schemaTables = property(lambda self: self._loadSchemaTables()._schemaTables)


def connectDataBase(driverName, serverName, serverPort, databaseName, userName, password, connectionName=None, compressData=False, afterConnectFunc=None, **kwargs):
    driverName = unicode(driverName).upper()
    if driverName == 'MYSQL':
        return CMySqlDatabase(serverName, serverPort, databaseName, userName, password, connectionName, compressData=compressData, afterConnectFunc=afterConnectFunc, **kwargs)
    else:
        raise CDatabaseException(CDatabase.errUndefinedDriver % driverName)


def connectDataBaseByInfo(connectionInfo):
    return connectDataBase(connectionInfo['driverName'], connectionInfo['host'], connectionInfo['port'],
                           connectionInfo['database'], connectionInfo['user'], connectionInfo['password'])