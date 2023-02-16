#!/usr/bin/env python3
# vim: ai ts=4 sts=4 et sw=4
#
# pip install --user termcolor
#
# pip install --user mysql-connector-python
# http://dev.mysql.com/doc/connector-python/en/connector-python-connectargs.html


import os, os.path, sys
import time
import logging
import re
import socket
import threading
import getopt
import configparser
import psutil
import mysql.connector as mdb

from pprint import pprint

try:
    import termcolor
except: pass

DEFAULTHOST = '/var/run/mysqld/mysqld.sock'
NAGIOSSTATUSES = {'OK': 0, 'WARNING': 1, 'CRITICAL': 2,'UNKNOWN': 3, 'DEPENDENT': 4};
BACKUP_FLAG = '/tmp/replic_backup_is_running_flag'

# TODO: modify with parameters:
WARNINGTIME = 10
MAXSLAVESWAIT = 300

DRYRUN = False


#
# USAGE
#
def Usage(): # {{{
    print("""Usage: %s [options]

== GENERAL OPTIONS ==
    --debug                 : debug
    --dry-run               : dry-run, don't really do anything
    --host HOST             : hostname. Default: unix socket

== CHECK ==
    --check <w> <c>         : Nagios check with optional args:
                              <w> is the warning threshold in seconds
                              <c> is the critical threshold in seconds

== SWITCH ==
    --switch <slaves>       : switch master to HOST
                              optionnal arg <slaves> is a slaves list separated by comma
                              which are added to the slaves automatically fetched from master.
                              MANDATORY is master is down.
    --confident             : do not warn before to proceed

== EXAMPLES ==

--host srv.local --check    : check <src> from another server
--host new_master --switch  : switch slave(s) and current master to new_master

"""%(os.path.basename(sys.argv[0]),))
    sys.exit(1)
# }}}


def setup_logging(debug_level=None, threadless=False, logfile=None, rotate=False): # {{{
    # if threadless mode, it's a workarround for new Process
    if threadless or rotate:
        try:
            logfile = logging.root.handlers[0].baseFilename

            if rotate:
                try:
                    logging.root.handlers[0].close()
                    # rotate handled by logrotate
                except:
                    print("Unable to close file:")
                    print(sys.exc_value)
        except AttributeError: logfile=None

        # removing them with technic to not need lock :
        # see line 1198 from /usr/lib/python2.6/logging/__init__.py
        while len(logging.root.handlers) > 0:
            logging.root.handlers.remove(logging.root.handlers[0])

        if debug_level is None:
            debug_level = logging.root.getEffectiveLevel()
    else:
        # ensure closed
        logging.shutdown()
        if debug_level is None:
            debug_level = logging.DEBUG

    if logfile:
        loghandler = logging.handlers.WatchedFileHandler(logfile)
    else:
        loghandler = logging.StreamHandler()

    loghandler.setLevel(debug_level)
    #loghandler.setFormatter(logging.Formatter(logformat, logdatefmt))
    use_color = False
    if "TERM" in os.environ and ( re.search("term", os.environ["TERM"]) or os.environ["TERM"] in ('screen',) ):
        use_color = True
    loghandler.setFormatter(ColoredFormatter(use_color))

    while len(logging.root.handlers) > 0:
        logging.root.removeHandler(logging.root.handlers[0])

    logging.root.addHandler(loghandler)
    logging.root.setLevel(debug_level)

# }}}

# }}}

class ColoredFormatter(logging.Formatter): # {{{
    COLORS = {
        'WARNING': 'yellow',
        'INFO': 'cyan',
        'CRITICAL': 'white',
        'ERROR': 'red'
    }
    COLORS_ATTRS = {
        'CRITICAL': 'on_red',
    }

    def __init__(self, use_color = True):
        # main formatter:
        logformat = u'%(asctime)s %(threadName)14s.%(funcName)-15s %(levelname)-8s %(message)s'
        logdatefmt = '%H:%M:%S %d/%m/%Y'
        logging.Formatter.__init__(self, logformat, logdatefmt)
        
        # for thread-less scripts :
        logformat = u'%(asctime)s %(module)14s.%(funcName)-15s %(levelname)-8s %(message)s'
        self.mainthread_formatter = logging.Formatter(logformat, logdatefmt)

        self.use_color = use_color
        if self.use_color and not 'termcolor' in sys.modules:
            logging.debug("You could activate colors with 'termcolor' module")
            self.use_color = False

    def format(self, record):
        if self.use_color and record.levelname in self.COLORS:
            if record.levelname in self.COLORS_ATTRS:
                record.msg = u'%s'%termcolor.colored(record.msg, self.COLORS[record.levelname], self.COLORS_ATTRS[record.levelname])
            else:
                record.msg = u'%s'%termcolor.colored(record.msg, self.COLORS[record.levelname])
        if threading.currentThread().getName() == 'MainThread':
            return self.mainthread_formatter.format(record)
        else:
            return logging.Formatter.format(self, record)

# }}}

class ReplicMaster():
    def __init__(self, binlogfile=None, binlogpos=None):
        self.binlogfile = ''
        self.binlogpos = 0
        self.setMasterPos(binlogfile, binlogpos)
    
    def setMasterPos(self, binlog, pos):
        self.binlogfile = str(binlog)
        self.binlogpos = int(pos)



class ReplicSlave():
    def __init__(self, host):
        self.host = host
        self.sss = None
        self.name = ''
    
    # Last_Error
    # Until_Log_File
    # Gtid_IO_Pos
    # Seconds_Behind_Master
    # Master_User
    # Master_Port
    # Until_Log_Pos
    # Master_Log_File
    # Read_Master_Log_Pos
    # Exec_Master_Log_Pos
    # Master_Server_Id
    # Last_SQL_Error
    # Slave_IO_State
    # Until_Condition
    # Last_Errno
    # Master_Host
    # Slave_SQL_Running
    # Last_IO_Errno
    # Slave_IO_Running
    # Last_SQL_Errno
    # Using_Gtid
    def setStatus(self, sss):
        self.sss = {}
        for k in sss:
            try:
                self.sss[k.decode('UTF-8')] = sss[k]
            except:
                self.sss[k] = sss[k]
        return True

    
    def setName(self, name):
        self.name = name

    
    def getName(self):
        return self.name


    def getPrettyName(self):
        if self.name == '':
            return 'main'
        else:
            return self.name

    
    def getShortStatus(self,):
        if self.sss is None:
            return False
        if self.isRunning():
            pass
    

    def isRunning(self,):
        if self.isIoRunning() and self.isSqlRunning():
            return True
        return False


    def getStatus(self, key):
        return self.sss[key]


    def isIoRunning(self,):
        if self.getStatus('Slave_IO_Running') == 'Yes':
            return True
        return False

    def isSqlRunning(self,):
        if self.getStatus('Slave_SQL_Running') == 'Yes':
            return True
        return False


    def getBackupFlag(self,):
        if self.host is None:
            return BACKUP_FLAG
        else:
            return BACKUP_FLAG + '.' + self.host


    def isBackupRunningForTooLong(self,):
        bflag = self.getBackupFlag()
        if os.path.exists(bflag):
            if time.time() - os.stat(bflag).st_mtime > 7200:
                return True
        return False


    def isBackupRunning(self,):
        logging.debug("Check if a backup is running")
        bflag = self.getBackupFlag()

        for p in psutil.process_iter():
            if p.name in ('mysqldump',):
                logging.debug(p.name)
                logging.debug("Touch backup flag: %s", bflag)
                f = open(bflag, 'a')
                try: # touch
                    os.utime(bflag, (time.time(), time.time()))
                finally:
                    f.close()
                return True

        return False


    def removeBackupFlag(self,):
        bflag = self.getBackupFlag()
        if os.path.exists(bflag):
            logging.debug("remove flag '%s'", bflag)
            os.unlink(bflag)


    def isBackupflagPresent(self,):
        return os.path.exists(self.getBackupFlag())


    def getBehindMaster(self,):
        return self.getStatus('Seconds_Behind_Master')





class ReplicServer():
    DEFAULT_USER = 'root'
    DEFAULT_PWD = ''
    DEFAULT_TIMEOUT = 3
    DEFAULT_SOCKETS = ('/var/run/mysqld/mysqld.sock', '/var/run/mysqld/mariadb.sock')

    def __init__(self, host=None):
        self.host = None
        self.socket = None
        self.user = self.DEFAULT_USER
        self.password = self.DEFAULT_PWD
        self.mdb = None
        self.confident = False
        self.retry = True

        self.master = None
        self.slave = None
        self.slaves = {}

        self.has_multi_source_support = None
        self.gtid_domain = None
        self.server_id = None
        self.connection_name = None
        self.connect_timeout = self.DEFAULT_TIMEOUT

        self.parseMyCnf()
        if host is not None:
            self.setHost(host)
        else:
            self.setHost('localhost')


    def close(self):
        if self.mdb is not None:
            self.mdb.disconnect()
            try:
                self.mdb.shutdown()
            except AttributeError: pass


    def setTimeout(self, t):
        self.connect_timeout = t

    def setConfident(self, flag=False):
        self.confident = bool(flag)


    def parseMyCnf(self,):
        cfp = configparser.ConfigParser()
        cfp.read(os.path.expanduser("~/.my.cnf"))
        for k in ('host', 'user', 'password'):
            try:
                getattr(self, "set%s" % k.capitalize())(cfp.get('mysql', k))
            except (configparser.NoOptionError, configparser.NoSectionError): pass


    def setHost(self, host):
        self.host = host


    def setUser(self, user):
        self.user = user


    def setPassword(self, p):
        self.password = p


    def connect(self,):
        if self.mdb is not None:
            try:
                self.mdb.ping()
                return
            except mdb.InterfaceError:
                logging.debug("Connection to database is closed.")
                try:
                    self.mdb.shutdown()
                except AttributeError: pass
                self.mdb = None

        if self.host == 'localhost':
            for socket in self.DEFAULT_SOCKETS:
                if os.path.exists(socket):
                    self.socket = socket
                    break

        if '/' in self.host:
            self.socket = self.host
            self.host = None
        logging.debug("Connect to %s ..."%self.host)
        self.mdb = mdb.connect(
            host=self.host,
            unix_socket=self.socket,
            user=self.user,
            password=self.password,
            database='mysql',
            connection_timeout=self.connect_timeout,
            autocommit=True
        )
        self.query("SET wait_timeout=3600")


    def query(self, q, cursor=None, on_slave=True):
        if cursor is None:
            cursor = self.mdb.cursor()
        if on_slave and self.has_multi_source_support and self.connection_name is not None:
            logging.debug("%s (%s) : %s", self.host, self.connection_name, q)
            cursor.execute("SET @@default_master_connection='%s'"%self.connection_name)
        else:
            logging.debug("%s : %s", self.host, q)
        cursor.execute(q)
        return cursor

    
    def execQuery(self, q):
        source = False
        if self.has_multi_source_support and self.connection_name is not None:
            source = True
            logging.debug("%s (%s) : %s", self.host, self.connection_name, q)
        else:
            logging.debug("%s : %s", self.host, q)
        if DRYRUN:
            return False
        cursor = self.mdb.cursor()
        if source:
            cursor.execute("SET @@default_master_connection='%s'"%self.connection_name)
        cursor.execute(q)
        cursor.close()
    

    def fetchInfos(self):
        self.getMasterInfos()
        self.getSlaveInfos()
        if self.hasMultiSourceSupport():
            self.getAllSlaves()
    

    def hasMultiSourceSupport(self):
        if self.has_multi_source_support is None:
            self.connect()
            try:
                cursor = self.query("SET @@default_master_connection=''")
                cursor.close()
                self.has_multi_source_support = True
            except mdb.DatabaseError:
                self.has_multi_source_support = False
        return self.has_multi_source_support
        
    def hasMultiSources(self):
        return len(self.slaves) > 1

    def getAllSlaves(self):
        self.connect()
        cursor = self.query("SHOW ALL SLAVES STATUS", self.mdb.cursor(dictionary=True))
        for row in cursor:
            self.slaves[row['Connection_name']] = {}
        
        for slave in sorted(self.slaves.keys(), reverse=True):
            logging.debug("Check slave with connection name '%s'", slave)
            self.query("SET @@default_master_connection='%s'"%slave, on_slave=False).close()
            self.getSlaveInfos()
            self.slaves[slave] = self.slave
            self.slaves[slave].setName(slave)
        return True

    def selectSource(self, with_master_id):
        for slave in self.slaves:
            if self.slaves[slave].getStatus('Master_Server_Id') == with_master_id:
                self.slave = self.slaves[slave]
                self.connection_name = slave
                logging.info("%s selected replication source : '%s'", self.host, slave)
                return True
        return False

    def getMasterInfos(self,):
        self.connect()
        cursor = self.query("SHOW MASTER STATUS")
        for row in cursor:
            if row[0]:
                self.setMaster(row[0], row[1])
        cursor.close()


    def setMaster(self, binlogfile, binlogpos):
        if self.master is None:
            self.master = ReplicMaster(binlogfile, binlogpos)
        else:
            self.master.setMasterPos(binlogfile, binlogpos)

        logging.debug("%s is a master: '%s':%d", self.host, binlogfile, binlogpos)


    def getSlaveInfos(self):
        self.connect()
        cursor = self.query("SHOW SLAVE STATUS", self.mdb.cursor(dictionary=True))
        slave = None
        for row in cursor:
            # if not yet connected, retry in 1s :
            if self.retry and row['Slave_IO_Running'] == 'No':
                cursor.close()
                time.sleep(1)
                self.retry = False
                return self.getSlaveInfos()

            slave = ReplicSlave(self.host)
            if slave.setStatus(row):
                #logging.debug("%s is a slave", self.host)
                pass
            else:
                slave = None
        cursor.close()
        self.slave = slave


    def isMaster(self,):
        return self.master is not None


    def isSlave(self,):
        return self.slave is not None

    
    def getGtidPos(self, domain=None):
        pos = str(self.getVariable('gtid_current_pos'))

        if domain:
            gtids = pos.split(',')
            for gtid in gtids:
                if gtid.startswith("%d-"%domain):
                    return gtid
            return None
        return pos


    def getGtidDomain(self, refresh=False):
        if refresh or self.gtid_domain is None:
            self.gtid_domain = int(self.getVariable('gtid_domain_id'))
        return self.gtid_domain


    def getMasterPos(self,):
        return (self.master.binlogfile, self.master.binlogpos)

    
    def isReadOnly(self,):
        ro = int(self.getVariable('read_only'))
        return ro

    
    def hasGtid(self,):
        if self.isSlave():
            slave_use_gtid = self.slave.getStatus('Using_Gtid')
            return slave_use_gtid.endswith('_Pos')
        elif self.isMaster():
            pos = self.getGtidPos()
            return pos is not None


    def getGrantsFor(self, user):
        grants = []
        users = []

        cursor = self.query("SELECT user,host FROM mysql.user WHERE user = '%s'"%user)
        for row in cursor:
            users.append(row)
        cursor.close()

        for row in users:
            gcursor = self.query("SHOW GRANTS FOR '%s'@'%s'"%(row[0], row[1]))
            for r in gcursor:
                if r:
                    grants.append(r[0])
        return grants


    def getVariable(self, var):
        q = self.query('SELECT @@%s'%var)
        val = None
        for row in q:
            if row[0] is not None:
                val = row[0]
        q.close()
        return val

    
    def getMasterConfig(self,):
        config = {}
        for var in ('gtid_strict_mode', 'binlog_format', 'sync_binlog'):
            config[var] = self.getVariable(var)
        return config

    def getServerId(self):
        if self.server_id is None:
            self.server_id = self.getVariable('server_id')
        return self.server_id

    def getPositionToCatch(self, gtid_domain):
        self.getMasterInfos()
        gtid_position_to_catch = self.getGtidPos(gtid_domain)
        position_to_catch = self.getMasterPos()
        return (gtid_position_to_catch, position_to_catch)

    def waitReplication(self, gtid_domain, master_pos):
        catched_up = False
        self.getSlaveInfos()
        if self.hasGtid():
            gtid = self.getGtidPos(gtid_domain)
            if gtid == master_pos[0]:
                catched_up = True
        else:
            binlog = self.slave.getStatus('Master_Log_File')
            exec_pos = self.slave.getStatus('Exec_Master_Log_Pos')
            if (binlog, exec_pos) == master_pos[1]:
                catched_up = True
            
        if catched_up:
            logging.info("Slave '%s' catched-up.", self.host)
        return catched_up

    def setConfig(self, conf):
        for var in conf.keys():
            self.execQuery("SET GLOBAL %s = %s"%(var, conf[var]))

# {{{
    def nagiosCheck(self, args):
        nagios_status = NAGIOSSTATUSES['UNKNOWN']
        nagios_msg = 'Gni?'
        warning_threshold = 30
        critical_threshold = 120
        if len(args) == 1:
            critical_threshold = int(args[0])
        elif len(args) == 2:
            warning_threshold = int(args[0])
            critical_threshold = int(args[1])

        if warning_threshold > critical_threshold:
            logging.critical("Warning threshold is greater than critical, are you crazy??")

        try:
            self.retry = False
            self.setTimeout(1)
            self.fetchInfos()

            # neither a master nor a slave
            if not self.isMaster() and not self.isSlave():
                nagios_status = NAGIOSSTATUSES['UNKNOWN']
                nagios_msg = "standalone"

            elif self.isMaster():
                nagios_status = NAGIOSSTATUSES['OK']
                nagios_msg = "MASTER"

            if self.isSlave():
                nagios_msg = "REPLICA"
                nagios_status = NAGIOSSTATUSES['OK']
                if self.hasMultiSourceSupport():
                    self.getAllSlaves()
                    if len(self.slaves) > 1:
                        nagios_msg = "MSR"
                else:
                    self.slaves[''] = self.slave

                slave_nagios_set = False
                for slave in self.slaves.values():
                    slave_nagios_status = nagios_status
                    slave_nagios_msg = nagios_msg
                    # both SQL and IO are running :
                    if slave.isIoRunning() and slave.isSqlRunning():
                        sbm = slave.getBehindMaster()
                        logging.debug("'%s' Seconds_Behind_Master=%s Warning=%d Critical=%d", slave.getPrettyName(), sbm, warning_threshold, critical_threshold)
                        if sbm < warning_threshold:
                            slave.removeBackupFlag()
                            slave_nagios_status = NAGIOSSTATUSES['OK']
                            nagios_msg += ", %s=%ds" % (slave.getPrettyName(), sbm)
                        elif sbm < critical_threshold:
                            slave_nagios_status = NAGIOSSTATUSES['WARNING']
                            slave_nagios_msg += ", %s=%ds" % (slave.getPrettyName(), sbm)
                        elif sbm >= critical_threshold:
                            if slave.isBackupflagPresent():
                                slave_nagios_status = NAGIOSSTATUSES['WARNING']
                                slave_nagios_msg = 'Backup is catching-up'
                            else:
                                slave_nagios_status = NAGIOSSTATUSES['CRITICAL']
                                slave_nagios_msg += ", %s=%ds" % (slave.getPrettyName(), sbm)

                    elif slave.isBackupRunningForTooLong():
                        slave_nagios_status = NAGIOSSTATUSES['CRITICAL']
                        slave_nagios_msg += 'Backup is stuck'

                    elif slave.isBackupRunning():
                        slave_nagios_status = NAGIOSSTATUSES['UNKNOWN']
                        slave_nagios_msg += 'Backup in progress'

                    elif not slave.isIoRunning() and slave.isSqlRunning():
                        slave_nagios_status = NAGIOSSTATUSES['WARNING']
                        slave_nagios_msg += 'I/O slave stopped'

                    elif slave.isIoRunning() and not slave.isSqlRunning():
                        if slave.getStatus('Last_Errno'):
                            slave_nagios_status = NAGIOSSTATUSES['CRITICAL']
                            slave_nagios_msg = "[%s] %s" % (slave.getStatus('Last_Errno'), slave.getStatus('Last_Error'))
                        else:
                            slave_nagios_status = NAGIOSSTATUSES['WARNING']
                        slave_nagios_msg += 'SQL slave stopped'
                            

                    else:
                        nagios_status = NAGIOSSTATUSES['CRITICAL']
                        slave_nagios_msg = "[%s] %s" % (slave.getStatus('Last_SQL_Errno'), slave.getStatus('Last_SQL_Error'))
                        if self.hasMultiSourceSupport():
                            if slave.getStatus('Last_SQL_Errno') == 0:
                                nagios_msg += ", %s: stopped"%(slave.getPrettyName(), )
                            else:
                                nagios_msg += ", %s: %s"%(slave.getPrettyName(), slave.getStatus('Last_SQL_Errno'))
                    
                    if not slave_nagios_set and slave_nagios_status > nagios_status:
                        nagios_status = slave_nagios_status
                        nagios_msg = slave_nagios_msg
                        slave_nagios_set = True

        except (mdb.InterfaceError, mdb.errors.DatabaseError):
            # Check if this is a DRBD secondary node
            nagios_status = NAGIOSSTATUSES['CRITICAL']
            nagios_msg = 'Connection refused'
            try:
                drbd = open('/proc/drbd', 'r')
                for line in drbd:
                    if 'ro:Secondary/' in line:
                        nagios_status = NAGIOSSTATUSES['OK']
                        nagios_msg = 'DRBD secondary node'
            except IOError: pass
        except:
            logging.critical("Unknown error", exc_info=True)
            nagios_status = NAGIOSSTATUSES['CRITICAL']
            nagios_msg = 'Unknown error'
            
                
        # output for nagios check:
        print(nagios_msg)
        self.close()
        return nagios_status
# }}}        

    
    def fetchSlaves(self,):
        self.connect()
        cursor = self.query("SHOW PROCESSLIST", self.mdb.cursor(dictionary=True))
        slaves = []
        for row in cursor:
            #Command: Binlog Dump
            if row['Command'].startswith('Binlog Dump'):
                (slave_host, slave_port) = row['Host'].split(':')
                # reverse lookup IP => hostname
                slave_host = socket.gethostbyaddr(slave_host)[0]
                slaves.append(ReplicServer(slave_host))
        cursor.close()
        return slaves



def do_check(rs, args):
    if rs.host is None:
        rs.setHost(DEFAULTHOST)
    return rs.nagiosCheck(args)


def do_switch(newmaster, args):
    newmaster.setTimeout(30)
    newmaster.fetchInfos()
    if not newmaster.isSlave():
        logging.critical("'%s' is not a slave, can't switch.", newmaster.host)
        return 4

    repl_grants = []
    gtid_domain = 0
    master_config = {}
    slaves = []
    if args:
        slaves = map(ReplicServer, args[0].split(','))

    # OK, search his master
    current_master = ReplicServer(newmaster.slave.getStatus('Master_Host'))

# DEBUG: Fake a bad master:
#    current_master = ReplicServer("10.0.4.56")

    current_master.fetchInfos()
    if current_master.isMaster():
        # search for slaves
        # (only if master is online)
        slaves.extend(current_master.fetchSlaves())
        gtid_domain = current_master.getGtidDomain()
        master_config = current_master.getMasterConfig()
        if current_master.slave is not None \
            and current_master.slave.isRunning() \
            and current_master.slave.getStatus('Master_Host') == newmaster.host:
            logging.critical("%s is already master for %s ...", newmaster.host, current_master.host)
            return 8
    else:
        # TODO: switch when master is DOWN
        logging.critical("Failed to get master informations, is it down ?")
        gtid_domain = newmaster.getGtidDomain()

    # fetch infos and filter
    remove_list = []
    slaves_with_vip = []
    master_vip = None
    for slave in slaves:
        try:
            slave.fetchInfos()
        except mdb.InterfaceError:
            logging.warning("Slave %s is down, removing it.", slave.host)
            remove_list.append(slave)
            continue

        if slave.hasMultiSources():
            slave.selectSource(current_master.getServerId())

        try:
            slave_master = slave.slave.getStatus('Master_Host')
        except AttributeError:
            logging.warning("%s is not a slave", slave.host, exc_info=True)
            remove_list.append(slave)
            continue

        # Exclude slaves that are not in the pool
        if slave_master != current_master.host:
            if 'vip' in slave_master:
                if master_vip is None:
                    master_vip = slave_master
                if master_vip == slave_master:
                    if slave.hasMultiSources():
                        logging.info("%s is a MSR slave from VIP %s", slave.host, slave_master)
                    else:
                        logging.info("%s is a slave from VIP %s", slave.host, slave_master)
                    slaves_with_vip.append(slave)
                else:
                    logging.warning("%s is not a slave from %s but from %s", slave.host, current_master.host, slave_master)
                    remove_list.append(slave)
            elif master_vip != slave_master:
                logging.warning("%s is not a slave from %s but from %s", slave.host, current_master.host, slave_master)
                remove_list.append(slave)
            continue
        else:
            logging.info("%s is a slave from %s", slave.host, slave_master)

        if newmaster.host == slave.host:
            logging.debug("Found myself, removing myself from the slaves list")
            remove_list.append(slave)
            continue


    for r in remove_list:
        slaves.remove(r)
    
    master_user = newmaster.slave.getStatus('Master_User')
    for slave in slaves:
        slave_binlog = slave.slave.getStatus('Master_Log_File')
        slave_pos = slave.slave.getStatus('Exec_Master_Log_Pos')
        slave_use_gtid = slave.slave.getStatus('Using_Gtid')
        slave_gtid_pos = slave.slave.getStatus('Gtid_IO_Pos')

        slave_repl_user = slave.slave.getStatus('Master_User')
        if master_user is not None and slave_repl_user != master_user:
            logging.warning("Replication user change '%s' to '%s'. Not handled!", master_user, slave_repl_user)
        #else:
        #    master_user = slave_repl_user
            

        logging.debug("'%s' is at '%s':%d GTID=%s Gtid_Pos=%s"%(slave.host, slave_binlog, slave_pos, slave_use_gtid, slave_gtid_pos))
        if slave.hasGtid():
            if slave.getGtidDomain() != gtid_domain:
                logging.warning("Slave '%s' hasn't the same GtidDomain (%d) than the master (%d)", slave.host, slave.getGtidDomain(), gtid_domain)
        else:
            logging.warning("%s has not GTID", slave.host)

        if not slave.isReadOnly():
            logging.warning("%s is NOT read-only!!", slave.host)
            # TODO: In this case, this server is probably the spare master.
        
    # Try to generate GRANTS from current_master
    if current_master.isMaster():
        repl_grants = current_master.getGrantsFor(master_user)
    else:
        logging.warning("Generates replication GRANTS ...")
        for slave in slaves:
            repl_grants.append("GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '%s'@'%s'"%(master_user, slave.host))


    # print summary
    logging.info("Switch master from '%s' to '%s' with GtidDomain=%d", current_master.host, newmaster.host, gtid_domain)
    if slaves:
        logging.info("With those slaves: %s" % ",".join([x.host for x in slaves]))
    else:
        logging.info("Without any other slaves.")
    logging.info("Replication GRANT (2 firsts only) :")
    for g in repl_grants[:2]:
        logging.info(g)
    repl_grants.append("FLUSH PRIVILEGES")

    # Last check: verify new master
    if not newmaster.isMaster():
        logging.critical("New master '%s' must have log-bin activated."%newmaster.host)
        if not DRYRUN:
            return 6

    # 0/ update APP with new config

    # WARNING
    if not newmaster.confident:
        logging.warning("Will proceed in %d seconds, CTRL+C to abort ...", WARNINGTIME)
        for i in range(WARNINGTIME):
            print("\r%ds ..." % (WARNINGTIME - i - 1),)
            sys.stdout.flush()
            time.sleep(1)
        print()


    phase = 1
    logging.info("Phase %d : configure new master (%s)", phase, newmaster.host)
    newmaster.execQuery("SET GLOBAL read_only=OFF")
    if newmaster.hasGtid() and newmaster.getGtidDomain() != gtid_domain:
        newmaster.execQuery("SET GLOBAL gtid_domain_id=%d" % gtid_domain)
    for g in repl_grants:
        newmaster.execQuery(g)

    if current_master.isMaster():
        phase += 1
        logging.info("Phase %d : set current master read-only", phase)
        current_master.execQuery("SET GLOBAL read_only=ON")

    # if something goes wrong, it's the last step to revert:
    # - set old master writable

    phase += 1
    logging.info("Phase %d : wait for slaves to catch-up replication delay ...", phase)
    gtid_position_to_catch = None
    position_to_catch = None
    slaves_to_wait = list(slaves) # copy
    for i in range(MAXSLAVESWAIT*5):
        if current_master.isMaster():
            current_master.getMasterInfos()
            tmp_gtid = current_master.getGtidPos(gtid_domain)
            tmp_pos = current_master.getMasterPos()

            if gtid_position_to_catch is not None and position_to_catch != tmp_pos:
                if not DRYRUN:
                    logging.critical("Position has changed on master! Something is wrong!")
                    pass # what to do in this case??
                else:
                    pass

            gtid_position_to_catch = tmp_gtid
            position_to_catch = tmp_pos
            logging.debug("Position to catch-up: %s:%d GTID=%s" % (position_to_catch[0], position_to_catch[1], gtid_position_to_catch))

        slaves_ok = []
        master_pos = current_master.getPositionToCatch(gtid_domain)
        newmaster.waitReplication(gtid_domain, master_pos)
        for slave in slaves_to_wait:
            if slave.waitReplication(gtid_domain, master_pos):
                slaves_ok.append(slave)
        for slave in slaves_ok:
            slaves_to_wait.remove(slave)

        if len(slaves_to_wait) == 0:
            logging.info("All slaves catched-up.")
            break
        time.sleep(0.2)
    
    phase += 1
    logging.info("Phase %d : get new master binlog position...", phase)
    newmaster.execQuery("STOP SLAVE")
    newmaster.getMasterInfos()
    newmaster_gtid = newmaster.getGtidPos(gtid_domain)
    newmaster_pos = newmaster.getMasterPos()

    phase += 1
    logging.info("Phase %d : configure slaves with new master...", phase)
    for slave in slaves:
        slave.execQuery("STOP SLAVE")
        # Switch back to "old" fashion replication: no VIP, no GTID
        slave.execQuery("CHANGE MASTER TO MASTER_HOST='%s', MASTER_USER='%s', MASTER_LOG_FILE='%s', MASTER_LOG_POS=%d"%(newmaster.host, master_user, newmaster_pos[0], newmaster_pos[1]))
        slave.execQuery("START SLAVE")
    
    # check replication status
    time.sleep(1) # let connection beeing established...
    for slave in slaves:
        slave.getSlaveInfos()
        if slave.slave.isRunning() and slave.slave.getStatus('Master_Host') == newmaster.host:
            logging.info("Slave '%s' has switched successfully! replication delay: %d", slave.host, slave.slave.getBehindMaster())
        elif not slave.slave.isIoRunning():
            logging.critical("Switch of slave '%s' failed", slave.host)
        elif not slave.slave.isSqlRunning():
            logging.critical("Switch of slave '%s' failed: check SQL thread", slave.host)
            logging.critical("%s: %s", slave.host, slave.slave.getStatus("Last_Error"))
        elif DRYRUN:
            logging.debug("DRYRUN slave '%s' has not switched", slave.host)
        else:
            logging.debug("wtf???") # TODO: handle that case...


    phase += 1
    logging.info("Phase %d : transform old master to a slave...", phase)
    current_master.execQuery("STOP SLAVE")
    current_master.execQuery("CHANGE MASTER TO MASTER_HOST='%s', MASTER_USER='%s', MASTER_LOG_FILE='%s', MASTER_LOG_POS=%d"%(newmaster.host, master_user, newmaster_pos[0], newmaster_pos[1]))
    current_master.execQuery("START SLAVE")
    time.sleep(2)
    current_master.getSlaveInfos()
    if current_master.isSlave():
        if current_master.slave.isRunning(): # and newmaster.hasGtid():
            try:
                logging.info("Switch old master to GTID current_pos")
                current_master.execQuery("STOP SLAVE")
                current_master.execQuery("CHANGE MASTER TO MASTER_USE_GTID=current_pos")
                time.sleep(2)
                if current_master.slave.isRunning():
                    current_master.execQuery("STOP SLAVE")
                    current_master.getSlaveInfos()
                    if current_master.slave.isRunning():
                        current_master.execQuery("CHANGE MASTER TO MASTER_USE_GTID=slave_pos")
            finally:
                current_master.execQuery("START SLAVE")
            current_master.getSlaveInfos()
            #current_master.execQuery("RESET MASTER") # ??? only if everything is ok ...
        logging.debug("Disable sync_binlog on old master")
        current_master.execQuery("SET GLOBAL sync_binlog=0")

    else:
        if not DRYRUN:
            logging.critical("Something is wrong with old master (%s)", current_master.host)
        else:
            logging.warning("DRYRUN: not switched.")


    phase += 1
    logging.info("Phase %d : set master config from old master.", phase)
    newmaster.setConfig(master_config)
    
    phase += 1
    logging.info("Phase %d : reset slave on new master.", phase)
    newmaster.execQuery("RESET SLAVE ALL")

    phase += 1
    logging.info("Phase %d : switching to GTID ...", phase)
    for slave in slaves:
        slave.execQuery("STOP SLAVE")
        slave.execQuery("CHANGE MASTER TO master_use_gtid=current_pos")
        slave.execQuery("START SLAVE")
    master_pos = newmaster.getPositionToCatch(gtid_domain)
    for slave in slaves:
        slave.waitReplication(gtid_domain, master_pos)

    if master_vip is not None:
        logging.info("Master is going to be RESET, slaves will switch to VIP (if available)")
        logging.info("Please check that everything is OK then :")
        input("Press Enter to continue...")

        phase += 1
        logging.info("Phase %d : switching to VIP ...", phase)
        for slave in slaves:
            slave.execQuery("STOP SLAVE")
            slave.execQuery("CHANGE MASTER TO MASTER_HOST='%s'"%master_vip)
            slave.execQuery("START SLAVE")
        master_pos = newmaster.getPositionToCatch(gtid_domain)
        for slave in slaves:
            slave.waitReplication(gtid_domain, master_pos)

    phase += 1
    logging.info("Phase %d : switching to GTID Slave_Pos...", phase)
    for slave in slaves:
        slave.execQuery("STOP SLAVE")
        slave.execQuery("CHANGE MASTER TO master_use_gtid=slave_pos")
        slave.execQuery("START SLAVE")
    master_pos = newmaster.getPositionToCatch(gtid_domain)
    for slave in slaves:
        slave.waitReplication(gtid_domain, master_pos)

    for slave in slaves:
        slave.getSlaveInfos()
        if slave.slave.isRunning():
            logging.info("Slave '%s' status: master is %s, replication delay: %d", slave.host, slave.slave.getStatus('Master_Host'), slave.slave.getBehindMaster())

    logging.warning("If everything is OK, exec this query on old master %s : 'RESET MASTER'", current_master.host)

    for srv in slaves + [current_master, newmaster]:
        srv.close()

    return 0



#
# MAIN
#
if __name__ == '__main__':
    exitcode = 1
    setup_logging()
    logging.root.setLevel(logging.INFO)
    actions = []
    replic_server = ReplicServer()
    
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hdH:u:p:", 
            [
            "help",
            "debug",
            "confident",
            "dry-run",
            "host=",
            "user=",
            "passwd=",
            "check",
            "switch",
            ]
        )
    except getopt.GetoptError:
           # print help information and exit:
        Usage()

    for o, a in opts:
        if o in ("--help", "-h"):
            Usage()
        elif o in ("--debug","-d"):
            logging.root.setLevel(logging.DEBUG)
        elif o in ("--dry-run",):
            DRYRUN = True
            WARNINGTIME = 3
        elif o in ("--confident",):
            DRYRUN = False
            replic_server.setConfident(True)
        elif o in ("--host", "-H"):
            replic_server.setHost(a)
        elif o in ("--user", "-u"):
            replic_server.setUser(a)
        elif o in ("--passwd", "-p"):
            replic_server.setPassword(a)
        elif o in ("--check",):
            actions.append("check")
        elif o in ("--switch",):
            actions.append("switch")
    
    try:
        exitcode = 0
        for action in actions:
            # dynamic function call
            logging.debug("Call %s", action)
            exitcode += getattr(sys.modules[__name__], "do_%s" % action)(replic_server, args)
    except KeyboardInterrupt:
        logging.info("Cancelled.")
        exitcode = 2

    except SystemExit: pass # sys.exit()
    
    except:
        logging.critical("Fatal error :", exc_info=True)
        exitcode = 3
    
    replic_server.close()
    sys.exit(exitcode)





