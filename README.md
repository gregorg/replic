# replic

MariaDB replication checks and master switch

# Dependencies

You must install 2 pip modules:
```
pip install --user termcolor
pip install --user mysql-connector-python
```

# check mode

Used for Nagios checks. It will check both master and slave.

```
replic.py --check
```

You can check remotely:

```
replic.py --host server_name --check
```

# switch mode

Used to switch from a MariaDB master to another. If old master is still reachable, it will transform it to a slave.

- first it will do some sanity checks, like that the new master has binary logs activated
- then, if old master is still alive, it will guess slaves. If not, you have to specify slaves on command line:
```
replic.py --host new_master --switch slave1,slave2,slave3
```
- if everything is fine, it will proceed to the switch
- disable writes on old master
- enable writes on new master, apply replication grants, 
- wait for slaves to catch-up there replication delays from old master
- configure slaves with the new master, with MariaDB's GTIDs is available
- configure old master to be a slave from new master
- copy some master config to the new master
- reset old master

You still have some works to do if switch is successfull:
- apply writes grants
- change configuration for your app

