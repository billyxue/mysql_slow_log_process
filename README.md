mysql_slow_log_process
======================

mysql slow log collection and process


dispath_db_slow.sh
        for client steup

server.pl
        recvive slow log from client

collect_slow_client.pl
slow.conf
        collect local instance slow log  & config file


parse_slow_to_db.sh
        parse collected slow log and add to table : db_slow.t_slow_log 

process_slow
        real parse script , modified from mysqldumpslow (in mysql source)

run automatic every day
30 9 * * * cd /home/mysql/billy/temp && bash parse_slow_to_db.sh  >> /home/mysql/billy/temp/parse.log 2>&1


#################### ####################
setup a mysql instance to collect:
#################### ####################

1. add a record in DB_DBM.t_slow_config
        ( slow_collect_flag 1 => collect   0 => don't collect)
        eg: INSERT INTO t_slow_config values (default, '192.168.31.68',3306,'/var/run/mysqld/mysqld.sock','backup','backup@pukcab',1
,'192.168.12.160', 6677, 1); 

2. dispath stricpts:
        bash dispath_db_slow.sh

##########################################
数据库慢查询分析系统，功能概述：
1、	每天定时收集相关数据库的慢查询日志处理后存储到mysql中
2、	各项目组成员可通常WEB查询本项目相关库的慢查询日志
3、	DBA会给出影响相对较大的SQL的优化建议


