mysql_slow_log_process
======================

mysql slow log collection and process

client:
  collect and rotate slow log query 
  send them to server 
  
server:
  receive the slow log and parse them then  put them into MYSQL tables
  
