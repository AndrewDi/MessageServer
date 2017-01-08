#!/usr/bin/env bash
if [ -f pid ];then
   count=$(ps -fhp $(cat pid)|tail -1|grep -w "MessageServer.jar"|wc -l)
   if [ $count -ge 1 ];then
      echo "[Error] MessageServer Has been start,plase check it"
      exit -1;
   fi
fi
nohup java -Xmx1024m -Xms512m -Dcom.sun.management.jmxremote.port=1099 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -XX:+UseParNewGC -XX:+UseFastAccessorMethods -XX:+CMSParallelRemarkEnabled -jar MessageServer.jar &
echo $! > pid