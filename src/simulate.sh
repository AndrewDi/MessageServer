#!/bin/sh
count=$1
(sleep 1;i=1;while [[ $i -le $count ]]
do
  echo "SNAPDB,2016-12-30-17.03.20.571800,DBMDB,/home/db2inst1/db2inst1/NODE0000/SQL00001/MEMBER0000/,DBMDB,ACTIVE,0,db,LINUXX8664,LOCAL,2016-12-19-17.26.14.555128,,2016-12-12-17.40.14.000000,621,99853,14,11,1,20,621,621,2,15104534,0,179328,0,0,0,0,0,0,0,4,409,0,139176,0,84795,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,49561,2033590327,69,1017510395,2033606233,361,641356,1523889238,12694,1,1,0,0,179318,99869,0,507908309,507925843,508406934,51784578,5503183405,5,1008507355,34948645,0,216855384,0,1,0,149000,158015530,179025,61892000,140266068,1,91090871,0,0,,34948539,34948539,1017545731,49800,0,34292397,3051999710,702827,2973211,7334,2,1508568,,,,,,,,,7745,0,0,0,0,0,1,0,,0,0,0,0,0,0,0,0,0,0,0";
  let i=$i+1;
done)|telnet 0 11111
