#!/bin/bash
# -*- coding: utf-8 -*-
#***************************************************************#
# ScriptName: get_thunder_log.sh
# Author: craazw@gmail.com
# Create Date: 2016-02-18 14:12
# Function:
#***************************************************************#
ip=`cat /home/fzhibo/fzhibo.conf | grep fzhibo_ip | awk -F '"' '{print $4}'`
daytime=`date '+%Y%m%d%H'`
daytime=$[${daytime}-1]
log_file="liuliang.log.$daytime"
log_api="http://$ip:8889/$log_file"
wget $log_api
#sed  -i 's/^A/   /g' $log_file
#sed  -i 's/^B/   /g' $log_file
#cat  $log_file > /home/crazw/log2mongo/thunder_logs/thunder.log && rm -f $log_file
cat $log_file | logger -p local0.debug  && rm -f $log_file