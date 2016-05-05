#!/bin/bash
# -*- coding: utf-8 -*-
#***************************************************************#
# ScriptName: iostat_install.sh
# Author: craazw@gmail.com
# Create Date: 2016-03-29 15:54
# Function: 飞享CDS磁盘监控部署脚本
#***************************************************************#

# 运行iostat命令
nohup iostat -x -k -t 300 > /tmp/iostat.log&

# 添加开机启动项
echo "nohup iostat -x -k -t 300 > /tmp/iostat.log&" >> /etc/rc.local

# 下载脚本
mkdir -p /home/crazw/scripts/
wget http://d.crazw.com/fxdata/iostat2fclog.py -O /home/crazw/scripts/iostat2fclog.py

# 添加cron任务
crontab -l > mycron
echo "3 0 * * * >/tmp/iostat.log" >> mycron
echo "*/5 * * * * python /home/crazw/scripts/iostat2fclog.py" >> mycron
crontab mycron
rm mycron
