#!/usr/bin/env python
# encoding: utf-8

"""
@     file: iostat2fclog.py
@     time: 3/14/16 11:01 AM
@   author: crazw
@     site: www.crazw.com
@  contact: craazw@gmail.com
@ function:
         0. 运行: nohup iostat -x -k -t 300 > /tmp/iostat.log&
         1. 添加开机启动项, 后台执行: nohup iostat -x -k -t 300 > /tmp/iostat.log&
         2. 添加cron任务, 每天00:03分时清理临时日志: 
            3 0 * * * >/tmp/iostat.log
            */5 * * * * python /home/crazw/scripts/iostat2fclog.py

"""
import commands


def get_disk_num():
    iostat_num_line = int(commands.getoutput('iostat -x -k -t | wc -l')) - 2

    return int(iostat_num_line)


def get_iostat(iostat_num_line):
    commands.getoutput('tail -n ' + str(iostat_num_line) + ' /tmp/iostat.log | grep sd > /tmp/tmp_iostat.log')


def read_log():
    with open('/tmp/tmp_iostat.log', 'r') as fp:
        for item in fp.readlines():

            # 磁盘
            sd_list = [x for x in item.strip('\n').split(' ') if x != '']

            line = {'disk': sd_list[0], 'r_s': sd_list[3], 'w_s': sd_list[4], 'rkB_s': sd_list[5], 'wkB_s': sd_list[6], 'avgrq_sz': sd_list[7], 'avgqu_sz': sd_list[8], 'await': sd_list[9], 'svctm': sd_list[10], 'util': sd_list[11]}
            commands.getoutput('echo ' + str(line)[1:-1] + '| logger -p local0.debug')


def main():
    iostat_num_line = get_disk_num()

    get_iostat(iostat_num_line)

    read_log()


if __name__ == '__main__':
    main()
