#!/usr/bin/env python
# encoding: utf-8

"""
@     file: iostat2fclog.py
@     time: 3/14/16 11:01 AM
@   author: crazw
@     site: www.crazw.com
@  contact: craazw@gmail.com
@ function:
         1. 添加开机启动项, 后台执行: nohup iostat -x -k -t 300 > /tmp/iostat.log&
         2. 添加cron任务, 每天00:03分时清理临时日志: 3 0 * * * >/tmp/iostat.log
         3. 添加cron任务,每5分钟执行: */5 * * * * python /home/crazw/scripts/iostat2fclog.py

"""
import os
# import time


def get_iostat():
    os.system('tail -n 12 /tmp/iostat.log>/tmp/tmp_iostat.log')


def get_item(fobj):
    item = ['', '', '', '', '', '', '', '', '', '', '', '']
    for no, line in enumerate(fobj):
        slot = no % 12
        item[slot] = line.rstrip()
        if slot == 11:
            yield item


def read_log():
    with open('/tmp/tmp_iostat.log', 'r') as fp:
        for item in get_item(fp):
            item = sorted(item)

            # 采集时间
            # pm_time = time.strptime(item[3], "%m/%d/%Y %I:%M:%S %p")
            # make_time = time.strftime('%Y-%m-%d %H:%M:%S', pm_time)

            # cpu负载情况
            avg_cpu_list = [x for x in item[2].split(' ') if x != '']
            avg_cpu_user = avg_cpu_list[0]
            avg_cpu_nice = avg_cpu_list[1]
            avg_cpu_system = avg_cpu_list[2]
            avg_cpu_iowait = avg_cpu_list[4]
            avg_cpu_idle = avg_cpu_list[5]

            # 磁盘
            sda_list = [x for x in item[6].split(' ') if x != '']
            sda_r_s = sda_list[3]
            sda_w_s = sda_list[4]
            sda_rkB_s = sda_list[5]
            sda_wkB_s = sda_list[6]
            sda_avgrq_sz = sda_list[7]
            sda_avgqu_sz = sda_list[8]
            sda_await = sda_list[9]
            sda_svctm = sda_list[10]
            sda_util = sda_list[11]

            sdb_list = [x for x in item[7].split(' ') if x != '']
            sdb_r_s = sdb_list[3]
            sdb_w_s = sdb_list[4]
            sdb_rkB_s = sdb_list[5]
            sdb_wkB_s = sdb_list[6]
            sdb_avgrq_sz = sdb_list[7]
            sdb_avgqu_sz = sdb_list[8]
            sdb_await = sdb_list[9]
            sdb_svctm = sdb_list[10]
            sdb_util = sdb_list[11]

            sdc_list = [x for x in item[8].split(' ') if x != '']
            sdc_r_s = sdc_list[3]
            sdc_w_s = sdc_list[4]
            sdc_rkB_s = sdc_list[5]
            sdc_wkB_s = sdc_list[6]
            sdc_avgrq_sz = sdc_list[7]
            sdc_avgqu_sz = sdc_list[8]
            sdc_await = sdc_list[9]
            sdc_svctm = sdc_list[10]
            sdc_util = sdc_list[11]

            sdd_list = [x for x in item[9].split(' ') if x != '']
            sdd_r_s = sdd_list[3]
            sdd_w_s = sdd_list[4]
            sdd_rkB_s = sdd_list[5]
            sdd_wkB_s = sdd_list[6]
            sdd_avgrq_sz = sdd_list[7]
            sdd_avgqu_sz = sdd_list[8]
            sdd_await = sdd_list[9]
            sdd_svctm = sdd_list[10]
            sdd_util = sdd_list[11]

            sde_list = [x for x in item[10].split(' ') if x != '']
            sde_r_s = sde_list[3]
            sde_w_s = sde_list[4]
            sde_rkB_s = sde_list[5]
            sde_wkB_s = sde_list[6]
            sde_avgrq_sz = sde_list[7]
            sde_avgqu_sz = sde_list[8]
            sde_await = sde_list[9]
            sde_svctm = sde_list[10]
            sde_util = sde_list[11]

            # sdf_list = [x for x in item[11].split(' ') if x != '']
            # sdf_r_s = sdf_list[3]
            # sdf_w_s = sdf_list[4]
            # sdf_rkB_s = sdf_list[5]
            # sdf_wkB_s = sdf_list[6]
            # sdf_avgrq_sz = sdf_list[7]
            # sdf_avgqu_sz = sdf_list[8]
            # sdf_await = sdf_list[9]
            # sdf_svctm = sdf_list[10]
            # sdf_util = sdf_list[11]


            # line = {'avg_cpu_user': avg_cpu_user, 'avg_cpu_nice': avg_cpu_nice, 'avg_cpu_system': avg_cpu_system, 'avg_cpu_iowait':avg_cpu_iowait, 'avg_cpu_idle':avg_cpu_idle, 'sdb_r_s': sdb_r_s, 'sdb_w_s': sdb_w_s, 'sdb_rkB_s': sdb_rkB_s, 'sdb_wkB_s': sdb_wkB_s, 'sdb_avgrq_sz': sdb_avgrq_sz, 'sdb_avgqu_sz': sdb_avgqu_sz, 'sdb_await': sdb_await, 'sdb_svctm': sdb_svctm, 'sdb_util': sdb_util, 'sdc_r_s': sdc_r_s, 'sdc_w_s': sdc_w_s, 'sdc_rkB_s': sdc_rkB_s, 'sdc_wkB_s': sdc_wkB_s, 'sdc_avgrq_sz': sdc_avgrq_sz, 'sdc_avgqu_sz': sdc_avgqu_sz, 'sdc_await': sdc_await, 'sdc_svctm': sdc_svctm, 'sdc_util': sdc_util, 'sdd_r_s': sdd_r_s, 'sdd_w_s': sdd_w_s, 'sdd_rkB_s': sdd_rkB_s, 'sdd_wkB_s': sdd_wkB_s, 'sdd_avgrq_sz': sdd_avgrq_sz, 'sdd_avgqu_sz': sdd_avgqu_sz, 'sdd_await': sdd_await, 'sdd_svctm': sdd_svctm, 'sdd_util': sdd_util, 'sde_r_s': sde_r_s, 'sde_w_s': sde_w_s, 'sde_rkB_s': sde_rkB_s, 'sde_wkB_s': sde_wkB_s, 'sde_avgrq_sz': sde_avgrq_sz, 'sde_avgqu_sz': sde_avgqu_sz, 'sde_await': sde_await, 'sde_svctm': sde_svctm, 'sde_util': sde_util, 'sda_r_s': sda_r_s, 'sda_w_s': sda_w_s, 'sda_rkB_s': sda_rkB_s, 'sda_wkB_s': sda_wkB_s, 'sda_avgrq_sz': sda_avgrq_sz, 'sda_avgqu_sz': sda_avgqu_sz, 'sda_await': sda_await, 'sda_svctm': sda_svctm, 'sda_util': sda_util, 'sdf_r_s': sdf_r_s, 'sdf_w_s': sdf_w_s, 'sdf_rkB_s': sdf_rkB_s, 'sdf_wkB_s': sdf_wkB_s, 'sdf_avgrq_sz': sdf_avgrq_sz, 'sdf_avgqu_sz': sdf_avgqu_sz, 'sdf_await': sdf_await, 'sdf_svctm': sdf_svctm, 'sdf_util': sdf_util}
            line = {'avg_cpu_user': avg_cpu_user, 'avg_cpu_nice': avg_cpu_nice, 'avg_cpu_system': avg_cpu_system, 'avg_cpu_iowait':avg_cpu_iowait, 'avg_cpu_idle':avg_cpu_idle, 'sdb_r_s': sdb_r_s, 'sdb_w_s': sdb_w_s, 'sdb_rkB_s': sdb_rkB_s, 'sdb_wkB_s': sdb_wkB_s, 'sdb_avgrq_sz': sdb_avgrq_sz, 'sdb_avgqu_sz': sdb_avgqu_sz, 'sdb_await': sdb_await, 'sdb_svctm': sdb_svctm, 'sdb_util': sdb_util, 'sdc_r_s': sdc_r_s, 'sdc_w_s': sdc_w_s, 'sdc_rkB_s': sdc_rkB_s, 'sdc_wkB_s': sdc_wkB_s, 'sdc_avgrq_sz': sdc_avgrq_sz, 'sdc_avgqu_sz': sdc_avgqu_sz, 'sdc_await': sdc_await, 'sdc_svctm': sdc_svctm, 'sdc_util': sdc_util, 'sdd_r_s': sdd_r_s, 'sdd_w_s': sdd_w_s, 'sdd_rkB_s': sdd_rkB_s, 'sdd_wkB_s': sdd_wkB_s, 'sdd_avgrq_sz': sdd_avgrq_sz, 'sdd_avgqu_sz': sdd_avgqu_sz, 'sdd_await': sdd_await, 'sdd_svctm': sdd_svctm, 'sdd_util': sdd_util, 'sde_r_s': sde_r_s, 'sde_w_s': sde_w_s, 'sde_rkB_s': sde_rkB_s, 'sde_wkB_s': sde_wkB_s, 'sde_avgrq_sz': sde_avgrq_sz, 'sde_avgqu_sz': sde_avgqu_sz, 'sde_await': sde_await, 'sde_svctm': sde_svctm, 'sde_util': sde_util, 'sda_r_s': sda_r_s, 'sda_w_s': sda_w_s, 'sda_rkB_s': sda_rkB_s, 'sda_wkB_s': sda_wkB_s, 'sda_avgrq_sz': sda_avgrq_sz, 'sda_avgqu_sz': sda_avgqu_sz, 'sda_await': sda_await, 'sda_svctm': sda_svctm, 'sda_util': sda_util}
            os.system('echo ' + str(line)[1:-1] + '| logger -p local0.debug')

def main():
    get_iostat()
    read_log()


if __name__ == '__main__':
    main()