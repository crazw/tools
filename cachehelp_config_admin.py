#!/usr/bin/env python
# -*- encoding: utf-8 -*-
#****************************************************************#
# ScriptName: cachehelp_config_admin.py
# Author: craazw@gmail.com
# Create Date: 2016-02-26 10:11
# Function: 根据配置文件配置crontab任务
#***************************************************************#

from cache_help import read_conf
from cache_help import recover_task
import os
import shutil


def get_cron_list(conf):
    cron_list = []
    commit_cron = feedback_cron = relay_push_cron = change_task_cron = ''
    if conf['enable'] == 1:
        # 提交数据cron任务
        commit_time = int(conf['commit_period']) / 60
        if 1 <= commit_time < 60:
            if commit_time in [1, 2, 3, 4, 5, 6, 10, 12, 15, 20, 30]:
                commit_cron = '*/' + str(commit_time) + ' * * * * python /home/icache/cachehelp/cachehelp_commit.py #cachehelp cron\n'
        elif 60 <= commit_time < 1440:
            commit_time /= 60
            commit_cron = '0 */' + str(commit_time) + ' * * * python /home/icache/cachehelp/cachehelp_commit.py #cachehelp cron\n'
        else:
            print "commit_period error"
            exit()
        cron_list.append(commit_cron)

        # 自统计cron任务
        feedback_time = int(conf['statistics_period'])

        if 1 < feedback_time < 24:
            if feedback_time in [1, 2, 3, 4, 6, 8, 12]:
                feedback_cron = '0 */' + str(feedback_time) + ' * * * python /home/icache/cachehelp/feedback.py #cachehelp cron\n'
        elif 24 <= feedback_time:
            feedback_time /= 24
            feedback_cron = '0 0 */' + str(feedback_time) + ' * * python /home/icache/cachehelp/feedback.py #cachehelp cron\n'
        else:
            print "statistics_period error"
            exit()
        cron_list.append(feedback_cron)

        # 补发检查cron任务
        relay_push_time = int(conf['relay_push_size'])

        if 1 < relay_push_time < 24:
            if relay_push_time in [1, 2, 3, 4, 6, 8, 12]:
                relay_push_cron = '0 */' + str(relay_push_time) + ' * * * python /home/icache/cachehelp/check_lost.py #cachehelp cron\n'
        elif 24 <= relay_push_time:
            relay_push_time /= 24
            relay_push_cron = '0 0 */' + str(relay_push_time) + ' * * python /home/icache/cachehelp/check_lost.py #cachehelp cron\n'
        else:
            print "statistics_period error"
            exit()
        cron_list.append(relay_push_cron)

        # 任务转化的cron任务
        if conf['canhelp'] == 1:
            change_task_time = int(conf['admin_period']) / 60
            if 1 <= change_task_time < 60:
                if change_task_time in [1, 2, 3, 4, 5, 6, 10, 12, 15, 20, 30]:
                    change_task_cron = '*/' + str(change_task_time) + ' * * * * python /home/icache/cachehelp/cachehelp_task.py #cachehelp cron\n'
            elif 60 <= change_task_time < 1440:
                change_task_time /= 60
                change_task_cron = '0 */' + str(change_task_time) + ' * * * python /home/icache/cachehelp/cachehelp_task.py #cachehelp cron\n'
            else:
                print "admin_period error"
                exit()
            cron_list.append(change_task_cron)

        # 任务转化: 根据具体的category是否变化,调整mode类型
        recover_task(conf)

    return cron_list


def update_crontab(cron_list):
    # 导出crontab
    os.system('crontab -l > mycron.txt')

    # 删除旧的
    with open('mycron.txt', 'r') as f:
        with open('mycron.log', 'w') as g:
            for line in f.readlines():
                if '#cachehelp cron' not in line:
                    g.write(line)
    shutil.move('mycron.log', 'mycron.txt')

    # 写入新的
    try:
        fp = open('mycron.txt', 'a')
    except Exception, e:
        raise e
    else:
        for iterm in cron_list:
            fp.write(iterm)
        fp.close()

    # 导入crontab
    os.system('crontab mycron.txt && rm mycron.txt')


def main(conf_file):
    # 读取配置文件
    conf = read_conf(conf_file)

    # 获取新版crontab
    cron_list = get_cron_list(conf)

    # 更新crontab
    update_crontab(cron_list)

if __name__ == '__main__':
    conf_file = '/home/icache/cachehelp/cachehelp.conf'

    main(conf_file)