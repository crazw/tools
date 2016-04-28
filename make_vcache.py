#!/usr/bin/env python
# encoding: utf-8

"""
@     file: make_vcache.py
@     time: 4/18/16 4:10 PM
@   author: crazw
@     site: www.crazw.com
@  contact: craazw@gmail.com
@ function: 自动生成cache数据,以:2016-04-12为原始数据
"""
import argparse
import os
import time
import sys
import datetime
import commands
import tarfile


# 自定义提示信息
class MyParser(argparse.ArgumentParser):
    """
    自定义argparse.ArgumentParser类,为其添加帮助输出提示信息
    """

    # 红色输出函数
    @staticmethod
    def inred(s):
        return "%s[31;31m%s%s[0m" % (chr(27), s, chr(27))

    def notice_help(self, msg):
        sys.stderr.write('\n===> 错误: ' + MyParser.inred(msg) + "\n\n" + "-" * 35 + "具体用法" + "-" * 35 + "\n")
        self.print_help()
        sys.exit(2)


class ArgsParse(object):
    """
    解析参数
    """

    def __init__(self):
        self.parser = MyParser(version='1.0.0')

        # 必填参数
        self.require_group = self.parser.add_argument_group(MyParser.inred('原数据相关参数'))
        self.require_group.add_argument("--mtar", type=str, default='./cache_output_24h.tar.gz',
                                        help='生产数据的母包路径,默认是:./cache_output_24h.tar.gz')

        # Commit模式的参数组
        self.commit_group = self.parser.add_argument_group(MyParser.inred('单天模式'))
        self.commit_group.add_argument("--out_day", type=str,
                                       help='生成虚拟包的日期(开始日期), 日期格式: 2016-03-18')
        self.commit_group.add_argument("--out_dir", type=str, default='./',
                                       help='生成数据输出路径, 默认是(当前路径): ./')

        # Canhelp模式的参数组
        self.canhelp_group = self.parser.add_argument_group(MyParser.inred('多天模式(可选)'))
        self.canhelp_group.add_argument("-n", type=int,
                                        help='输出连续多少天的数据, 比如: -n 30 ')

        self.args = self.parser.parse_args()

    def __check_mtar_arg(self):
        # 没有参数时,输出提示信息
        if not self.args.mtar:
            src_file = "./cache_output_24h.tar.gz"
        else:
            src_file = self.args.mtar

        if not os.path.exists(src_file):
            self.parser.notice_help('母包: ' + src_file + ' 不存在,请确认!')

        print "===> 母文件路径: " + src_file

        return src_file

    def __check_out_day_arg(self):
        try:
            time.strptime(self.args.out_day, '%Y-%m-%d')
        except:
            self.parser.notice_help('时间参数格式不对, 具体如: 2018-08-08')
        time_args = time.strftime('%Y-%m-%d', time.strptime(self.args.out_day, '%Y-%m-%d'))
        return time_args

    def __check_out_dir_arg(self):
        if not self.args.out_dir:
            out_dir = "./"
        else:
            out_dir = self.args.out_dir

        if not os.path.exists(out_dir):
            self.parser.notice_help('输出路径: ' + out_dir + ' 不存在,请手动创建!')
        print "===> 输出路径: " + out_dir

        return out_dir

    def __check_days_arg(self):
        if not self.args.n:
            days = 1
        else:
            days = int(self.args.n)
        print "===> 产生数据: " + str(days) + " 天"

        return days

    def get_args(self):
        __args = dict({})

        # 检查参数
        __args['mtar'] = self.__check_mtar_arg()
        __args['out_day'] = self.__check_out_day_arg()
        __args['out_dir'] = self.__check_out_dir_arg()
        __args['days'] = self.__check_days_arg()

        return __args


class MakeVcache(object):
    def __init__(self, args):
        self.tar_path = args['mtar']
        self.target_path = ''
        self.out_dir = args['out_dir']
        self.day = args['days']
        self.out_day = args['out_day']

    # 解压
    def __extract(self):
        tar = tarfile.open(self.tar_path)
        tar.extractall(path=self.target_path)
        tar.close()

    # 打包
    def __make_tar(self):
        # 创建压缩包名
        tar = tarfile.open(self.out_dir + "/vcache." + self.out_day + '-' + str(self.day) + ".tar.gz", "w:gz")

        tmp_time_day = time.strptime(self.out_day, '%Y-%m-%d')
        tmp_datetime_day = datetime.datetime(tmp_time_day[0], tmp_time_day[1], tmp_time_day[2])

        # 创建压缩包
        for root, dir, files in os.walk(self.target_path + "cache_output"):
            for fitem in files:
                fullpath = os.path.join(root, fitem)
                # 处理文件
                print "===> 处理文件: " + fullpath

                # 临时去掉 UNLOCK TABLES;
                commands.getoutput('sed -i "s#UNLOCK TABLES;##g" ' + fullpath)

                # 复制所有的insert语句到临时文件
                commands.getoutput('cat ' + fullpath + ' | grep "INSERT" >> ' +
                                   self.target_path + "cache_output/" + fitem + '_tmp')

                # 追加具体的每一天
                for day_ago in range(self.day):
                    day = tmp_datetime_day - datetime.timedelta(days=-day_ago)
                    day = day.strftime('%Y-%m-%d')
                    print "===> 添加日期: " + day
                    if day_ago >= 1:
                        commands.getoutput('cat ' + self.target_path + "cache_output/" + fitem + '_tmp >> ' + fullpath)
                    commands.getoutput('sed -i "s#2016-04-14#' + day + '#g" ' + fullpath)

                # 添加临时去掉的 UNLOCK TABLES;
                commands.getoutput('echo "UNLOCK TABLES;" >> ' + fullpath)

                # 添加到tar包
                tar.add(fullpath)
        tar.close()

        # 清理残留文件
        commands.getoutput('rm -fr ' + self.target_path + "cache_output")

    def make_cache(self):
        self.__extract()
        self.__make_tar()


def main():
    # 解析参数
    ap_class = ArgsParse()
    args = ap_class.get_args()

    # 制作数据
    mv_class = MakeVcache(args)
    mv_class.make_cache()


if __name__ == '__main__':
    main()
