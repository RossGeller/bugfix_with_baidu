# -*- coding: utf-8 -*-
# 作者: 张克
# 时间: 2024-11-14
# Topic: 面向航迹数据的爬虫，做了兼容性的设计，重写了代码。
# Description： 新增了IO协程，做了高耦合低内聚改进，提高了处理速度
# 版本：2.0.1
# Copyright (c) 五院四教. All rights reserved.
# -*-
import asyncio
import datetime
import logging
import os
import shutil
import time
from concurrent.futures import ThreadPoolExecutor
import aiohttp
import pandas as pd

# icao_path是需要爬取的Icao号列表
icao_Path = f"./ICAOAll_for_test.xlsx"

# track_Saved_path是爬取完毕文件存放地址，如果不存在，会在指定路径生成一个文件夹
track_Saved_path = f"D:/Track_data"

# date_end是查询时间的终止时间,定义为今天日期
date_end = datetime.date.today()


class WorkLog:
    """
    设计日志系统，包含写日志和读日志两个操作。
    """

    def __init__(self, log_file):
        self.log_file = log_file
        if not os.path.exists(self.log_file):
            with open(self.log_file, 'w') as file:
                print("第一次运行，创建更新日志文件")
                file.close()
        else:
            print("更新日志文件已存在")

    def read_date_from_workLog(self):
        with open(self.log_file, 'r') as f_r:
            date_start_string = f_r.read()  # 从文件中读取上次更新日期，是文本类型
            print('上次更新日期:', date_start_string)
            if os.path.getsize(self.log_file) != 0:
                # 从日志读出来的是string格式,强制转换为datetime格式,在转化为date格式
                date_Start = datetime.datetime.strptime(date_start_string, '%Y/%m/%d').date()

            else:
                print('无更新，爬虫第一次启动')
                # 如果是第一次用，date_start初始化查询时间的起始时间
                date_Start = datetime.date(2022, 1, 1)
                date_Start.strftime('%Y/%m/%d')
            f_r.close()
        return date_Start

    # 编写工作日志方法

    def write_data_to_workLog(self):
        with open(self.log_file, 'w') as f_w:
            date_End = datetime.date.today()  # 获取当天日期
            print('现在日期为：', date_End)
            f_w.write(date_End.strftime('%Y/%m/%d'))
            print('本次日期已同步更新日志')
            f_w.close()
        return date_End


def getDates(datestart, dateend):
    """
    时间段函数，生成Dates一个列表，根据输入的起始和终止时间计算出期间的每个日期，并转换为需要的格式
    :param datestart: 开始日期
    :param dateend: 截止日期
    :return: 一个列表，每个元素是一个日期
    """
    Dates = []
    detal = datetime.timedelta(days=1)
    while datestart <= dateend:
        Dates.append(datestart.strftime('%Y/%m/%d/'))
        datestart += detal
    # print(Dates)
    return Dates


def read_icao_excel(icao_path):
    """
    从指定路径文件中读取icao中得excel，然后返回
    :param icao_path: 指定文件路径
    :return: 返回读取得内容，以数组形式，分别是icao号，型号，机型，国别
    """
    # file_name = os.listdir(icao_folder_path)  # 读取文件
    # file_addr = icao_folder_path + '/' + file_name
    df = pd.read_excel(icao_path, sheet_name="Sheet1")
    # 将数据转换为数组
    # icao_list = df.iloc[:, :]
    icao_info = df.values.tolist()
    # data_array = df.shape
    print('要抓取的航迹基本信息读取完毕')
    return icao_info


async def fetchTrace_from_web(web):
    """
    在网页上异步抓取数据。输入目标列表三元组，为icao-date-url得到json字符串。
    :param web: 要抓取的网址数组web，为icao-date-url
    :return: 抓取的json数据
    """
    str1 = web[0]
    headers = {
        "accept": "application/json, text/javascript, */*; q=0.01",
        "accept-encoding": "gzip, deflate, br",
        "accept-language": "zh-CN,zh;q=0.9",
        "referer": "https://globe.adsbexchange.com/?icao=" + str1,
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36",
        "x-requested-with": "XMLHttpRequest"
    }
    async with aiohttp.ClientSession() as session:
        timeout = aiohttp.ClientTimeout(20)
        async with session.get(web[2], headers=headers, timeout=timeout) as r:
            result = await r.json()
            print('开始爬取：', r.url)
    return result


def parseTrace(jsonObj):
    """
    解析json中得数据
    :param jsonObj:网页返回的json数据
    :return:由一个个航迹点构成的对象
    """
    '''
    Json格式片段：
    {"icao":"a3520e",
    "r":"N313AZ",
    "t":"B763",
    "dbFlags":0,
    "desc":"BOEING 767-300",
    "ownOp":"CARGO AIRCRAFT MANAGEMENT INC",
    "year":"1990",
    "timestamp": 1732342123.994,
    "trace":[...]
    }
    '''
    icao = jsonObj["icao"]
    reg = jsonObj["r"]
    type = jsonObj["t"]
    # dbFlags = jsonObj["dbFlags"]  # 用不到
    # Fulltype = jsonObj["desc"]  # 用不到
    time_stamp = jsonObj["timestamp"]
    time_data = time.localtime(time_stamp)  # 把绝对时间转换为标准日期
    time_local = time.strftime("%Y_%m_%d", time_data)  # 取年月日
    Trace = jsonObj["trace"]
    commentData = []  # 一次航迹的所有航迹点
    information = []  # 存储一次航迹的基本信息
    dataItem = [icao, reg, type, time_local]  # 拼接成本次航迹基本信息
    # dataItem = [icao, reg, type, time_local, dbFlags, Fulltype, time_stamp]
    information.append(dataItem)
    for item in Trace:
        # 序号
        number = item[0]
        # 纬度
        latitude = item[1]
        # 经度
        longitude = item[2]
        # 海拔
        altitude = item[3]
        # 速度
        speed = item[4]
        # 航向
        track = item[5]
        # 垂直爬升率
        RateG = item[7]
        dataItem = [number, latitude, longitude, altitude, speed, track, RateG]  # 组成一个航迹点
        commentData.append(dataItem)  # 一个航迹点加入excel文件中
    # print(information)
    return information, commentData


def get_all_fetch_url(single_icao, Dates_for_single_icao):
    """
    抓取单个icao号和每个日期，生成一个列表
    :param single_icao: 一个icao号，字符
    :param Dates_for_single_icao: 所有日期
    :return: 返回列表三元组，为icao-date-url
    """
    single_icao_all_url = []  # 用于存放所有网址
    # 获取所有icao号
    # 获取单个icao号的所有日期
    for date_num in Dates_for_single_icao:
        str0 = date_num
        str1 = single_icao
        str2 = str1[-2] + str1[-1]
        url = "https://globe.adsbexchange.com/globe_history/" + str0 + "traces/" + str2 + "/trace_full_" + str1 + ".json"
        single_icao_all_url.append([single_icao, date_num, url])
    return single_icao_all_url


async def fetch_and_save_to_EXCEL(icao, model, type, nation, date_start, date_end, dirpath):
    """
    把单个icao数据抓取分解后放入excel
    :param icao: 单目标icao号
    :param model: 单目标型号
    :param type: 单目标机型
    :param nation: 单目标国家
    :param date_start: 抓取起始日期
    :param date_end: 抓取结束日期
    :param dirpath: 路径
    :return: 无
    """
    # 不存在，则创建文件夹，同时把爬取起始时间改了，因为用户会新增一个icao号
    single_icao_dirname = f"{icao}_{model}_{type}_{nation}"  # 文件夹名
    single_icao_dir_path = os.path.join(dirpath, single_icao_dirname)  # 文件夹地址

    if not os.path.exists(single_icao_dirname):
        print('没有对应ICAO号的文件夹，是第一次抓取')
        os.makedirs(single_icao_dirname)
        # 用于防止用户新增了一个ICAO号
        Dates_for_single_icao = getDates(datetime.date(2022, 1, 1), date_end)
    else:
        Dates_for_single_icao = getDates(date_start, date_end)

    single_icao_urls = get_all_fetch_url(icao, Dates_for_single_icao)  # 三元组列表

    tasks = [fetchTrace_from_web(url) for url in single_icao_urls]  # 创建所有异步任务
    results = await asyncio.gather(*tasks)  # 等待所有任务完成

    for result in results:
        try:
            Info, Data = parseTrace(result)
            # 准备数据
            df1_data = {
                'icao': [Info[0][0]],
                'reg': [Info[0][1]],
                'type': [Info[0][2]],
                'time_local': [Info[0][3]],
                'dbFlags': [Info[0][4]],
                'Fulltype': [Info[0][5]],
                'time_stamp': [Info[0][6]]
            }
            df2_data = {
                'Time': [],
                'Latitude': [],
                'Longitude': [],
                'Altitude': [],
                'Speed': [],
                'Track': [],
                'Geom_Rate': []
            }

            for item in range(len(Data)):
                # print(Data[item])
                df2.loc[item] = Data[item]

            # for item in Data:
            #     df2_data['Time'].append(item[0])  # 假设Data中的每个元素都是一个包含时间的列表或元组
            #     df2_data['Latitude'].append(item[1])  # 以此类推，根据Data的实际结构
            #     # ... 为其他字段添加数据

            df1 = pd.DataFrame(df1_data)
            df2 = pd.DataFrame(df2_data)

            # 可以选择将多个DataFrame合并为一个，或者分别写入不同的Excel文件
            # 这里为了简单起见，我们分别写入不同的文件
            singal_icao_file_fullname = os.path.join(single_icao_dir_path, f"{single_icao_dirname}_{Info[0][3]}.xlsx")
            with pd.ExcelWriter(singal_icao_file_fullname) as work_excel:
                df1.to_excel(work_excel, sheet_name='Sheet1', index=False)
                df2.to_excel(work_excel, sheet_name='Sheet2', index=False)
            print("写入完毕")
        except Exception as e:
            print(f"获取信息失败: {e}")  # 打印更具体的错误信息


# [icao , reg ,type,time_local,dbFlags,Fulltype,time_stamp]


def main():
    """提醒"""
    logging.warning('一定要按照预设的格式编写ICAOAll.xlsx')
    logging.warning('工作启动，您可以去忙别的事情了')
    logging.warning('有故障我会告诉你的，结束我也会告诉你的。')

    """启动更新日志"""
    # 初始化工作日志实例
    work_log = WorkLog("./useLog.txt")
    # 调用工作日志方法记录日志
    data_s = work_log.read_date_from_workLog()  # 起始时间
    data_e = work_log.write_data_to_workLog()  # 结束时间

    """数据准备"""
    icao_array = read_icao_excel(icao_Path)  # 得到了所有航迹数组

    """数据抓取"""
    for i in range(len(icao_array)):
        # 提取单个目标的
        single_icao = str(icao_array[i][0])  # 飞机呼号,excel会把字符串变成数字
        single_model = str(icao_array[i][1])  # 飞机型号
        single_type = str(icao_array[i][2])  # 飞机类型
        single_nationality = str(icao_array[i][3])  # 飞机隶属国别

        # getEXCEL函数根据输入的参数在指定路径生成以ICAO号为文件名的XLSX文件，其中按sheet保存了航迹信息
        asyncio.run(fetch_and_save_to_EXCEL(single_icao, single_model, single_type, single_nationality, data_s, data_e,
                                            track_Saved_path))

    """数据抓取结束"""
    logging.warning('程序执行结束，你可以关闭软件了。')


if __name__ == "__main__":
    main()
