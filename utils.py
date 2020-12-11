import pandas as pd
import numpy as np


def data_to_df(apidata):
    """
    将万得API获取的原始数据转换为pandas.Dataframe
    :param apidata: 从万得API获取的原始数据
    :return: pandas.Dataframe
    """
    df = pd.DataFrame(apidata.Data)
    df = df.T
    df.index = apidata.Times
    df.columns = apidata.Codes
    return df


def loop_order(current_month):
    '''
    根据当前交易月判断合约月份循环顺序
    :param current_month: 当前交易月
    :return: 合约月份循环顺序
    '''
    loop_order = []
    for i in range(12):
        if current_month <= 11:
            current_month += 1
            loop_order.append(current_month)
        elif current_month > 11 and current_month <= current_month + 12:
            current_month += 1
            loop_order.append(current_month - 12)
    return loop_order


def adjusted_close_px(name):
    '''
    换月跳空价格调整
    :param name: 品种名称
    :return: 跳空调整后收盘价序列
    '''
    file_path = "./合约/{0}.xlsx".format(name)
    detailed_data = pd.read_excel(file_path)
    detailed_data = detailed_data.set_index(keys='Unnamed: 0')

    # 获取品种首个交易日对应index
    for i in range(len(detailed_data)):
        if np.isnan(detailed_data.iloc[i, 0]) == False:
            initial_index = i
            break

    detailed_data[name] = None
    # drop = 0
    current_month = detailed_data.index[initial_index].month
    # drop_dict = {} # debug
    drop_dates = []

    for i in range(initial_index, len(detailed_data)):
        if i == initial_index:
            detailed_data.iloc[i, -1] = detailed_data.iloc[i, 0]
            order = loop_order(current_month) # 合约循环顺序
            for j in order:
                if detailed_data.iloc[i, 0] == detailed_data.iloc[i, j]:
                    record = j
                    # print(record)
                    break
            if order[0] != record:
                order = loop_order(record - 1)
        else:
            for k in order:
                if detailed_data.iloc[i, 0] == detailed_data.iloc[i, k]:
                    new_record = k
                    # print(new_record)
                    break
            if new_record != record:
                # drop += detailed_data.iloc[i, 0] - detailed_data.iloc[i-1, 0]
                # drop_dict[detailed_data.index[i]] = drop
                drop_dates.append(detailed_data.index[i])
                # print(drop)
                detailed_data.iloc[i, -1] = detailed_data.iloc[i-1, -1]
                record = new_record
                order = loop_order(new_record - 1)
            else:
                detailed_data.iloc[i, -1] = detailed_data.iloc[i-1, -1] * (detailed_data.iloc[i, 0] / detailed_data.iloc[i-1, 0])

    return detailed_data[name], drop_dates


def mom_order(data, R, min_num):
    '''
    生成动量指标排序
    :param data: 跳空调整后主连收盘价数据
    :param R: 时间窗口（排序期）
    :param min_num: 开始策略回测的最低品种数
    :return: 动量指标排序
    '''
    # 计算时间窗口（排序期R）内各品种历史收益率
    for i in range(len(data.columns)):
        data.iloc[:, i] = data.iloc[:, i] / data.iloc[:, i].shift(R) - 1

    # 生成排序
    signal_df = data.rank(axis=1, method="first")

    # count当日品种数，形成新列
    signal_df["num"] = signal_df.count(axis='columns')

    for i in range(len(signal_df['num'])):
        if signal_df['num'][i] == min_num:
            initial_index = i
            break

    signal_df = signal_df.iloc[initial_index:, :]

    signal_df.to_excel("./动量因子排序/动量因子排序(R={0}).xlsx".format(str(int(R))))
    print("已完成{0}".format(str(R)))


def mom_signal(data, R, min_num):
    # 计算时间窗口（排序期R）内各品种历史收益率
    for i in range(len(data.columns)):
        data.iloc[:, i] = data.iloc[:, i] / data.iloc[:, i].shift(R) - 1

    # count当日品种数，形成新列
    data["num"] = data.count(axis='columns')

    for i in range(len(data['num'])):
        if data['num'][i] == min_num:
            initial_index = i
            initial_date = data.index[i]
            break

    signal_df = data.iloc[initial_index:, :]

    # 生成信号
    for i in range(len(signal_df)):
        n = signal_df.iloc[i, -1] * 0.2
        if n % 1 != 0:
            n = int(n) + 1
        else:
            n = int(n)

        sort_list = signal_df.iloc[i, :-1].dropna().sort_values(ascending=False)
        buy = []
        sell = []
        for j in range(n):
            buy.append(sort_list[j])
            sell.append(sort_list[-(j + 1)])

        for k in range(len(signal_df.iloc[i, :-1])):
            if signal_df.iloc[i, k] in buy:
                signal_df.iloc[i, k] = 1
            elif signal_df.iloc[i, k] in sell:
                signal_df.iloc[i, k] = -1
            else:
                signal_df.iloc[i, k] = 0
    signal_df = signal_df.shift(1)
    signal_df.to_excel("./动量信号/动量信号(R={0}).xlsx".format(str(int(R))))
    print("已完成{0}".format(str(R)))


def stock_order(data, R, min_num):
    '''
    生成库存偏离度指标排序
    :param data: 库存数据
    :param R: 计算偏离度时分子所用移动平均时间窗口
    :param min_num: 开始策略回测的最低品种数
    :return: 库存偏离度指标排序
    '''
    # 计算时间窗口（排序期R）库存偏离度
    for i in range(len(data.columns)):
        data_1 = data.copy()
        data.iloc[:, i] = data_1.iloc[:, i] / data_1.iloc[:, i].rolling(R).mean()

    # 生成排序
    signal_df = data.rank(axis=1, method="first")

    # count当日品种数，形成新列
    signal_df["num"] = signal_df.count(axis='columns')

    for i in range(len(signal_df['num'])):
        if signal_df['num'][i] == min_num:
            initial_index = i
            break

    signal_df = signal_df.iloc[initial_index:, :]

    signal_df.to_excel("./库存因子排序/库存因子排序(R={0}).xlsx".format(str(int(R))))
    print("已完成{0}".format(str(R)))


def stock_signal(data, R, min_num):
    # 计算时间窗口（排序期R）库存偏离度
    for i in range(len(data.columns)):
        data_1 = data.copy()
        data.iloc[:, i] = data_1.iloc[:, i] / data_1.iloc[:, i].rolling(R).mean()

    # count当日品种数，形成新列
    data["num"] = data.count(axis='columns')

    for i in range(len(data['num'])):
        if data['num'][i] == min_num:
            initial_index = i
            initial_date = data.index[i]
            break

    signal_df = data.iloc[initial_index:, :]

    # 生成信号
    for i in range(len(signal_df)):
        n = signal_df.iloc[i, -1] * 0.2
        if n % 1 != 0:
            n = int(n) + 1
        else:
            n = int(n)

        sort_list = signal_df.iloc[i, :-1].dropna().sort_values(ascending=True)
        buy = []
        sell = []
        for j in range(n):
            buy.append(sort_list[j])
            sell.append(sort_list[-(j + 1)])

        for k in range(len(signal_df.iloc[i, :-1])):
            if signal_df.iloc[i, k] in buy:
                signal_df.iloc[i, k] = 1
            elif signal_df.iloc[i, k] in sell:
                signal_df.iloc[i, k] = -1
            else:
                signal_df.iloc[i, k] = 0
    # signal_df = signal_df.shift(1)
    signal_df.to_excel("./库存信号/库存信号(R={0}).xlsx".format(str(int(R))))
    print("已完成{0}".format(str(R)))


def factor_m(data, R, min_num):
    # 计算时间窗口（排序期R）内各品种历史收益率
    for i in range(len(data.columns)):
        data.iloc[:, i] = data.iloc[:, i] / data.iloc[:, i].shift(R) - 1
    new_data = data[data.count(axis=1) >= min_num]  # 筛选大于最小品种限制的日期
    factor_mat = new_data.shift(1).iloc[1:]
    return factor_mat


def factor_s(data, R, min_num):
    # 计算时间窗口（排序期R）内各品种历史收益率
    for i in range(len(data.columns)):
        data_1 = data.copy()
        data.iloc[:, i] = data_1.iloc[:, i] / data_1.iloc[:, i].rolling(R).mean()
    new_data = data[data.count(axis=1) >= min_num]
    factor_mat = new_data.shift(1).iloc[1:]
    return factor_mat


def trading_day_read(start, end, file_url):
    # 交易日序列读取模块
    trading_day_series = pd.read_excel(file_url, index_col=None, header=0)
    trading_day_list_d = list(
        trading_day_series.loc[(trading_day_series.d >= start) & (trading_day_series.d <= end)]['d'])
    return trading_day_list_d


def position_avg(factor_sig):
    # 选中的1，没选择的0，等权
    weight = factor_sig
    # 等权计算；若使用流通市值加权则需要调整
    weight = (weight.T / abs(weight).sum(1)).T
    stock_posit = weight.dropna(how='all')
    return stock_posit


def non_overlap_return(daily_close, period=1):
    # 单日收益率计算模块
    close_price = daily_close.copy()
    close_price['day_since_inception'] = range(len(close_price))
    specific_close = close_price[close_price.day_since_inception % period == 0]
    specific_close = specific_close.drop('day_since_inception', axis=1)
    specific_return = specific_close / specific_close.shift(1) - 1
    return specific_return


def posit_matrix_standardize(positmat, tradingday_data):
    # posit矩阵标准化模块
    posit_mat_std = pd.DataFrame(index=tradingday_data, columns=positmat.columns)
    posit_mat_std = posit_mat_std.loc[posit_mat_std.index >= positmat.index[0]]
    i = 1  # factor_sig计数器
    for index, row in posit_mat_std.iterrows():
        if i < len(positmat):
            if index < positmat.index[i]:  # 如果posit_mat.index小于等于factor_sig.index[i]说明还没到换仓的时候
                row[positmat.columns] = positmat.loc[positmat.index[i - 1]]
            else:  # 到了换仓的时候，计数器往后推，指向下一个换仓日
                i = i + 1
                row[positmat.columns] = positmat.loc[positmat.index[i - 1]]
        else:
            row[positmat.columns] = positmat.loc[positmat.index[-1]]
    posit_mat_std = posit_mat_std.astype('float64')  # 使用iterrows会改变数据类型，在此改回float
    return posit_mat_std


def signal_process(signal, N, tradeday, start_date, end_date):
    # 信号读取与处理
    position_mat = position_avg(signal)
    # 按N-week调仓周期截取positmat
    position_mat = position_mat.loc[(position_mat.index >= start_date) & (position_mat.index <= end_date)]
    position_mat = position_mat.iloc[list(range(0, len(position_mat.index), 5 * N))]
    # 标准化position矩阵
    position_mat_std = posit_matrix_standardize(position_mat, tradeday)
    return position_mat_std


def nav_calculation(position, daily_return):
    # 净值计算
    nav_tmp = 1
    nav_series = pd.Series(index=position.index, data=np.nan)
    position_diff_mat = pd.DataFrame()
    for i in position.index:
        if i == position.index[0]:  # 首日赋值，不考虑费率
            nav_series[i] = nav_tmp
        elif i in position.index:  # 调仓日先计算收益，再计算仓位差额，再扣除费率
            nav_tmp = nav_tmp * ((position.loc[i] * daily_return.loc[i]).sum(axis=0) + 1)
            position_diff = abs(position.loc[i] - position.shift(1).loc[i])
            position_diff_mat = position_diff_mat.append(position_diff)
            nav_tmp = nav_tmp * (1 - position_diff.sum(axis=0) * cost_rate)
            nav_series[i] = nav_tmp
        else:  # 非调仓日仅计算收益
            nav_tmp = nav_tmp * ((position.loc[i] * daily_return.loc[i]).sum(axis=0) + 1)
            nav_series[i] = nav_tmp
    return nav_series


def annualret_sharpe_calmar_cal(nav_series, tradingday_num):
    max_drawdown_series = []
    x = pd.DataFrame(nav_series).iloc[:, 0]
    r = (x - x.shift(1)) / (x.shift(1))
    r = r.fillna(0.0)
    # 年化收益率
    annual_ratio = (x.iloc[len(x) - 1] / x.iloc[0]) ** (tradingday_num / len(x)) - 1

    # 年化波动率
    # annual_volatility = np.sqrt(r.var() * tradingday_num)
    # 夏普比率
    sharpe_ratio = (r.mean() / np.sqrt(r.var())) * np.sqrt(tradingday_num)

    # 最大回撤(滚动时间序列)
    for tag in range(len(x)):
        i_max = x.iloc[:tag].max()
        i_maxdrawdown = -1 * (1 - x[tag] / i_max)
        max_drawdown_series.append(i_maxdrawdown)

    max_drawdown_series[0] = 0
    # max_drawdown_ts = pd.DataFrame(data=max_drawdown_series, index=mom_nav_series.index, columns=['MAX_DRAWDOWN'])

    max_drawdown = np.array(max_drawdown_series).min()

    calmar_ratio = annual_ratio / (-1) * max_drawdown

    return annual_ratio, sharpe_ratio, calmar_ratio
