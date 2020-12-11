import pandas as pd
import numpy as np
from utils import trading_day_read, position_avg, posit_matrix_standardize, non_overlap_return, signal_process, nav_calculation

### 库存回测 ###

# 参数设置
start_date = '20060104'
end_date = '20201010'
cost_rate = 0.0003
N_list = [1,2,3,4,5,6,7,8]
R_list = [5] + list(range(10, 120+1, 10))
df = pd.DataFrame(index=R_list, columns=N_list)

for N in N_list:

    # 数据读取
    date_series_path = 'tradingday.xlsx'
    d_tradeday_list = trading_day_read(start_date, end_date, date_series_path)

    for R in R_list:

        signal_loading = pd.read_excel("./库存信号/库存信号(R={0}).xlsx".format(str(int(R))), index_col=[0])
        position_mat = position_avg(signal_loading.iloc[:, :-1])
        # 按N-week调仓周期截取positmat
        position_mat = position_mat.loc[(position_mat.index >= start_date) & (position_mat.index <= end_date)]
        position_mat = position_mat.iloc[list(range(0, len(position_mat.index), 5*N))]
        # 标准化position矩阵
        position_mat_std = posit_matrix_standardize(position_mat, d_tradeday_list)

        close_mat = pd.read_excel('close_adjusted.xlsx', index_col=0)
        close_mat[close_mat == 0] = np.nan  # 品种开始交易日期前收盘价为0，此处置空

        return_mat = non_overlap_return(close_mat, period=1)  # 计算单日收益矩阵

        # 净值计算
        nav = 1
        nav_series = pd.Series(index=position_mat_std.index, data=np.nan)
        position_diff_mat = pd.DataFrame()
        for i in position_mat_std.index:
            if i == position_mat_std.index[0]:  # 首日赋值，不考虑费率
                nav_series[i] = nav
            elif i in position_mat.index:  # 调仓日先计算收益，再计算仓位差额，再扣除费率
                nav = nav * ((position_mat_std.loc[i] * return_mat.loc[i]).sum(axis=0) + 1)
                position_diff = abs(position_mat_std.loc[i] - position_mat_std.shift(1).loc[i])
                position_diff_mat = position_diff_mat.append(position_diff)
                nav = nav * (1 - position_diff.sum(axis=0) * cost_rate)
                nav_series[i] = nav
            else:  # 非调仓日仅计算收益
                nav = nav * ((position_mat_std.loc[i] * return_mat.loc[i]).sum(axis=0) + 1)
                nav_series[i] = nav
        nav_series.to_excel("./库存回测/N_{0}_R_{1}.xlsx".format(str(N), str(R)))
        print("已完成N_{0}_R_{1}".format(str(N), str(R)))
        print(nav)
        df.loc[R, N] = nav

df.to_excel('./库存回测/summary.xlsx')



### 库存动量两因子回测 ###

# 参数设置
start_date = '20050516'
end_date = '20191231'
cost_rate = 0.0003
N = 2  # N周换仓

for R_stock in R_list:
    for R_mom in R_list:
        file_name = 'R_stock_{}_R_mom_{}.xlsx'.format(str(int(R_stock)), str(int(R_mom)))
        read_file_path = './stock_mom_signal/' + file_name
        write_file_path = './stock_mom_nav/' + file_name

        # 计算每日仓位
        signal_loading = pd.read_excel(read_file_path, index_col=[0])
        daily_position = signal_process(signal_loading, N, d_tradeday_list, start_date, end_date)
        # 计算净值
        nav = nav_calculation(daily_position, return_mat)

        nav.to_excel(write_file_path)
