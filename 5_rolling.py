import pandas as pd
import numpy as np
from utils import trading_day_read, position_avg, posit_matrix_standardize, non_overlap_return, signal_process, nav_calculation

### 生成滚动优化信号 ###

start_date = '20070402'
end_date = '20191231'
date_series_path = './tradingday.xlsx'
d_tradeday_list = trading_day_read(start_date, end_date, date_series_path)

# 全部净值汇总收益表
nav_all = pd.DataFrame(index=d_tradeday_list, columns=list(range(1,170)), data=np.nan)

# 净值数据读取
R_list = [5] + list(range(10, 120+1, 10))
i = 0  # 计数器
for R_stock in R_list:
    for R_mom in R_list:
        file_name = 'R_stock_{}_R_mom_{}.xlsx'.format(str(int(R_stock)), str(int(R_mom)))
        read_file_path = './stock_mom_nav/' + file_name
        nav_loading = pd.read_excel(read_file_path, index_col=[0])
        nav_all.iloc[:, i] = nav_loading.iloc[:, 0]
        i = i+1

# 将nan赋值为1
for k in range(len(nav_all.columns)):
    for i in range(len(nav_all)):
        if np.isnan(nav_all.iloc[i, k]):
            nav_all.iloc[i, k] = 1

# n_rolling_day = 200
n_rolling_day_list = list(range(40, 200+1, 10))
# 增加新的列: stock_x_mom_y
nav_all['stock_x_mom_y'] = np.nan
# 参数使用时间为n_day
# n_day=180
n_day_list = list(range(10, 200+1, 10))

for n_rolling_day in n_rolling_day_list:
    for n_day in n_day_list:
        if n_rolling_day > n_day:
            nav_all['stock_x_mom_y'] = np.nan
            n = int((len(nav_all)-n_rolling_day)/n_day)
            n_list = list(range(n))
            for i in n_list:
                sort_list = ((nav_all.iloc[n_rolling_day-1+n_day*i]-nav_all.iloc[0+n_day*i])/nav_all.iloc[0+n_day*i]).dropna().sort_values(ascending=False)
                mix=list(sort_list.index)  # 将收益率最大的列名，存到stock_x_mom_y
                nav_all.iloc[n_rolling_day-1+n_day*i, -1] = mix[0]

            name_list = ["铜","铝","橡胶","豆油","PTA","锌","塑料","棕榈油","黄金","螺纹钢","PVC","铅","焦炭","甲醇","白银","玻璃","焦煤","动力煤","沥青","铁矿石","聚丙烯","热轧卷板","镍","锡"]
            singal_rolling = pd.DataFrame(index=d_tradeday_list, columns=name_list, data=np.nan)

            # stock_x_mom_y 得出最优的排序期参数x 参数y
            for i in range(len(nav_all)):
                if np.isnan(nav_all.iloc[i,-1]) == False:
                    if nav_all.iloc[i, -1] == 13 or nav_all.iloc[i, -1] == 26 or nav_all.iloc[i, -1] == 39 or nav_all.iloc[i, -1] == 52 or nav_all.iloc[i, -1] == 65 or nav_all.iloc[i, -1] == 78 or nav_all.iloc[i, -1] == 91 or nav_all.iloc[i, -1] == 104 or nav_all.iloc[i, -1] == 117 or nav_all.iloc[i, -1] == 130 or nav_all.iloc[i, -1] == 143 or nav_all.iloc[i, -1] == 156 or nav_all.iloc[i, -1] == 169:
                        R_stock = int(nav_all.iloc[i, -1]/13)-1
                        R_mom = 12
                    else:
                        R_stock = int(nav_all.iloc[i, -1] / 13)
                        R_mom = int(nav_all.iloc[i, -1] % 13 - 1)
                    file_name = 'R_stock_{}_R_mom_{}_adjusted.xlsx'.format(str(int(R_list[R_stock])), str(int(R_list[R_mom])))
                    read_file_path = './stock_mom_signal_adjusted/' + file_name
                    signal_loading = pd.read_excel(read_file_path, index_col=[0])
                    singal_rolling.iloc[i+1:i+n_day+1, 0:-1] = signal_loading.iloc[i+1:i+n_day+1, 0:-1]

            singal_rolling.to_excel("./signal_rolling/n_rolling_day_{0}_n_day_{1}.xlsx".format(str(int(n_rolling_day)), str(int(n_day))))

### 滚动优化回测 ###

# 参数设置
start_date = '20070402'
end_date = '20191231'
cost_rate = 0.0003
N = 2  # N周换仓

# 数据读取
date_series_path = './tradingday.xlsx'
d_tradeday_list = trading_day_read(start_date, end_date, date_series_path)
close_mat = pd.read_excel('./close_mat_adjusted.xlsx', index_col=0)
close_mat[close_mat == 0] = np.nan  # 品种开始交易日期前收盘价为0，此处置空
return_mat = non_overlap_return(close_mat, period=1)  # 计算单日收益矩阵

n_rolling_day_list = list(range(40, 200+1, 10))
n_day_list = list(range(10, 200+1, 10))

for n_rolling_day in n_rolling_day_list :
    for n_day in n_day_list:
        if n_rolling_day > n_day:
            file_name = 'n_rolling_day_{}_n_day_{}.xlsx'.format(str(int(n_rolling_day)), str(int(n_day)))
            read_file_path = './signal_rolling/' + file_name
            write_file_path = './signal_rolling_nav/' + file_name

            # 计算每日仓位
            signal_loading = pd.read_excel(read_file_path, index_col=[0])
            daily_position = signal_process(signal_loading, N, d_tradeday_list)
            # 计算净值
            nav = nav_calculation(daily_position, return_mat)

            nav.to_excel(write_file_path)
