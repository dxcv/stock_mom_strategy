import numpy as np
import pandas as pd
from utils import mom_order, mom_signal, stock_order, stock_signal, factor_m, factor_s
import math

# 库存、动量单因子信号
R_list = [5] + list(range(10, 120+1, 10))
min_num = 5

for R in R_list:
    mom_file_path = "close_adjusted.xlsx"
    mom_data = pd.read_excel(mom_file_path, index_col=0)
    mom_order(mom_data, R, min_num)
    mom_signal(mom_data, R, min_num)
    stock_file_path = "stock_adjusted.xlsx"
    stock_data = pd.read_excel(stock_file_path, index_col=0)
    stock_order(stock_data, R, min_num)
    stock_signal(stock_data, R, min_num)

nav_mat = pd.DataFrame(index=R_list, columns=R_list, data=np.nan)  # index为stock，columns为mom
for R_stock in R_list:
    stock_factor = factor_s(stock_data, R_stock, min_num)
    for R_mom in R_list:
        mom_factor = factor_m(mom_data, R_mom, min_num)

        # 时间序列统一
        mom_factor = mom_factor[mom_factor.index.isin(stock_factor.index)]
        stock_factor = stock_factor[stock_factor.index.isin(mom_factor.index)]

        # 用stock筛选前后40%，分别做多和做空
        signal_long = pd.DataFrame(index=stock_factor.index, columns=stock_factor.columns, data=np.nan)
        signal_short = pd.DataFrame(index=stock_factor.index, columns=stock_factor.columns, data=np.nan)
        # stock_rank = stock_factor.rank(axis=1, pct=True)  # rank默认ascending=True升序，库存越低rank值越低
        # signal_long[stock_rank <= 0.4] = 1
        # signal_short[stock_rank >= 0.6] = 1
        stock_rank = stock_factor.rank(axis=1, method='first')  # rank默认ascending=True升序，库存越低rank值越低
        for i in stock_rank.index:
            num = stock_rank.loc[i].count()
            selected_num = math.ceil(num * 0.4)
            signal_long.loc[i][stock_rank.loc[i] <= selected_num] = 1
            signal_short.loc[i][stock_rank.loc[i] > (num-selected_num)] = 1

        # 再用mom筛选多空的前后50%
        # ascending=False降序，动量越高rank值越低
        long_mom_rank = (signal_long * mom_factor).rank(axis=1, ascending=False)
        short_mom_rank = (signal_short * mom_factor).rank(axis=1, ascending=False)
        for i in signal_long.index:
            num = signal_long.loc[i].count()
            selected_num = math.ceil(num * 0.5)
            signal_long.loc[i][long_mom_rank.loc[i] > selected_num] = np.nan
            signal_short.loc[i][short_mom_rank.loc[i] <= selected_num] = np.nan
        # signal_long[long_mom_rank > 0.5] = np.nan  # 筛掉做多部分的后50%
        # signal_short[short_mom_rank <= 0.5] = np.nan  # 筛掉做空部分的前50%

        # 构造统一signal矩阵
        signal_mat = signal_long.fillna(0) + signal_short.multiply(-1).fillna(0)
        signal_mat[signal_mat == 0.0] = np.nan
        signal_mat.to_excel("./stock_mom_signal/R_stock_{0}_R_mom_{1}.xlsx".format(str(int(R_stock)), str(int(R_mom))))
