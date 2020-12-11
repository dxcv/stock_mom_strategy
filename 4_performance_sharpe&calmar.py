import pandas as pd
import numpy as np
from utils import annualret_sharpe_calmar_cal

R_list = [5] + list(range(10, 120+1, 10))

sharpe_mat = pd.DataFrame(index=R_list, columns=R_list , data=np.nan)
calmar_mat = pd.DataFrame(index=R_list, columns=R_list, data=np.nan)

for R_stock in R_list:
    for R_mom in R_list:
        file_name = 'R_stock_{}_R_mom_{}.xlsx'.format(str(int(R_stock)), str(int(R_mom)))
        read_file_path = './stock_mom_nav/' + file_name
        mom_nav_series = pd.read_excel(read_file_path, index_col=[0])
        tradingday_num = 253.0

        max_drawdown_series = []
        x = pd.DataFrame(mom_nav_series).iloc[:, 0]
        r = (x - x.shift(1)) / (x.shift(1))
        r = r.fillna(0.0)
        # 年化收益率
        annual_ratio = (x.iloc[len(x) - 1] / x.iloc[0]) ** (tradingday_num / len(x)) - 1
        # 年化波动率
        annual_volatility = np.sqrt(r.var() * tradingday_num)
        # 夏普比率
        sharpe_ratio = (r.mean() / np.sqrt(r.var())) * np.sqrt(tradingday_num)
        sharpe_mat.loc[R_stock][R_mom] = sharpe_ratio

        # 最大回撤(滚动时间序列)
        for tag in range(len(x)):
            i_max = x.iloc[:tag].max()
            i_maxdrawdown = -1 * (1 - x[tag] / i_max)
            max_drawdown_series.append(i_maxdrawdown)

        max_drawdown_series[0] = 0
        max_drawdown_ts = pd.DataFrame(data=max_drawdown_series, index=mom_nav_series.index, columns=['MAX_DRAWDOWN'])

        max_drawdown = np.array(max_drawdown_series).min()

        calmar_ratio = annual_ratio / (-1) * max_drawdown
        calmar_mat.loc[R_stock][R_mom] = calmar_ratio

with pd.ExcelWriter('./mom_backtest_stat.xlsx') as writer:
    sharpe_mat.to_excel(writer, sheet_name='sharpe ratio')
    calmar_mat.to_excel(writer, sheet_name='calmar ratio')



sharpe_mat = pd.DataFrame(index=R_list, columns=R_list, data=np.nan)
calmar_mat = pd.DataFrame(index=R_list, columns=R_list, data=np.nan)
annualret_mat = pd.DataFrame(index=R_list, columns=R_list, data=np.nan)

for i in R_list:  # i为stock
    for j in R_list:  # j为mom
        file = './stock_mom_nav/R_stock_' + str(i) + '_R_mom_' + str(j) + '.xlsx'
        nav_series = pd.read_excel(file, index_col=0)
        annual_ratio,  sharpe_ratio, calmar_ratio = \
            annualret_sharpe_calmar_cal(nav_series, tradingday_num=253.0)

        annualret_mat.loc[i][j] = annual_ratio
        calmar_mat.loc[i][j] = calmar_ratio
        sharpe_mat.loc[i][j] = sharpe_ratio

with pd.ExcelWriter('./stock_mom_nav/stock_mom_backtest_stat.xlsx') as writer:
    sharpe_mat.to_excel(writer, sheet_name='sharpe ratio')
    calmar_mat.to_excel(writer, sheet_name='calmar ratio')
    annualret_mat.to_excel(writer, sheet_name='annual ratio')
