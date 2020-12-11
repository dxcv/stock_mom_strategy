from WindPy import *
import pandas as pd
import numpy as np
from utils import data_to_df
from utils import loop_order, adjusted_close_px

w.start()

codes = "CU.SHF,AL.SHF,RU.SHF,Y.DCE,TA.CZC,ZN.SHF,L.DCE,P.DCE,AU.SHF,RB.SHF,V.DCE,PB.SHF,J.DCE,MA.CZC,AG.SHF,FG.CZC," \
        "JM.DCE,ZC.CZC,BU.SHF,I.DCE,PP.DCE,HC.SHF,NI.SHF,SN.SHF"
code_list = ["CU.SHF","AL.SHF","RU.SHF","Y.DCE","TA.CZC","ZN.SHF","L.DCE","P.DCE","AU.SHF","RB.SHF","V.DCE","PB.SHF",
             "J.DCE","MA.CZC","AG.SHF","FG.CZC","JM.DCE","ZC.CZC","BU.SHF","I.DCE","PP.DCE","HC.SHF","NI.SHF","SN.SHF"]
name_list = ["铜","铝","橡胶","豆油","PTA","锌","塑料","棕榈油","黄金","螺纹钢","PVC","铅","焦炭","甲醇","白银","玻璃","焦煤",
             "动力煤","沥青","铁矿石","聚丙烯","热轧卷板","镍","锡"]

# 获取24个品种主连收盘价
raw_data = w.wsd(codes, "close", "2006-01-01", "2020-10-10", "")
data = data_to_df(raw_data)
data.columns = name_list
data.to_excel("主连收盘价.xlsx")

# 获取24个品种十二个月合约价格
codes = pd.read_excel("合约列表.xlsx")
codes = codes.set_index(keys="品种")

for index, row in codes.iterrows():
    code_str = ""
    for i in range(len(row)):
        if i == 0:
            code_str += row[i]
        else:
            try:
                code_str += "," + row[i]
            except:
                break

    raw_close_data = w.wsd(code_str, "close", "2006-01-01", "2020-10-10", "")
    close_data = data_to_df(raw_close_data)
    close_data.to_excel("./合约/{0}.xlsx".format(index))

# 导入各交易所库存、社会库存对应Wind数据代码
codes_df = pd.read_excel("库存数据代码.xlsx")

codes_str = ""
for i in range(len(codes_df)):
    if i == 0:
        codes_str += str(codes_df.iloc[i, 2])
    else:
        try:
            codes_str += "," + str(codes_df.iloc[i, 2])
        except:
            break

# 获取24个品种库存数据
raw_stock_data = w.edb(codes_str, "2006-01-01", "2020-10-10", "Fill=Previous")
stock_data = data_to_df(raw_stock_data)
# stock_data.to_excel("库存_raw.xlsx")
#
# stock_data = pd.read_excel("库存_raw.xlsx")
# stock_data = stock_data.set_index(keys='Unnamed: 0')

stock_preprocessed = pd.DataFrame(index=stock_data.index, columns=name_list)

# 处理库存数据，按品种加总、统一单位
for name in name_list:
    codes = list(codes_df[codes_df["品种"] == name]["数据代码"])
    codes = [str(j) for j in codes]
    units = list(codes_df[codes_df["品种"] == name]["单位"])
    # 统一单位
    for i in range(len(codes)):
        if units[i] == "万吨":
            stock_data[codes[i]] = stock_data[codes[i]] * 10000
        elif units[i] == "千克" or units[i] == "公斤":
            stock_data[codes[i]] = stock_data[codes[i]] / 1000
        elif units[i] == "短吨":
            stock_data[codes[i]] = stock_data[codes[i]] * 0.9072
        elif units[i] == "金衡盎司":
            stock_data[codes[i]] = stock_data[codes[i]] * 0.0311 / 1000
    stock_preprocessed[name] = stock_data[codes].sum(axis=1)

stock_preprocessed = stock_preprocessed.replace(0, np.nan)
# stock_preprocessed.to_excel("库存数据.xlsx")

# 处理库存数据，按品种加总、统一单位、按数据频度进行调整
for name in name_list:
    codes = list(codes_df[codes_df["品种"] == name]["数据代码"])
    codes = [str(j) for j in codes]
    units = list(codes_df[codes_df["品种"] == name]["单位"])
    freqs = list(codes_df[codes_df["品种"] == name]["频度"])
    # 统一单位
    for i in range(len(codes)):
        if units[i] == "万吨":
            if freqs[i] == "日":
                stock_data[codes[i]] = stock_data[codes[i]].shift(1) * 10000
            else:
                stock_data[codes[i]] = stock_data[codes[i]].shift(7) * 10000
        elif units[i] == "千克" or units[i] == "公斤":
            if freqs[i] == "日":
                stock_data[codes[i]] = stock_data[codes[i]].shift(1) / 1000
            else:
                stock_data[codes[i]] = stock_data[codes[i]].shift(7) / 1000
        elif units[i] == "短吨":
            if freqs[i] == "日":
                stock_data[codes[i]] = stock_data[codes[i]].shift(1) * 0.9072
            else:
                stock_data[codes[i]] = stock_data[codes[i]].shift(7) * 0.9072
        elif units[i] == "金衡盎司":
            if freqs[i] == "日":
                stock_data[codes[i]] = stock_data[codes[i]].shift(1) * 0.0311 / 1000
            else:
                stock_data[codes[i]] = stock_data[codes[i]].shift(7) * 0.0311 / 1000
        else:
            if freqs[i] == "日":
                stock_data[codes[i]] = stock_data[codes[i]].shift(1)
            else:
                stock_data[codes[i]] = stock_data[codes[i]].shift(7)
    stock_preprocessed[name] = stock_data[codes].sum(axis=1)

stock_preprocessed = stock_preprocessed.replace(0, np.nan)
stock_preprocessed.to_excel("库存数据_new.xlsx")

# 读取库存数据及调整后收盘价
stock_preprocessed = pd.read_excel("库存数据_new.xlsx")
stock_preprocessed = stock_preprocessed.set_index(keys="Unnamed: 0")
data = pd.read_excel("close_adjusted.xlsx")
data = data.set_index(keys='Unnamed: 0')
stock_preprocessed = stock_preprocessed.loc[data.index[0]:, :]

# 删除没有收盘价格（未交易）时的库存数据
for name in stock_preprocessed.columns:
    if np.isnan(data[name][0]):
        for i in range(len(data)):
            if np.isnan(data[name][i]) == False:
                end_index = i - 1
                start_index = i
                break
        end_date = data.index[end_index]
        start_date = data.index[start_index]
        stock_preprocessed.loc[:end_date, name] = np.nan
        stock_preprocessed.loc[start_date:, name].fillna(method="ffill")

stock_preprocessed.to_excel("stock_adjusted.xlsx")

# 主连收盘价跳空调整
exception_list = ["铜","铝","锌","铅","镍","锡"]
close_adjusted = pd.DataFrame()

for name in name_list:
    file_path = "./合约/{0}.xlsx".format(name)
    if name in exception_list:
        detailed_data = pd.read_excel(file_path)
        detailed_data = detailed_data.set_index(keys='Unnamed: 0')
        detailed_data.columns = [name]
        close_adjusted = pd.concat([close_adjusted, detailed_data], axis=1, join='outer')
    else:
        series, drop_dates = adjusted_close_px(name)
        close_adjusted = pd.concat([close_adjusted, series], axis=1, join='outer')
    print("已完成{0}".format(name))

close_adjusted.to_excel("close_adjusted.xlsx")

# 将库存数据非交易日剔除，并且保留有收盘交易日的数据
data_close = pd.read_excel('./close_adjusted.xlsx', index_col=0)
data_stock = pd.read_excel('./stock_adjusted.xlsx', index_col=0)

data_stock_columns = list(data_stock.columns)
data_close_columns = list(data_close.columns)

data_close_list = range(len(data_close.columns))
data_stock_list = data_close_list
for j in range(len(data_close.columns)):  # 收盘价数据附在库存数据后面，index日期会自动对齐
    data_stock[data_close_list[j]] = data_close.iloc[:, j]

for k in range(len(data_close.columns)):
    for i in range(len(data_stock)):
        if np.isnan(data_stock.iloc[i, 24+k]):  # 各品种没有交易日的nan赋值给库存数据
            data_stock.iloc[i, k] = data_stock.iloc[i, 24+k]

for k in range(len(data_close.columns)):  # 去除掉nan值的行，相当于去除了非交易日期
    data_close[data_close_list[k]] = data_stock.iloc[:, k].dropna()

data_stock_adjusted = data_close.drop(data_close_columns, axis=1)
data_stock_adjusted.columns = data_stock_columns
data_stock_adjusted.to_excel("./stock_adjusted.xlsx")
