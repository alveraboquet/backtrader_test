import sys
import pandas as pd
from datetime import datetime
import pytz
import yfinance as yf
import time
from tqdm import tqdm
from os.path import exists
import pickle

로그파일명 = f"S11_log.txt"
데이터저장폴더 = f"csv"
모든심볼데이터파일명 = f"{데이터저장폴더}/alldata.pkl"
printLog = False


# write list to binary file
def write_list(a_list, path):
    # store list in binary file so 'wb' mode
    with open(path, 'wb') as fp:
        pickle.dump(a_list, fp)
        print('Done writing list into a binary file')


# Read list to memory
def read_list(path):
    # for reading also binary mode is important
    with open(path, 'rb') as fp:
        n_list = pickle.load(fp)
        return n_list


def load_한국투자증권티커목록():
    cn = ['ncod', 'exid', 'excd', 'exnm', 'symb', 'rsym', 'knam', 'enam', 'stis', 'curr', 'zdiv', 'ztyp', 'base', 'bnit'
        , 'anit', 'mstm', 'metm', 'isdr', 'drcd', 'icod', 'sjong', 'ttyp', 'etyp', 'ttyp_sb']
    dfnas = pd.read_csv('symbolfile/NASMST.COD', sep='\t', encoding='cp949', header=None, names=cn, dtype={'symb':str}, keep_default_na=False, na_values=['NaN'])
    dfams = pd.read_csv('symbolfile/AMSMST.COD', sep='\t', encoding='cp949', header=None, names=cn, dtype={'symb':str}, keep_default_na=False, na_values=['NaN'])
    dfnys = pd.read_csv('symbolfile/NYSMST.COD', sep='\t', encoding='cp949', header=None, names=cn, dtype={'symb':str}, keep_default_na=False, na_values=['NaN'])

    df = pd.concat([dfnas, dfnys, dfams])

    df_idx = df[df['stis'] == 1]
    df_stock = df[df['stis'] == 2]
    df_etf = df[df['stis'] == 3]
    df_warrant = df[df['stis'] == 4]

    #df_stock_over5 = df_stock[df_stock['base'] >= 5]

    return df_stock


def downloadDataAndSaveToCsv(stocklist, fromdate, todate, rootpath):
    cnt = 0
    for idx, row in tqdm(stocklist.iterrows(), total=len(stocklist)):
        if cnt < 1:
            # symbolname = '^GSPC'
            symbolname = 'SPY'
        else:
            symbolname = row['symb']
        fn = f"{rootpath}/{symbolname}.csv"

        cnt += 1

        try:
            if not exists(fn):
                df = yf.download(symbolname, start=fromdate, end=todate, auto_adjust=False)
                if len(df) < 1:
                    print(f"{symbolname} 데이터 길이가 0이라서 제외")
                    continue
                '''if row['symb']=="AQUNU":
                    k = 0'''
                if df.iloc[len(df) - 1].isnull().values.any():
                    print(f"[{symbolname}] 데이터에 nan이 포함되어 있어 제외")
                    continue
                df.to_csv(fn)
        except Exception as e:
            continue

        time.sleep(0.1)
        '''if cnt>50:
            break'''

def downloadDataAndSaveToPickle(stocklist, fromdate, todate, rootpath):
    cnt = 0
    for idx, row in tqdm(stocklist.iterrows(), total=len(stocklist)):
        if cnt < 1:
            # symbolname = '^GSPC'
            symbolname = 'SPY'
        else:
            symbolname = row['symb']
        fn = f"{rootpath}/{symbolname}.pkl"

        cnt += 1

        try:
            if not exists(fn):
                print(f"[{symbolname}] 다운로드 ...")
                df = yf.download(symbolname, start=fromdate, end=todate, auto_adjust=False)
                if len(df) < 1:
                    print(f"{symbolname} 데이터 길이가 0이라서 제외")
                    continue
                '''if row['symb']=="AQUNU":
                    k = 0'''
                if df.iloc[len(df) - 1].isnull().values.any():
                    print(f"[{symbolname}] 데이터에 nan이 포함되어 있어 제외")
                    continue
                df.to_pickle(fn)
        except Exception as e:
            continue

        time.sleep(0.1)
        '''if cnt>50:
            break'''


def loadTickerDataToSinglePickle(stocklist, path):
    cnt = 0
    alldata = {}
    for idx, row in tqdm(stocklist.iterrows(), total=len(stocklist)):
        df = None
        if cnt < 1:
            # symbolname = '^GSPC'
            symbolname = 'SPY'
        else:
            symbolname = row['symb']

        try:
            symbolfilepath = f"{데이터저장폴더}/{symbolname}.pkl"
            if exists(symbolfilepath):
                df = pd.read_pickle(symbolfilepath)
                alldata[symbolname] = df
        except Exception as e:
            continue

        cnt += 1

    if len(alldata) > 0:
        write_list(alldata, path=path)


if __name__ == '__main__':
    cur_date = datetime.now(pytz.timezone('America/New_York'))
    fromdate = "1990-01-01"
    todate = '2022-09-01'
    #todate = cur_date.strftime('%Y-%m-%d')

    stocklist = load_한국투자증권티커목록()
    downloadDataAndSaveToCsv(stocklist, fromdate, todate, 데이터저장폴더)
    #loadTickerDataToSinglePickle(stocklist, 모든심볼데이터파일명)
