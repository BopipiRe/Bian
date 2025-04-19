# 在脚本开头添加环境检查
import os
import asyncio
from datetime import datetime, timedelta

import aiohttp
import pandas as pd
import requests

async def push_wechat(msg):
    # 从GitHub Secrets获取token
    token = os.getenv('PUSHPLUS_TOKEN')
    url = f"https://www.pushplus.plus/send?token={token}&title=振幅提醒&content={msg}"
    requests.get(url)


async def get_all_futures_symbols(session):
    """异步获取所有USDT合约交易对"""
    url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    async with session.get(url) as response:
        data = await response.json()
        symbols = [s['symbol'] for s in data['symbols'] if 'USDT' in s['symbol']]
        return symbols


async def get_closed_5m_kline(session, symbol):
    """异步获取最近一根已闭合的5分钟K线"""
    url = "https://fapi.binance.com/fapi/v1/klines"
    params = {
        'symbol': symbol,
        'interval': '5m',
        'limit': 2  # 获取最近2根K线
    }
    async with session.get(url, params=params) as response:
        data = await response.json()
        if len(data) < 2:
            return None

        latest_kline = data[-1]
        prev_kline = data[-2]

        # 获取K线收盘时间的分钟数（UTC时间）
        latest_close_time = pd.to_datetime(latest_kline[6], unit='ms')
        prev_close_time = pd.to_datetime(prev_kline[6], unit='ms')

        # 获取当前时间的分钟数（UTC时间）
        current_time = datetime.utcnow()

        # 仅当当前分钟等于收盘时间的分钟时才返回数据
        if current_time < latest_close_time:
            if current_time < prev_close_time:
                return None
            else:
                res_kline = prev_kline
        else:
            res_kline = latest_kline
        open_price = float(res_kline[1])
        high_price = float(res_kline[2])
        low_price = float(res_kline[3])
        amplitude = (high_price - low_price) / open_price * 100

        return {
            'symbol': symbol,
            'open_time': pd.to_datetime(res_kline[0], unit='ms'),
            'close_time': pd.to_datetime(res_kline[6], unit='ms'),
            'open': open_price,
            'high': high_price,
            'low': low_price,
            'close': float(res_kline[4]),
            'amplitude': amplitude,
            'volume': float(res_kline[5])
        }


async def scan_symbol(session, symbol, results):
    """异步扫描单个交易对"""
    try:
        kline = await get_closed_5m_kline(session, symbol)
        if kline and kline['amplitude'] >= 2:
            results.append(kline)
    except Exception as e:
        print(f"Error processing {symbol}: {str(e)}")


async def scan_high_amplitude_contracts():
    """并发扫描所有合约"""
    async with aiohttp.ClientSession() as session:
        symbols = await get_all_futures_symbols(session)
        results = []
        tasks = [scan_symbol(session, symbol, results) for symbol in symbols]
        await asyncio.gather(*tasks)  # 并发执行所有任务

        if not results:
            print("未找到符合条件的合约")
            return None

        df = pd.DataFrame(results).sort_values('amplitude', ascending=False)
        return df


# 修改主程序部分
async def main():
    while True:
        try:
            now = datetime.now()
            print(f"开始扫描，时间: {now}")
            
            pd.set_option('display.max_columns', None)
            pd.set_option('display.float_format', '{:.4f}'.format)
            
            high_amp_klines = await scan_high_amplitude_contracts()
            
            if high_amp_klines is not None:
                print(high_amp_klines[['symbol', 'open_time', 'high', 'low', 'amplitude']])
                symbols = high_amp_klines['symbol'].tolist()
                if symbols:
                    msg = f"发现高振幅交易对:{', '.join(symbols)}"
                    print(msg)
                    await push_wechat(msg)
            
            # 在GitHub Actions中不需要无限循环，每次运行会启动新实例
            break
                
        except Exception as e:
            print(f"发生错误: {str(e)}")
            await asyncio.sleep(60)


if __name__ == "__main__":
    asyncio.run(main())
