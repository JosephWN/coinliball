from coinlib.trade.pubnubstreamapi import PubNubStreamApi


class StreamApi(PubNubStreamApi):
    """
    ticker: ticker_{pair}
    order_book: depth_{pair}
    execution: transactions_{pair}
    candle: candlestick_{pair}
    """
    PN_KEY = 'sub-c-e12e9174-dd60-11e6-806b-02ee2ddab7fe'
