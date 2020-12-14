
#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys

def toFloat_Or_0(s):
    try:
        return float(s)
    except:
        return 0

def ToInt_Or_0(s):
    try:
        return int(s)
    except:
        return 0

def float2str(s):
    if s >= sys.maxsize:
        return ''
    else:
        return '{:.3f}'.format(s)

def int2str(s):
    return str(s)

g_init_HOST = ''
g_orders = {}  # (OrderId, side): (qty, price)
g_priceQty_bid = {} # {price:quantity}
g_priceQty_ask = {} # {price:quantity}
g_bid_price = 0
g_ask_price = sys.maxsize
g_bid_size = 0
g_ask_size = 0
max_orders = 0 # for performance metrics
def upsertPxQty_bid(price, delta): 
    """ Update or Insert bid price-quantity dictionary
    delta is the quantity to change
    """

    #global g_init_HOST
    #global g_inited
    #global g_orders
    global g_priceQty_bid
    #global g_priceQty_ask
    global g_bid_price
    #global g_ask_price
    global g_bid_size
    #global g_ask_size

    if price in g_priceQty_bid:  # existing price
        qty = g_priceQty_bid[price]
        if qty + delta == 0:  # price was closed
            del g_priceQty_bid[price]

            if len(g_priceQty_bid) > 0:
                g_bid_price = max(g_priceQty_bid.keys())
                g_bid_size = g_priceQty_bid[g_bid_price]
            else: # no bids
                g_bid_price = 0
                g_bid_size = 0
        else:
            g_priceQty_bid[price] = qty + delta
            if price >= g_bid_price:
                g_bid_price = price
                g_bid_size = qty + delta
    else: # new price
        g_priceQty_bid[price] = delta

        if price > g_bid_price:  # higher bid price
            g_bid_price = price
            g_bid_size = max(delta, 0)
    return

def upsertPxQty_ask(price, delta):  
    """ Update or Insert ask price-quantity dictionary
    delta is the quantity to change
    """

    #global g_init_HOST
    #global g_inited
    #global g_orders
    #global g_priceQty_bid
    global g_priceQty_ask
    #global g_bid_price
    global g_ask_price
    #global g_bid_size
    global g_ask_size

    if price in g_priceQty_ask:  # existing price
        qty = g_priceQty_ask[price]
        if qty + delta == 0:  # price was closed
            del g_priceQty_ask[price]

            if len(g_priceQty_ask) > 0:
                g_ask_price = min(g_priceQty_ask.keys())
                g_ask_size = g_priceQty_ask[g_ask_price]
            else:  # no asks
                g_ask_price = sys.maxsize
                g_ask_size = 0
        else:
            g_priceQty_ask[price] = qty + delta
            if price <= g_ask_price:
                g_ask_price = price
                g_ask_size = qty + delta
    else:        # new price
        g_priceQty_ask[price] = delta

        if price < g_ask_price:  # lower ask price
            g_ask_price = price
            g_ask_size = max(delta, 0)
    return

def delOrd(OrderId, side):  
    """ To delete an order from the order book
    There are 2 situations:
      1.  An order is cancelled 
      2.  An order is filled
    """
    
    global g_init_HOST
    global g_inited
    global g_orders
    #global g_priceQty_bid
    #global g_priceQty_ask
    # global g_bid_price
    # global g_ask_price
    # global g_bid_size
    # global g_ask_size

    if not (OrderId, side) in g_orders:
        return

    (qty, px) = g_orders[(OrderId, side)]

    del g_orders[(OrderId, side)]

    if side == 'BUY':
        upsertPxQty_bid(px, -qty)
    else:
        upsertPxQty_ask(px, -qty)
          
    return

def upsertOrd(OrderId,side,quantity,price):
    """ To update or insert an order
    There are 2 situations:
      1.  An order is added 
      2.  An order is updated
    """

    global g_init_HOST
    global g_inited
    global g_orders
    global g_priceQty_bid
    global g_priceQty_ask

  # global g_bid_price
  # global g_ask_price
  # global g_bid_size
  # global g_ask_size

    delta = 0  #   quantity change
    if (OrderId, side) in g_orders:
        (qty, px) = g_orders[(OrderId, side)]
        delta = quantity - qty
    else:
        delta = quantity

    if side == 'BUY':
        upsertPxQty_bid(price, delta)
    else:
        upsertPxQty_ask(price, delta)

    g_orders[(OrderId, side)] = (quantity, price)

def tradeOrd(OrderId, side, quantity):
    """ To fill an order
    We will ignore the fill of market orders
    """

    global g_init_HOST
    global g_inited
    global g_orders
    global g_priceQty_bid
    global g_priceQty_ask

  # global g_bid_price
  # global g_ask_price
  # global g_bid_size
  # global g_ask_size

    if (OrderId, side) in g_orders:
        (qty, px) = g_orders[(OrderId, side)]
        qty = qty - quantity  # Trade instead of add order
        if quantity == 0:  #  0
            delOrd(OrderId, side)
            return   # in case duplicate process
        else:
            g_orders[(OrderId, side)] = (qty, px)
            if side == 'BUY':
                upsertPxQty_bid(px, -quantity)
            else:
                upsertPxQty_ask(px, -quantity)
            return
    else:
        return   # for market orders
def processOneMsg(element):
    """ To process one streaming message or a row of record
    There are some special situations:
    1. To initialize the order book
    2. To ignore market orders
    """

    global g_init_HOST
    global g_inited
    global g_orders
    #global g_priceQty_bid
    #global g_priceQty_ask
    global g_bid_price
    global g_ask_price
    global g_bid_size
    global g_ask_size

    if (type(element) != str):
        element=element.decode('utf-8')
    
    if element.startswith("HOST"): # To deal with the CSV header
      return ["time,bid_price,ask_price,bid_size,ask_size,seq_num"]

    (HOST,
        seq_num,
        is_image,
        add_orderid,
        add_side,
        add_price,
        add_qty,
        add_position,
        update_orderid,
        update_side,
        update_price,
        update_qty,
        update_position,
        delete_orderid,
        delete_side,
        trade_orderid,
        trade_side,
        trade_qty,
        trade_price,) = element.split(',')

    HOST = HOST
    seq_num = seq_num
    is_image = bool(is_image)
    add_orderid = add_orderid
    add_side = add_side
    add_price = toFloat_Or_0(add_price)
    add_qty = ToInt_Or_0(add_qty)
    add_position = ToInt_Or_0(add_position)
    update_orderid = update_orderid
    update_side = update_side
    update_price = toFloat_Or_0(update_price)
    update_qty = ToInt_Or_0(update_qty)
    update_position = ToInt_Or_0(update_position)
    delete_orderid = delete_orderid
    delete_side = delete_side
    trade_orderid = trade_orderid
    trade_side = trade_side
    trade_qty = ToInt_Or_0(trade_qty)
    trade_price = toFloat_Or_0(trade_price)

    if is_image:
        if HOST != g_init_HOST:  
            # To initialize the order book
            g_init_HOST = HOST
            g_orders = {}
            g_bid_price = 0
            g_ask_price = sys.maxsize
            g_bid_size = 0
            g_ask_size = 0

    if delete_orderid != '':  # Process DELETE
        delOrd(delete_orderid, delete_side)
    elif add_orderid != '': # Process ADD
        if add_price > 0 and add_price < 1e+308:
            upsertOrd(add_orderid, add_side, add_qty, add_price)
    elif update_orderid != '': # Process UPDATE
        if update_price > 0 and update_price < 1e+308:
            upsertOrd(update_orderid, update_side, update_qty, update_price)
    elif trade_orderid != '': # Process TRADE
        tradeOrd(trade_orderid, trade_side, trade_qty)

    global max_orders
    max_orders = (len(g_orders) if len(g_orders) > max_orders else 0) 

    result = [HOST,
        float2str(g_bid_price),
        float2str(g_ask_price),
        int2str(g_bid_size),
        int2str(g_ask_size),
        seq_num,]

    return [','.join(result) ] #For some environment, we have to add '\n'


import apache_beam as beam

class OneRowParDo(beam.DoFn):
    def process(self, element):
      return processOneMsg(element)

streamingMode=True #
pubSubTopic="projects/grasshopper-298307/topics/L3toL1"

def run_pipeline():
    if streamingMode:
        options = PipelineOptions(args, save_main_session=True, streaming=True)
    else:
        options = beam.options.pipeline_options.PipelineOptions(streaming=False)
    p = beam.Pipeline(options=options)

    if streamingMode:
      read = (
          p 
          | 'Read from PubSub '>> beam.io.ReadFromPubSub(topic=pubSubTopic)
      )
    else:
      read = (
          p 
          | 'Reads from csv' >> beam.io.ReadFromText('market_data_v2.csv') 
      )
    process= (
        read 
        | 'Structures data' >> beam.ParDo(OneRowParDo())
        | 'Save L1 data into a file' >> beam.io.WriteToText('L1_market_data',file_name_suffix=".csv")
    )
    
    result = p.run()
    result.wait_until_finish()  # For it to hold the terminal until it finishes

run_pipeline()

