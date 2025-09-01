import pandas as pd


def extraction(engine):
    customers =  pd.read_sql("SELECT * FROM customer;", engine)
    orders =  pd.read_sql("SELECT * FROM orders;", engine)
    order_items =  pd.read_sql("SELECT * FROM order_items;", engine)
    products =  pd.read_sql("SELECT * FROM products;", engine)

    return (customers, orders, order_items, products)