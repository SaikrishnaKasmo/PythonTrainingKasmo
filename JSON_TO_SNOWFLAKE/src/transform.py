import pandas as pd 

def transform(product_dimension,sales_dimension,store_dimension,time_dimension,sales_fact):
    #product_dimension Transformations
    product_dimension["price"] = product_dimension["price"].round(2)

    #sales_dimension Transformations
    sales_dimension["start_date"] = pd.to_datetime(sales_dimension["start_date"])
    sales_dimension["end_date"] = pd.to_datetime(sales_dimension["end_date"])

    #sales_fact Transformations
    sales_fact["discount_applied"] = sales_fact["discount_applied"].round(2)
    sales_fact["tax_amount"] = sales_fact["tax_amount"].round(2)
    sales_fact["net_amount"] = sales_fact["net_amount"].round(2)
    sales_fact["total_amount"] = sales_fact["total_amount"].round(2)
    # sales_fact["customer_id"] = sales_fact["customer_id"].astype(str)
    # sales_fact["payment_method"] = sales_fact["payment_method"].astype(str)
    # sales_fact["transaction_type"] = sales_fact["transaction_type"].astype(str)

 
    #store_dimension
    store_dimension["opening_date"] = pd.to_datetime(store_dimension["opening_date"])

    #time_dimension
    time_dimension["date"] = pd.to_datetime(time_dimension["date"])

    return [product_dimension,sales_dimension,store_dimension,time_dimension,sales_fact]
