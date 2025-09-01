import pandas as pd
import numpy as np

def transform(customer,orders,order_items,products):

    #Transformation 1 : Join Orders and order_items
    sales = order_items.merge(orders,on='order_id',how="inner")

    #Transformation 2: #Compute Line Total
    sales['line_total'] = sales['quantity'] * sales['price']

    #Transformation 3: Filter Only Completed Orders
    sales = sales[sales["status"]=="COMPLETE"]

    #Transformation 4: Add Discount Column (Business Rule)
    sales["discount"] = np.where(sales["quantity"] >=5,0.10* sales["line_total"],0)
    sales["net_total"] = sales["line_total"] - (sales["discount"])

    # Transformation 5. Derive Order Month & Year
    sales['order_date'] = pd.to_datetime(sales['order_date'])
    sales['order_year'] = sales['order_date'].dt.year
    sales['order_month'] = sales['order_date'].dt.month

    # Transformation 6: Compute Aggreation at order level
    order_summary = sales.groupby('order_id').agg(
        order_total = ('line_total','sum'),
        total_quantity = ('quantity','sum'),
        customer_id = ('customer_id','first'),
        order_year = ('order_year','first'),
        order_month = ('order_month','first')
    ).reset_index()

    # Transformation 7: Customer Region Join
    order_summary = order_summary.merge(customer[["region","customer_id"]],on="customer_id",how= "left")

    # Transformation 8: Sales by Region + Month (Aggregation)
    sales_region = order_summary.groupby(['region','order_year','order_month']).agg(
        Total_revenue=('order_total','sum'),
        Total_quantity = ('total_quantity','first')
    ).reset_index()

    #Transformation 9: Category-wise Analysis (Enrichment)
    sales_with_products = sales.merge(products[["product_id","category"]],on="product_id",how="left")
    category_sales = sales_with_products.groupby("category").agg(
        category_revenue = ("net_total","sum"),
        category_order_count = ('order_id','sum')
    ).reset_index()

    # Transformation 10: Ranking Transformation
    region_sales_ranked = sales_region.copy()
    region_sales_ranked["Rank"] = region_sales_ranked.groupby(["order_month","order_year"])['Total_revenue'].rank(method='dense',ascending=False)

    #Transformation 11: Sorting values
    region_sales_ranked.sort_values(by=['order_year','order_month','Total_revenue'],ascending= [True,True,False])

    #Transformation 12: Outlier Flagging 
    threshold = order_summary['order_total'].quantile(0.95)
    order_summary['outlier_flag'] = np.where(order_summary['order_total'] > threshold, 1, 0)


    # Example transformation: Convert all column names to lowercase
    customer.columns = [col.lower() for col in customer.columns]
    orders.columns = [col.lower() for col in orders.columns]
    order_items.columns = [col.lower() for col in order_items.columns]
    products.columns = [col.lower() for col in products.columns]
    

    # - sales: line-level sales
# - order_summary: order-level aggregates
# - region_month_summary: regional monthly performance
# - category_summary: category-wise revenue & orders
    return (sales, order_summary, region_sales_ranked, category_sales)
