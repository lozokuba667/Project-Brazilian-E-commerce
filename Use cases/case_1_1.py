from pyspark import SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from IPython.display import display, HTML
spark = SparkSession \
    .builder \
    .getOrCreate()

orders = spark.read.option("header", "true") \
                   .option("inferSchema", "True") \
                   .csv("../notebooks/ecommerce_data/olist_orders_dataset.csv")
customers = spark.read.option("header","true") \
                   .option("inferSchema", "True") \
                   .csv("../notebooks/ecommerce_data/olist_customers_dataset.csv")
orders = orders.withColumn("order_approved_at", F.to_timestamp(F.col("order_approved_at"))) \
               .withColumn("order_delivered_carrier_date", F.to_timestamp(F.col("order_delivered_carrier_date"))) \
               .withColumn("order_delivered_customer_date", F.to_timestamp(F.col("order_delivered_customer_date"))) \
               .withColumn("order_estimated_delivery_date", F.to_timestamp(F.col("order_estimated_delivery_date"))) 

orders_delivered = orders.where((F.col("order_delivered_customer_date").isNotNull())) \
                       .withColumn("delayed [h]", (F.col("order_delivered_customer_date").cast("bigint") - F.col("order_estimated_delivery_date").cast("bigint")) / 3600) \
                       .select("order_id", "customer_id", F.round(F.col("delayed [h]"),2).alias("delayed [h]"), 
                               F.month("order_delivered_customer_date").alias("month"),
                               F.year("order_delivered_customer_date").alias("year"))
number_of_delivered_orders = orders_delivered.count()

orders_delayed = orders_delivered.where(F.col("delayed [h]") > 0) \
                                 .select("order_id", "delayed [h]", "customer_id").orderBy(F.col("delayed [h]").desc())

orders_delivered_geo = orders_delivered.join(customers, ["customer_id"]) \
                                      .select("customer_unique_id","customer_id", "order_id", "delayed [h]","customer_state", "customer_city", "month", "year")
orders_delayed = orders_delivered_geo.where(F.col("delayed [h]") > 0) \
                                 .select("order_id", "delayed [h]", "customer_id", "customer_unique_id").orderBy(F.col("delayed [h]").desc())
customers_with_more_delays = orders_delayed.groupBy("customer_unique_id") \
                          .agg(F.count("customer_id").alias("number_of_delays")) \
                          .where(F.col("number_of_delays") > 1) \
                          .orderBy(F.col("number_of_delays").desc())
orders_delayed = orders_delayed.drop("customer_id","customer_unique_id")
number_of_delayed_orders = orders_delayed.count()

states = spark.read.option("header", "true") \
                   .option("inferSchema", "True") \
                   .csv("../notebooks/ecommerce_data/states_name.csv")
orders_delivered_geo = orders_delivered_geo.join(states, ['customer_state']).drop('customer_state').withColumnRenamed("state_name", "customer_state")

avg_delay_by_state = orders_delivered_geo.where(F.col("delayed [h]") > 0) \
                    .groupBy("customer_state") \
                   .agg(F.round(F.mean("delayed [h]"),2).alias("average_delay_[h]"),
                        F.count("delayed [h]").alias("number_of_delays")) \
                   .orderBy(F.col("number_of_delays").desc())

avg_order_by_state = orders_delivered_geo.groupBy("customer_state") \
                   .agg(F.round(F.mean("delayed [h]"),2).alias("delivered_average_delay_[h]"),
                        F.count("delayed [h]").alias("number_of_orders")) \
                   .orderBy(F.col("number_of_orders").desc())

avg_delay_by_state_percent = avg_delay_by_state.join(avg_order_by_state, ['customer_state']) \
                   .withColumn("delays_%", F.round(F.col("number_of_delays") / F.col("number_of_orders")*100, 2))

import plotly.express as px

df_avg_delay_by_state_percent = avg_delay_by_state_percent.select("customer_state", "delays_%").toPandas()
fig = px.bar(df_avg_delay_by_state_percent, x='customer_state', y='delays_%'  )
fig.update_xaxes(title_text='State')
fig.update_yaxes(title_text='% of delayed orders')
percentage_delay_by_state = fig.to_html()

#% zamówień zamówień opóźnonych ogólnie
df_avg_delay_by_state_h = avg_delay_by_state_percent.select("customer_state", "delivered_average_delay_[h]").toPandas()
fig = px.bar(df_avg_delay_by_state_h, x='customer_state', y='delivered_average_delay_[h]')
fig.update_xaxes(title_text='State')
fig.update_yaxes(title_text='Average delay time [h]')
delay_rate_in_states = fig.to_html()

import pandas as pd
d = {"value": [number_of_delayed_orders, number_of_delivered_orders - number_of_delayed_orders], 
     "state": ["Delayed", "On time"]}
df = pd.DataFrame(data = d)
fig = px.pie(df, values = 'value', names='state')
delay_rate = fig.to_html()

base_html = """
<!doctype html>
<html>
    <head >
        <meta http-equiv="Content-type" content="text/html; charset=utf-8">
        <script type="text/javascript" src="https://ajax.googleapis.com/ajax/libs/jquery/2.2.2/jquery.min.js"></script>
        <link rel="stylesheet" type="text/css" href="https://cdn.datatables.net/1.10.16/css/jquery.dataTables.css">
        <script type="text/javascript" src="https://cdn.datatables.net/1.10.16/js/jquery.dataTables.js"></script>
    </head>
    <body style='white-space:nowrap;'>%s
        <script type="text/javascript">$(document).ready(function(){$('table').DataTable({
            "pageLength": 10,
            "retrieve": true
        });});
        </script>
    </body>
</html>
"""

df_customers_with_more_delays = base_html % customers_with_more_delays.withColumnRenamed("customer_unique_id", "Customer") \
                                                                      .withColumnRenamed("number_of_delays", "Number of delayed orders") \
                                                                      .toPandas().to_html()
df_orders_delayed = base_html % orders_delayed.withColumnRenamed("delayed [h]", "Delay time [h]").toPandas().to_html()

html_string = '''
<!doctype html>
<html>
    <head>
        <meta charset="UTF-8">
        <title>Dashboard</title>
        <style>body{ margin:0 100; background:whitesmoke; }
              .row {display: flex;}
              .column {flex: 33.33%; padding: 5px;} 
        </style>
    </head>
    
    <body>
        <h1 style='text-align: center;'>Case 1</h1>      
        <!-- *** Section 1 *** --->
        <div class="row"> 
        
        <div class="column">
            <h2>Customers whose package was delayed</h2>
            ''' + df_orders_delayed + '''
        </div>
         <div class="column">
            <h2>Delay rate of packages</h2>
            ''' + delay_rate + '''
        </div>  
    </div>
     <!-- *** Section 2 *** --->
    <h2>Customers whose package was delayed more than once</h2>
        ''' + df_customers_with_more_delays + '''
        <!-- *** Section 3 *** --->
        <h2>Average delay per state</h2>
        ''' + delay_rate_in_states + '''
        <p>Average delay by state,  the value below 0 means that order came before estimated delivery date.  </p>
        <!-- *** Section 4 *** --->
    <h2>Propotion of delayed orders by states</h2>
            ''' + percentage_delay_by_state + ''' 
            
    </body>
</html>'''

with open('report.html', 'w', encoding = 'utf8') as f:
    f.write(html_string)