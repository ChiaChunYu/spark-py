from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("SalesAnalysis").getOrCreate()

sales_data = spark.read.text("b_sales.txt")
prices_data = spark.read.text("b_price.txt")

# Parse sales records data
sales_parsed = sales_data.rdd.map(lambda line: line.value.split())

# Parse item prices data
prices_parsed = prices_data.rdd.map(lambda line: line.value.split())

# Create RDD with (ItemID, price) pairs
item_prices = prices_parsed.map(lambda x: (x[0], float(x[1])))

# Broadcast item prices to all nodes
broadcast_item_prices = spark.sparkContext.broadcast(item_prices.collectAsMap())

# Function to calculate total sales for each store and total number sold for each item
def calculate_totals(record):
    store_id = record[0]
    sales = record[1:]

    total_sales_store = 0
    item_totals = {}

    for i in range(0, len(sales), 2):
        item_id = sales[i]
        quantity_sold = int(sales[i + 1])
        item_price = broadcast_item_prices.value.get(item_id, 0)

        # Calculate total sales for each store
        total_sales_store += quantity_sold * item_price

        # Calculate total number sold for each item
        if item_id in item_totals:
            item_totals[item_id] += quantity_sold
        else:
            item_totals[item_id] = quantity_sold

    return store_id, total_sales_store, item_totals

# Apply the function to calculate totals
totals = sales_parsed.map(calculate_totals)

# Collect the results to the driver
result = totals.collect()

# Calculate average total sales
total_sales_sum = sum([record[1] for record in result])
num_stores = len(result)
average_total_sales = total_sales_sum / num_stores

# Calculate grand total sales
grand_total_sales = total_sales_sum

# Print the results
print("Total Sales for Each Store:")
for record in result:
    print(f"StoreID: {record[0]}, Total Sales: {record[1]}")

print("\nTotal Number Sold for Each Item:")
for record in result:
    print(f"StoreID: {record[0]}, Item Totals: {record[2]}")

print(f"\nAverage Total Sales: {average_total_sales}")
print(f"Grand Total Sales of All Stores: {grand_total_sales}")

spark.stop()
