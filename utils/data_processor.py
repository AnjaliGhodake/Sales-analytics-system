def calculate_total_revenue(transactions):
    """
    Calculates total revenue from all transactions
    """

    total_revenue = 0.0

    for tx in transactions:
        total_revenue += tx['Quantity'] * tx['UnitPrice']

    return round(total_revenue, 2)


def region_wise_sales(transactions):
    """
    Analyzes sales by region
    """

    region_data = defaultdict(lambda: {
        'total_sales': 0.0,
        'transaction_count': 0
    })

    total_sales_overall = 0.0

    for tx in transactions:
        revenue = tx['Quantity'] * tx['UnitPrice']
        region = tx['Region']

        region_data[region]['total_sales'] += revenue
        region_data[region]['transaction_count'] += 1
        total_sales_overall += revenue

    result = {}

    for region, data in region_data.items():
        percentage = (data['total_sales'] / total_sales_overall) * 100 if total_sales_overall else 0
        result[region] = {
            'total_sales': round(data['total_sales'], 2),
            'transaction_count': data['transaction_count'],
            'percentage': round(percentage, 2)
        }

    # Sort


def top_selling_products(transactions, n=5):
    """
    Finds top n products by total quantity sold
    """

    product_data = defaultdict(lambda: {
        'quantity': 0,
        'revenue': 0.0
    })

    for tx in transactions:
        product = tx['ProductName']
        qty = tx['Quantity']
        revenue = qty * tx['UnitPrice']

        product_data[product]['quantity'] += qty
        product_data[product]['revenue'] += revenue

    product_list = [
        (product, data['quantity'], round(data['revenue'], 2))
        for product, data in product_data.items()
    ]

    # Sort by total quantity descending
    product_list.sort(key=lambda x: x[1], reverse=True)

    return product_list[:n]


def customer_analysis(transactions):
    """
    Analyzes customer purchase patterns
    """

    customer_data = defaultdict(lambda: {
        'total_spent': 0.0,
        'purchase_count': 0,
        'products_bought': set()
    })

    for tx in transactions:
        customer = tx['CustomerID']
        amount = tx['Quantity'] * tx['UnitPrice']

        customer_data[customer]['total_spent'] += amount
        customer_data[customer]['purchase_count'] += 1
        customer_data[customer]['products_bought'].add(tx['ProductName'])

    result = {}

    for customer, data in customer_data.items():
        avg_order_value = (
            data['total_spent'] / data['purchase_count']
            if data['purchase_count'] else 0
        )

        result[customer] = {
            'total_spent': round(data['total_spent'], 2),
            'purchase_count': data['purchase_count'],
            'avg_order_value': round(avg_order_value, 2),
            'products_bought': sorted(list(data['products_bought']))
        }

    # Sort by total_spent descending
    sorted_result = dict(
        sorted(result.items(), key=lambda x: x[1]['total_spent'], reverse=True)
    )

    return sorted_result


def daily_sales_trend(transactions):
    """
    Analyzes sales trends by date
    """

    daily_data = defaultdict(lambda: {
        'revenue': 0.0,
        'transaction_count': 0,
        'unique_customers': set()
    })

    for tx in transactions:
        date = tx['Date']
        revenue = tx['Quantity'] * tx['UnitPrice']

        daily_data[date]['revenue'] += revenue
        daily_data[date]['transaction_count'] += 1
        daily_data[date]['unique_customers'].add(tx['CustomerID'])

    result = {}

    for date in sorted(daily_data.keys(), key=lambda d: datetime.strptime(d, "%Y-%m-%d")):
        result[date] = {
            'revenue': round(daily_data[date]['revenue'], 2),
            'transaction_count': daily_data[date]['transaction_count'],
            'unique_customers': len(daily_data[date]['unique_customers'])
        }

    return result


def find_peak_sales_day(transactions):
    """
    Identifies the date with highest revenue
    """

    daily_summary = daily_sales_trend(transactions)

    peak_date = None
    peak_revenue = 0.0
    peak_tx_count = 0

    for date, data in daily_summary.items():
        if data['revenue'] > peak_revenue:
            peak_revenue = data['revenue']
            peak_date = date
            peak_tx_count = data['transaction_count']

    return peak_date, peak_revenue, peak_tx_count


def low_performing_products(transactions, threshold=10):
    """
    Identifies products with low sales
    """

    product_data = defaultdict(lambda: {
        'quantity': 0,
        'revenue': 0.0
    })

    for tx in transactions:
        product = tx['ProductName']
        qty = tx['Quantity']
        revenue = qty * tx['UnitPrice']

        product_data[product]['quantity'] += qty
        product_data[product]['revenue'] += revenue

    low_products = [
        (product, data['quantity'], round(data['revenue'], 2))
        for product, data in product_data.items()
        if data['quantity'] < threshold
    ]

    # Sort by total quantity ascending
    low_products.sort(key=lambda x: x[1])

    return low_products
