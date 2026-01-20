import requests
import os

def fetch_all_products():
    """
    Fetches all products from DummyJSON API
    """

    url = "https://dummyjson.com/products?limit=100"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        data = response.json()
        products = data.get("products", [])

        result = []
        for p in products:
            result.append({
                'id': p.get('id'),
                'title': p.get('title'),
                'category': p.get('category'),
                'brand': p.get('brand'),
                'price': p.get('price'),
                'rating': p.get('rating')
            })

        print(f"✅ Successfully fetched {len(result)} products from API")
        return result

    except requests.exceptions.RequestException as e:
        print(f"❌ API request failed: {e}")
        return []


def create_product_mapping(api_products):
    """
    Creates a mapping of product IDs to product info
    """

    product_mapping = {}

    for product in api_products:
        product_id = product.get('id')

        if product_id is not None:
            product_mapping[product_id] = {
                'title': product.get('title'),
                'category': product.get('category'),
                'brand': product.get('brand'),
                'rating': product.get('rating')
            }

    return product_mapping


def enrich_sales_data(transactions, product_mapping):
    """
    Enriches transaction data with API product information
    """

    enriched_transactions = []

    for tx in transactions:
        enriched_tx = tx.copy()

        try:
            # Extract numeric ID from ProductID (P101 -> 101)
            product_id_num = int(tx['ProductID'][1:])
        except (ValueError, IndexError):
            product_id_num = None

        api_data = product_mapping.get(product_id_num)

        if api_data:
            enriched_tx['API_Category'] = api_data.get('category')
            enriched_tx['API_Brand'] = api_data.get('brand')
            enriched_tx['API_Rating'] = api_data.get('rating')
            enriched_tx['API_Match'] = True
        else:
            enriched_tx['API_Category'] = None
            enriched_tx['API_Brand'] = None
            enriched_tx['API_Rating'] = None
            enriched_tx['API_Match'] = False

        enriched_transactions.append(enriched_tx)

    # Save enriched data to file
    save_enriched_data(enriched_transactions)

    return enriched_transactions


def save_enriched_data(enriched_transactions, filename='data/enriched_sales_data.txt'):
    """
    Saves enriched transactions back to file
    """

    # Ensure directory exists
    os.makedirs(os.path.dirname(filename), exist_ok=True)

    headers = [
        'TransactionID', 'Date', 'ProductID', 'ProductName',
        'Quantity', 'UnitPrice', 'CustomerID', 'Region',
        'API_Category', 'API_Brand', 'API_Rating', 'API_Match'
    ]

    with open(filename, 'w', encoding='utf-8') as file:
        file.write('|'.join(headers) + '\n')

        for tx in enriched_transactions:
            row = [
                str(tx.get('TransactionID', '')),
                str(tx.get('Date', '')),
                str(tx.get('ProductID', '')),
                str(tx.get('ProductName', '')),
                str(tx.get('Quantity', '')),
                str(tx.get('UnitPrice', '')),
                str(tx.get('CustomerID', '')),
                str(tx.get('Region', '')),
                str(tx.get('API_Category') or ''),
                str(tx.get('API_Brand') or ''),
                str(tx.get('API_Rating') or ''),
                str(tx.get('API_Match'))
            ]

            file.write('|'.join(row) + '\n')

    print(f"📁 Enriched data saved to: {filename}")
