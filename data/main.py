from datetime import datetime
from collections import defaultdict
import os



def clean_sales_data(file_path):
    total_records = 0
    valid_records = []
    invalid_count = 0

    with open(file_path, 'r', encoding='latin-1') as file:
        header = file.readline()  # skip header

        for line in file:
            line = line.strip()

            # Skip empty lines
            if not line:
                continue

            total_records += 1

            parts = line.split('|')

            # Skip rows with incorrect number of fields
            if len(parts) != 8:
                invalid_count += 1
                continue

            (
                transaction_id,
                date,
                product_id,
                product_name,
                quantity,
                unit_price,
                customer_id,
                region
            ) = parts

            # Remove commas from product name
            product_name = product_name.replace(',', '')

            # Validate TransactionID
            if not transaction_id.startswith('T'):
                invalid_count += 1
                continue

            # Validate CustomerID and Region
            if not customer_id.strip() or not region.strip():
                invalid_count += 1
                continue

            # Clean numeric fields
            try:
                quantity = int(quantity.replace(',', ''))
                unit_price = int(unit_price.replace(',', ''))
            except ValueError:
                invalid_count += 1
                continue

            # Validate numeric rules
            if quantity <= 0 or unit_price <= 0:
                invalid_count += 1
                continue

            # Record is valid
            valid_records.append([
                transaction_id,
                date,
                product_id,
                product_name,
                quantity,
                unit_price,
                customer_id,
                region
            ])

    # Required validation output
    print(f"Total records parsed: {total_records}")
    print(f"Invalid records removed: {invalid_count}")
    print(f"Valid records after cleaning: {len(valid_records)}")

    return valid_records

def generate_sales_report(transactions, enriched_transactions, output_file='output/sales_report.txt'):
    """
    Generates a comprehensive formatted text report
    """

    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    total_records = len(transactions)

    # -------------------------
    # OVERALL SUMMARY
    # -------------------------
    total_revenue = sum(tx['Quantity'] * tx['UnitPrice'] for tx in transactions)
    avg_order_value = total_revenue / total_records if total_records else 0

    dates = sorted(tx['Date'] for tx in transactions)
    date_range = f"{dates[0]} to {dates[-1]}" if dates else "N/A"

    # -------------------------
    # REGION-WISE PERFORMANCE
    # -------------------------
    region_data = defaultdict(lambda: {'sales': 0.0, 'count': 0})

    for tx in transactions:
        amount = tx['Quantity'] * tx['UnitPrice']
        region_data[tx['Region']]['sales'] += amount
        region_data[tx['Region']]['count'] += 1

    region_summary = []
    for region, data in region_data.items():
        pct = (data['sales'] / total_revenue) * 100 if total_revenue else 0
        region_summary.append(
            (region, data['sales'], pct, data['count'])
        )

    region_summary.sort(key=lambda x: x[1], reverse=True)

    # -------------------------
    # TOP PRODUCTS
    # -------------------------
    product_data = defaultdict(lambda: {'qty': 0, 'rev': 0.0})

    for tx in transactions:
        product_data[tx['ProductName']]['qty'] += tx['Quantity']
        product_data[tx['ProductName']]['rev'] += tx['Quantity'] * tx['UnitPrice']

    top_products = sorted(
        product_data.items(),
        key=lambda x: x[1]['qty'],
        reverse=True
    )[:5]

    # -------------------------
    # TOP CUSTOMERS
    # -------------------------
    customer_data = defaultdict(lambda: {'spent': 0.0, 'count': 0})

    for tx in transactions:
        customer_data[tx['CustomerID']]['spent'] += tx['Quantity'] * tx['UnitPrice']
        customer_data[tx['CustomerID']]['count'] += 1

    top_customers = sorted(
        customer_data.items(),
        key=lambda x: x[1]['spent'],
        reverse=True
    )[:5]

    # -------------------------
    # DAILY SALES TREND
    # -------------------------
    daily_data = defaultdict(lambda: {'rev': 0.0, 'count': 0, 'customers': set()})

    for tx in transactions:
        date = tx['Date']
        daily_data[date]['rev'] += tx['Quantity'] * tx['UnitPrice']
        daily_data[date]['count'] += 1
        daily_data[date]['customers'].add(tx['CustomerID'])

    daily_summary = sorted(daily_data.items())

    peak_day = max(daily_data.items(), key=lambda x: x[1]['rev'])

    # -------------------------
    # LOW PERFORMING PRODUCTS
    # -------------------------
    low_products = [
        (p, d['qty'], d['rev'])
        for p, d in product_data.items()
        if d['qty'] < 10
    ]

    # -------------------------
    # API ENRICHMENT SUMMARY
    # -------------------------
    enriched_count = sum(1 for tx in enriched_transactions if tx.get('API_Match'))
    failed_products = sorted({
        tx['ProductName']
        for tx in enriched_transactions
        if not tx.get('API_Match')
    })

    success_rate = (enriched_count / len(enriched_transactions)) * 100 if enriched_transactions else 0

    # -------------------------
    # WRITE REPORT
    # -------------------------
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("=" * 44 + "\n")
        f.write("        SALES ANALYTICS REPORT\n")
        f.write(f"      Generated: {now}\n")
        f.write(f"      Records Processed: {total_records}\n")
        f.write("=" * 44 + "\n\n")

        f.write("OVERALL SUMMARY\n")
        f.write("-" * 44 + "\n")
        f.write(f"Total Revenue:        ₹{total_revenue:,.2f}\n")
        f.write(f"Total Transactions:   {total_records}\n")
        f.write(f"Average Order Value:  ₹{avg_order_value:,.2f}\n")
        f.write(f"Date Range:           {date_range}\n\n")

        f.write("REGION-WISE PERFORMANCE\n")
        f.write("-" * 44 + "\n")
        f.write("Region     Sales            % Total   Transactions\n")
        for r, s, p, c in region_summary:
            f.write(f"{r:<10} ₹{s:>10,.2f}     {p:>6.2f}%     {c}\n")
        f.write("\n")

        f.write("TOP 5 PRODUCTS\n")
        f.write("-" * 44 + "\n")
        f.write("Rank  Product            Quantity   Revenue\n")
        for i, (p, d) in enumerate(top_products, 1):
            f.write(f"{i:<5} {p:<18} {d['qty']:<10} ₹{d['rev']:,.2f}\n")
        f.write("\n")

        f.write("TOP 5 CUSTOMERS\n")
        f.write("-" * 44 + "\n")
        f.write("Rank  Customer   Total Spent     Orders\n")
        for i, (c, d) in enumerate(top_customers, 1):
            f.write(f"{i:<5} {c:<10} ₹{d['spent']:,.2f}   {d['count']}\n")
        f.write("\n")

        f.write("DAILY SALES TREND\n")
        f.write("-" * 44 + "\n")
        f.write("Date         Revenue        Transactions  Customers\n")
        for date, d in daily_summary:
            f.write(f"{date}  ₹{d['rev']:>10,.2f}     {d['count']:<5}          {len(d['customers'])}\n")
        f.write("\n")

        f.write("PRODUCT PERFORMANCE ANALYSIS\n")
        f.write("-" * 44 + "\n")
        f.write(f"Best Selling Day: {peak_day[0]} (₹{peak_day[1]['rev']:,.2f})\n")
        f.write("Low Performing Products:\n")
        for p, q, r in low_products:
            f.write(f" - {p}: {q} units, ₹{r:,.2f}\n")
        f.write("\n")

        f.write("API ENRICHMENT SUMMARY\n")
        f.write("-" * 44 + "\n")
        f.write(f"Total Products Enriched: {enriched_count}\n")
        f.write(f"Success Rate: {success_rate:.2f}%\n")
        f.write("Products Not Enriched:\n")
        for p in failed_products:
            f.write(f" - {p}\n")

    print(f"📄 Sales report generated at: {output_file}")


# ---- MAIN EXECUTION ----
if __name__ == "__main__":
    file_path = "/mnt/data/sales_data.txt"
    clean_sales_data(file_path)
