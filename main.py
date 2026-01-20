from datetime import datetime
from collections import defaultdict
import os

from utils.api_handler import fetch_all_products, create_product_mapping, enrich_sales_data
from utils.data_processor import calculate_total_revenue, region_wise_sales, top_selling_products, customer_analysis, daily_sales_trend, find_peak_sales_day, low_performing_products
from utils.file_handler import read_sales_data, parse_transactions

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

def main():
    """
    Main execution function
    """

    try:
        print("=" * 40)
        print("SALES ANALYTICS SYSTEM")
        print("=" * 40)

        # -----------------------------
        # 1. Read sales data
        # -----------------------------
        print("\n[1/10] Reading sales data...")
        raw_lines = read_sales_data("data/sales_data.txt")
        print(f"✓ Successfully read {len(raw_lines)} transactions")

        # -----------------------------
        # 2. Parse and clean data
        # -----------------------------
        print("\n[2/10] Parsing and cleaning data...")
        parsed_transactions = parse_transactions(raw_lines)
        print(f"✓ Parsed {len(parsed_transactions)} records")

        # -----------------------------
        # 3. Show filter options
        # -----------------------------
        regions = sorted(set(tx['Region'] for tx in parsed_transactions if tx.get('Region')))
        amounts = [tx['Quantity'] * tx['UnitPrice'] for tx in parsed_transactions]

        print("\n[3/10] Filter Options Available:")
        print("Regions:", ", ".join(regions))
        print(f"Amount Range: ₹{min(amounts):,.0f} - ₹{max(amounts):,.0f}")

        apply_filter = input("\nDo you want to filter data? (y/n): ").strip().lower()

        region_filter = None
        min_amount = None
        max_amount = None

        if apply_filter == 'y':
            region_filter = input("Enter region (or press Enter to skip): ").strip()
            min_amt = input("Enter minimum amount (or press Enter to skip): ").strip()
            max_amt = input("Enter maximum amount (or press Enter to skip): ").strip()

            min_amount = float(min_amt) if min_amt else None
            max_amount = float(max_amt) if max_amt else None

        # -----------------------------
        # 4. Validate transactions
        # -----------------------------
        print("\n[4/10] Validating transactions...")
        valid_transactions, invalid_count, summary = validate_and_filter(
            parsed_transactions,
            region=region_filter or None,
            min_amount=min_amount,
            max_amount=max_amount
        )

        print(f"✓ Valid: {len(valid_transactions)} | Invalid: {invalid_count}")

        # -----------------------------
        # 5. Analyze sales data
        # -----------------------------
        print("\n[5/10] Analyzing sales data...")
        total_revenue = calculate_total_revenue(valid_transactions)
        region_sales = region_wise_sales(valid_transactions)
        top_products = top_selling_products(valid_transactions)
        customer_stats = customer_analysis(valid_transactions)
        daily_trend = daily_sales_trend(valid_transactions)
        peak_day = find_peak_sales_day(valid_transactions)
        low_products = low_performing_products(valid_transactions)
        print("✓ Analysis complete")

        # -----------------------------
        # 6. Fetch API data
        # -----------------------------
        print("\n[6/10] Fetching product data from API...")
        api_products = fetch_all_products()
        print(f"✓ Fetched {len(api_products)} products")

        # -----------------------------
        # 7. Enrich sales data
        # -----------------------------
        print("\n[7/10] Enriching sales data...")
        product_mapping = create_product_mapping(api_products)
        enriched_transactions = enrich_sales_data(valid_transactions, product_mapping)

        enriched_count = sum(1 for tx in enriched_transactions if tx.get('API_Match'))
        success_rate = (enriched_count / len(enriched_transactions)) * 100 if enriched_transactions else 0

        print(f"✓ Enriched {enriched_count}/{len(enriched_transactions)} transactions ({success_rate:.1f}%)")

        # -----------------------------
        # 8. Save enriched data
        # -----------------------------
        print("\n[8/10] Saving enriched data...")
        print("✓ Saved to: data/enriched_sales_data.txt")

        # -----------------------------
        # 9. Generate report
        # -----------------------------
        print("\n[9/10] Generating report...")
        generate_sales_report(valid_transactions, enriched_transactions)
        print("✓ Report saved to: output/sales_report.txt")

        # -----------------------------
        # 10. Completion
        # -----------------------------
        print("\n[10/10] Process Complete!")
        print("=" * 40)

    except Exception as e:
        print("\n❌ An error occurred during processing.")
        print("Error details:", str(e))
        print("Please check input files or try again.")


# ---- MAIN EXECUTION ----
if __name__ == "__main__":
    main()
