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


# ---- MAIN EXECUTION ----
if __name__ == "__main__":
    file_path = "/mnt/data/sales_data.txt"
    clean_sales_data(file_path)
