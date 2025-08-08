import sys
import re
from collections import defaultdict

# List of expected compression methods from the Compressions enum
COMPRESSION_METHODS = [
    "LZ4Compressor",
    "LZ4WithDictsCompressor",
    "ZstdCompressor",
    "ZstdWithDictsCompressor"
]

def parse_log_file(filepath):
    table_data = defaultdict(dict)

    with open(filepath, "r") as file:
        lines = file.readlines()

    current_table = None
    current_key = None

    for line in lines:
        line = line.strip()

        # Remove the log prefix to get the actual content
        if "> " in line:
            content = line.split("> ", 1)[-1]
        else:
            content = line

        # Check for NoCompression pattern
        if "NoCompression" in content and "Table with name" in content:
            match = re.search(r'NoCompression.*Table with name\s+(\w+)', content)
            if match:
                table_name = match.group(1)
                current_key = "NoCompression"
                current_table = table_name
                print(f"Debug: Found NoCompression - Table: {table_name}")
            continue

        # Check for compression method pattern: "Compression: {compression_method}"
        if "Compression:" in content and "Table with name" in content:
            # Extract compression method and table name
            match = re.search(r'Compression:\s+(\w+).*Table with name\s+(\w+)', content)
            if match:
                compression_method = match.group(1)
                table_name = match.group(2)

                if compression_method in COMPRESSION_METHODS:
                    current_key = compression_method
                    current_table = table_name
                    print(f"Debug: Found compression - Table: {table_name}, Method: {compression_method}")
            continue

        # If inside a block and this line has 'Space used (total):'
        if current_table and current_key and "Space used (total):" in content:
            try:
                # Extract the numeric value after 'Space used (total):'
                match = re.search(r'Space used \(total\):\s+(\d+)', content)
                if match:
                    value = int(match.group(1))
                    table_data[current_table][current_key] = value
                    print(f"Debug: Captured value {value} for {current_table} - {current_key}")
                    # Reset after capturing the value
                    current_table = None
                    current_key = None
            except (ValueError, IndexError):
                pass

    return table_data

def print_table(table_data):
    headers = ["table_name", "NoCompression"] + COMPRESSION_METHODS
    print(", ".join(headers))

    for table in sorted(table_data.keys()):
        row = [table]
        row.append(str(table_data[table].get("NoCompression", "")))
        for compression_method in COMPRESSION_METHODS:
            row.append(str(table_data[table].get(compression_method, "")))
        print(", ".join(row))

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python parse_log.py <log_file>")
        sys.exit(1)

    filename = sys.argv[1]
    data = parse_log_file(filename)
    print_table(data)