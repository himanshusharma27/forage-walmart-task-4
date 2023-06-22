import sqlite3
import csv

# Connect to the SQLite database
conn = sqlite3.connect('walmart.db')
cursor = conn.cursor()

# Create tables
cursor.execute('''CREATE TABLE IF NOT EXISTS Products (
                    product_id INTEGER PRIMARY KEY,
                    name TEXT,
                    manufacturer TEXT,
                    weight REAL,
                    flavor TEXT,
                    target_health_condition TEXT
                )''')

cursor.execute('''CREATE TABLE IF NOT EXISTS Animals (
                    animal_id INTEGER PRIMARY KEY,
                    name TEXT
                )''')

cursor.execute('''CREATE TABLE IF NOT EXISTS ProductAnimal (
                    product_id INTEGER,
                    animal_id INTEGER,
                    FOREIGN KEY (product_id) REFERENCES Products (product_id),
                    FOREIGN KEY (animal_id) REFERENCES Animals (animal_id),
                    PRIMARY KEY (product_id, animal_id)
                )''')

cursor.execute('''CREATE TABLE IF NOT EXISTS Manufacturers (
                    manufacturer_id INTEGER PRIMARY KEY,
                    name TEXT
                )''')

cursor.execute('''CREATE TABLE IF NOT EXISTS Customers (
                    customer_id INTEGER PRIMARY KEY,
                    name TEXT,
                    email TEXT
                )''')

cursor.execute('''CREATE TABLE IF NOT EXISTS Transactions (
                    transaction_id INTEGER PRIMARY KEY,
                    customer_id INTEGER,
                    date TEXT,
                    FOREIGN KEY (customer_id) REFERENCES Customers (customer_id)
                )''')

cursor.execute('''CREATE TABLE IF NOT EXISTS TransactionProduct (
                    transaction_id INTEGER,
                    product_id INTEGER,
                    quantity INTEGER,
                    FOREIGN KEY (transaction_id) REFERENCES Transactions (transaction_id),
                    FOREIGN KEY (product_id) REFERENCES Products (product_id),
                    PRIMARY KEY (transaction_id, product_id)
                )''')

cursor.execute('''CREATE TABLE IF NOT EXISTS Locations (
                    location_id INTEGER PRIMARY KEY,
                    name TEXT,
                    zip_code TEXT
                )''')

cursor.execute('''CREATE TABLE IF NOT EXISTS Shipments (
                    shipment_id INTEGER PRIMARY KEY,
                    origin_id INTEGER,
                    destination_id INTEGER,
                    FOREIGN KEY (origin_id) REFERENCES Locations (location_id),
                    FOREIGN KEY (destination_id) REFERENCES Locations (location_id)
                )''')

cursor.execute('''CREATE TABLE IF NOT EXISTS ShipmentProduct (
                    shipment_id INTEGER,
                    product_id INTEGER,
                    quantity INTEGER,
                    FOREIGN KEY (shipment_id) REFERENCES Shipments (shipment_id),
                    FOREIGN KEY (product_id) REFERENCES Products (product_id),
                    PRIMARY KEY (shipment_id, product_id)
                )''')

# Insert data from Spreadsheet 0 (Pet Products)
with open('forage-walmart-task-4/data/shipping_data_0.csv', 'r') as file:
    reader = csv.reader(file)
    next(reader)  # Skip the header row

    for row in reader:
        product_id, name, manufacturer, weight, flavor, target_health_condition = row

        # Insert product data
        cursor.execute('''INSERT INTO Products (product_id, name, manufacturer, weight, flavor, target_health_condition)
                          VALUES (?, ?, ?, ?, ?, ?)''', (product_id, name, manufacturer, weight, flavor, target_health_condition))

# Insert data from Spreadsheet 2 (Locations)
with open('shipping_data_2.csv', 'r') as file:
    reader = csv.reader(file)
    next(reader)  # Skip the header row

    for row in reader:
        location_id, name, zip_code = row

        # Insert location data
        cursor.execute('''INSERT INTO Locations (location_id, name, zip_code)
                          VALUES (?, ?, ?)''', (location_id, name, zip_code))

# Insert data from Spreadsheet 1 (Shipments)
with open('shipping_data_1.csv', 'r') as file:
    reader = csv.reader(file)
    next(reader)  # Skip the header row

    for row in reader:
        shipment_id, origin, destination, shipping_identifier = row

        # Insert shipment data
        cursor.execute('''INSERT INTO Shipments (shipment_id, origin_id, destination_id)
                          VALUES (?, ?, ?)''', (shipment_id, origin, destination))

        # Get the products associated with the shipping identifier
        cursor.execute('''SELECT product_id FROM Products WHERE shipping_identifier = ?''', (shipping_identifier,))
        products = cursor.fetchall()

        # Insert shipment-product relationship data
        for product_id in products:
            cursor.execute('''INSERT INTO ShipmentProduct (shipment_id, product_id, quantity)
                              VALUES (?, ?, ?)''', (shipment_id, product_id, 1))  # Assuming quantity is always 1

# Commit the changes and close the database connection
conn.commit()
conn.close()
