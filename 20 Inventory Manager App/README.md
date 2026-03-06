📦 Inventory & Sales Management System :

A Python-based Inventory & Sales Management application featuring:
 1. Stock tracking
 2. Sales recording
 3. Revenue reporting
 4. Low-stock alerts
 5. JSON data persistence
 6. CLI interface
 7. Streamlit web UI

Designed using Object-Oriented Programming principles and clean modular architecture.

🚀 Features

Product Management :
 1. Add new products
 2. Remove products
 3. Restock inventory
 4. Sell products
 5. Automatic stock updates

Sales Tracking :
 1. Record each sale with timestamp
 2. Persistent sales history (JSON)
 3. Revenue calculation
 4. Total revenue report

Reports :
 1. Total revenue summary
 2. Low stock alert report
 3. Sales history log

Interfaces :
 1. Command Line Interface (CLI)
 2. Simple Web UI using Streamlit

🏗 Project Structure
inventory_manager/
│
├── main.py          # CLI version
├── app.py           # Streamlit UI
├── models.py        # Product class
├── inventory.py     # Inventory manager
├── sales.py         # Sales manager
├── storage.py       # JSON persistence
│
├── data/
│   ├── products.json
│   └── sales.json
│
└── README.md

🧠 Architecture

The project follows a modular OOP design:
 # Product → Represents a single product
 # Inventory → Manages collection of products
 # SalesManager → Handles sales history and revenue calculations
 # Storage → Manages JSON data persistence
 # main.py → CLI interface
 # app.py → Streamlit UI interface

This separation ensures:
 1. Clean responsibilities
 2. Easy scalability
 3. Maintainable codebase

💻 Technologies Used :
 1. Python 3
 2. Streamlit
 3. JSON
 4. Datetime
 5. Object-Oriented Programming (OOP)

▶️ How to Run:
Bash: uv run main.py
Web: uv run streamlit run app.py
