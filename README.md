# Ecommerce Lakehouse

An end-to-end lakehouse implementation for an ecommerce platform, demonstrating modern data architecture best practices using Databricks, Confluent Cloud, GitHub Actions, and Streamlit.

## 📋 Overview

This project implements a complete data lakehouse solution for ecommerce operations, integrating real-time data streaming with analytics and business intelligence capabilities. The architecture is designed to be cost-effective, leveraging the Databricks free edition for data processing and analytics.

## 🏗️ Architecture & Tech Stack

### Core Components

- **Databricks (Free Edition)**: Data lakehouse platform for ETL, data processing, and analytics
- **Confluent Cloud**: Real-time data streaming and event management
- **GitHub Actions**: CI/CD automation and orchestration
- **Streamlit**: Interactive dashboards and data visualization
- **Delta Lake**: ACID-compliant data storage format for reliability and performance

### Data Flow

```
Real-time Events (Ecommerce) 
    ↓
Confluent Cloud (Kafka Topics)
    ↓
Databricks (ETL/Processing)
    ↓
Delta Lake (Data Lakehouse)
    ↓
Streamlit (Dashboards & Analytics)
```

## ✨ Key Features

- **Real-time Data Ingestion**: Stream ecommerce events via Confluent Cloud
- **Scalable Processing**: Databricks for distributed data processing
- **Data Quality**: Built-in validation and error handling
- **Interactive Analytics**: Streamlit dashboards for business insights
- **Automated Workflows**: GitHub Actions for CI/CD and scheduled jobs
- **Cost-Optimized**: Leverages free tier services where possible

## 🚀 Getting Started

### Prerequisites

- Databricks account (Free Edition)
- Confluent Cloud account
- GitHub repository
- Python 3.8+
- Streamlit

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/nahilahmed/ecommerce-lakehouse.git
   cd ecommerce-lakehouse
   ```

2. **Set up environment variables**
   ```bash
   cp .env.example .env
   # Edit .env with your credentials
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure Databricks**
   - Create a Databricks workspace (Free Edition)
   - Generate personal access token
   - Configure connection details in your environment

5. **Configure Confluent Cloud**
   - Create Kafka cluster and topics
   - Set up API keys and endpoints

## 📊 Dashboard

Run the Streamlit dashboard:

```bash
streamlit run app.py
```

## 🔄 Automation

GitHub Actions workflows automate:
- Data ingestion from Confluent Cloud
- ETL jobs on Databricks
- Data quality checks
- Dashboard updates

## 📁 Project Structure

```
ecommerce-lakehouse/
├── README.md
├── .github/
│   └── workflows/          # GitHub Actions CI/CD pipelines
├── databricks/
│   ├── notebooks/          # Databricks notebooks for ETL
│   └── jobs/              # Job configurations
├── confluent/
│   └── topics/            # Kafka topic configurations
├── streamlit/
│   └── app.py             # Dashboard application
├── src/
│   └── processors/        # Data processing modules
├── tests/                 # Test suite
├── requirements.txt       # Python dependencies
└── .env.example          # Environment variables template
```

## 🛠️ Development

### Running Tests

```bash
pytest tests/
```

### Making Changes

1. Create a feature branch
2. Implement changes
3. Run tests and validation
4. Submit pull request for review

## 📝 License

This project is open source and available under the MIT License.

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📧 Contact

For questions or support, please open an issue on GitHub.