# **Weather Data Pipeline with Airflow**

## **Project Overview**
This Apache Airflow DAG (`weather_dag`) automates the process of extracting weather data from the OpenWeatherMap API, transforming it into a structured format, and uploading the processed data to an Amazon S3 bucket. The pipeline runs **hourly** and ensures real-time weather data availability.

## **Key Components**
- **Extraction**: Fetches weather data for Kansas from OpenWeatherMap API.
- **Transformation**: Converts temperature from Kelvin to Fahrenheit and structures the data.
- **Loading**: Saves the processed data as a CSV file locally and then uploads it to an Amazon S3 bucket.

---

## **DAG Workflow**
### **1. API Readiness Check (`is_weather_api_ready`)**
- Uses **HttpSensor** to verify that OpenWeatherMap API is accessible before proceeding.

### **2. Data Extraction (`extract_weather_data`)**
- Uses **HttpOperator** to call OpenWeatherMap API and retrieve weather data for Kansas.

### **3. Data Transformation (`transform_load_weather_data`)**
- Extracts key attributes (temperature, humidity, wind speed, etc.).
- Converts temperature from Kelvin to Fahrenheit.
- Saves data as a **CSV file** in the local Airflow directory (`/usr/local/airflow/include/data_files`).
- Passes the file path and name to subsequent tasks via XCom.

### **4. Upload to S3 (`upload_to_s3`)**
- Uses **LocalFilesystemToS3Operator** to transfer the CSV file from local storage to an S3 bucket.

---

## **Setup & Configuration**
### **1. Environment Variables**
Before running the DAG, set the following environment variables in **`.env`** or Airflow UI:
```
S3_BUCKET_NAME=your-s3-bucket-name
WEATHER_API_KEY=your-openweathermap-api-key
```

### **2. Connections**
- **OpenWeatherMap API Connection**: Set up an **HTTP Connection** in Airflow (`weathermap_api`).
- **AWS Connection**: Set up an **AWS Connection** in Airflow (`aws_default`).

### **3. Required Python Libraries**
Ensure the following Python packages are installed in your Airflow environment:
```bash
pip install apache-airflow-providers-http apache-airflow-providers-amazon python-dotenv pandas
or if using ASTRO CLI run astro dev restart
```

---

## **What Has Been Learned?**
- **Using Sensors in Airflow**: The **HttpSensor** ensures the API is available before making requests.
- **Calling APIs with Airflow**: The **HttpOperator** retrieves real-time weather data.
- **Transforming Data in Python**: Extracting key fields and converting temperature.
- **XCom for Data Passing**: Sharing file paths between tasks using **XCom**.
- **Uploading to AWS S3**: Using **LocalFilesystemToS3Operator** to move local files to the cloud.

---

## **Next Steps**
- Extend the pipeline to support **multiple cities**.
- Store processed data in **Amazon Redshift** for analytics.
- Implement **error handling and logging** for robustness.

🚀 **This pipeline ensures an efficient and scalable weather data ingestion process!** 🚀