# 📊 Measurements Data Pipeline

## Overview  
This project is a **data processing pipeline** that:
- Loads, cleans, and processes energy measurement data.
- Aggregates hourly totals for `grid_purchase` and `grid_feedin`.
- Identifies the **hour with the highest** `grid_feedin` and `grid_purchase` of the day.
- Uses **logging** for execution tracking.
- Includes **unit tests** to validate processing.
- Runs as a **Docker container** for easy deployment.

---

## 📂 Project Structure  
```
├── measurements_pipeline.py   # Main processing script
├── pipeline_unit_tests.py     # Unit tests for the pipeline
├── requirements.txt           # Python dependencies
├── Dockerfile                 # Docker container setup
├── pipeline.log               # Execution logs
```

---

## 🚀 Running the Pipeline  

### **1️⃣ Install Dependencies**  
Ensure Python is installed, then run:
```sh
pip install -r requirements.txt
```

### **2️⃣ Run the Pipeline Locally**  
```sh
python measurements_pipeline.py
```
This starts the scheduler and processes the data **every 5 minutes**.

---

## 🐳 Running with Docker  

### **1️⃣ Build the Docker Image**
```sh
docker build -t measurements_pipeline .
```

### **2️⃣ Run the Container**
```sh
docker run -d --name measurements_container measurements_pipeline
```
- The pipeline will run **inside the container**.
- To check logs:
  ```sh
  docker logs -f measurements_container
  ```

### **3️⃣ Stop & Remove the Container**
```sh
docker stop measurements_container
docker rm measurements_container
```

---

## ✅ Running Unit Tests  
To validate the processing pipeline, run:
```sh
python -m unittest pipeline_unit_tests.py
```
Tests check:
- **Dataset loading**
- **Cleaning process**
- **Hourly aggregation**
- **Export functionality**

---

## 📜 Logging  
- Execution logs are saved in **`pipeline.log`**.
- Logs include **errors, warnings, and execution steps**.

---

## ⚠️ Notes  
- If running with **Docker**, ensure your dataset is inside the container.
- Modify `measurements_pipeline.py` if you need different scheduling intervals.

---
🚀 **Now you're ready to process data efficiently!** 🚀  
