# Measurements Data Pipeline

## 📌 Overview
This project processes and transforms battery measurement data, cleaning and enriching it for further analysis. The pipeline:
1. Loads raw measurement data from a CSV file.
2. Cleans and processes the dataset.
3. Aggregates `grid_purchase` and `grid_feedin` per hour.
4. Identifies the hour with the highest `grid_feedin` of the day.
5. Exports the cleaned dataset to a new CSV file.
6. Runs on a scheduled interval using `APScheduler`.
7. Can be containerized and executed in a Docker environment.

## 🚀 Setup & Usage

### 1️⃣ Install Dependencies (Without Docker)
```sh
pip install -r requirements.txt
```

### 2️⃣ Run the Pipeline (Without Docker)
```sh
python measurements_pipeline.py
```

### 3️⃣ Build and Run with Docker

#### Build the Docker Image
```sh
docker build -t measurements_pipeline .
```

#### Run the Container
```sh
docker run -d --name measurements_container measurements_pipeline
```

#### Check Logs
```sh
docker logs -f measurements_container
```

#### Stop and Remove the Container
```sh
docker stop measurements_container && docker rm measurements_container
```

## 📂 Project Structure
```
📦 measurements_pipeline
 ┣ 📜 measurements_pipeline.py    # Main pipeline script
 ┣ 📜 Dockerfile                  # Docker configuration
 ┣ 📜 requirements.txt             # Python dependencies
 ┣ 📜 README.md                    # Documentation
```

## 📊 Output
The cleaned dataset is saved as `cleaned_measurements.csv` in the working directory.

## 🤝 Contributions
Feel free to open an issue or submit a pull request for improvements.

## 📜 License
This project is open-source and available under the MIT License.

---
### ✨ Happy Coding! 🚀