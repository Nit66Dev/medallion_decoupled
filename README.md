# 🏗️ Real-Time E-Commerce Data Lakehouse (Medallion Architecture)

![Python](https://img.shields.io/badge/Python-3.10-blue.svg)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-3.5.1-orange.svg)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.1.0-blue.svg)
![Status](https://img.shields.io/badge/Status-Production_Ready-success.svg)

## 📖 Table of Contents
1. [Project Overview](#project-overview)
2. [Tech Stack](#tech-stack)
3. [Pipeline Architecture](#pipeline-architecture)
4. [Core Concepts: The Magic of Delta Lake](#core-concepts-the-magic-of-delta-lake)
5. [Engineering Notes & Troubleshooting: The Py4J JVM Network Bug](#engineering-notes--troubleshooting-the-py4j-jvm-network-bug)
6. [Repository Structure](#repository-structure)
7. [Getting Started & How to Run](#getting-started--how-to-run)

---

## 🚀 Project Overview
This project simulates a real-time e-commerce streaming pipeline. It ingests messy, live JSON clickstream data and processes it through a highly scalable, decoupled **Medallion Architecture** (Bronze, Silver, Gold layers) using **Apache Spark (PySpark)** and **Delta Lake**.

The pipeline is designed to be fault-tolerant, handling late-arriving data, schema enforcement, and stateful streaming aggregations on the fly.

## 🛠️ Tech Stack
* **Engine:** Apache Spark 3.5.1 (PySpark)
* **Storage Layer:** Delta Lake (ACID Transactions, Time Travel)
* **Processing Model:** Structured Streaming (Micro-batching, Watermarking)
* **Language:** Python 3.10

---

## 🏛️ Pipeline Architecture

The data flows through three distinct, decoupled layers, ensuring data quality and analytical readiness.

### 🥉 Bronze Layer (Raw Ingestion)
* **Source:** Live socket stream generating simulated user transactions (JSON).
* **Process:** Appends raw string payloads to Delta tables with ingestion timestamps.
* **Fault Tolerance:** Utilizes Delta checkpointing to maintain stream state and allow safe pipeline restarts without data loss.

### 🥈 Silver Layer (Cleansing & Conformance)
* **Source:** Reads the continuous Bronze Delta stream.
* **Process:** Parses complex JSON strings into typed PySpark `StructTypes`.
  * Enforces data quality: Drops records with missing `user_id`s and dynamically coalesces null `price` values to `0.0`.
  * Casts string timestamps into native PySpark `TimestampType`.

### 🥇 Gold Layer (Business Aggregations)
* **Source:** Reads the cleaned Silver Delta stream.
* **Process:** Filters for "purchase" events and calculates live revenue.
* **Advanced Streaming:** Implements a 1-minute **Watermark** to handle late-arriving data and groups metrics using a 30-second sliding/tumbling window to produce a real-time financial dashboard.

---

## 🧠 Core Concepts: The Magic of Delta Lake

As we discussed, standard Parquet files are immutable (read-only). If you need to update a single customer's address in a 10-Terabyte Parquet file, you must rewrite the entire file.

**Delta Lake** solves this. It is an open-source storage layer that sits on top of your Parquet files. When you write data as "delta", it saves the Parquet files and automatically creates a hidden folder called `_delta_log`.

This transaction log tracks every single insert, update, and delete you make. Because it keeps a history of all changes, it unlocks **Time Travel**—the ability to query exactly what your data looked like at any point in the past.

---

## ⚠️ Engineering Notes & Troubleshooting: The Py4J JVM Network Bug

Running Apache Spark locally on a Mac can sometimes trigger severe network timeout errors upon startup (e.g., `JAVA_GATEWAY_EXITED` and `Connection from <unknown remote> closed`). Here is a deep dive into the architecture, the root cause, and how to fix it.

### Part 1: The Architecture (How PySpark Works)
PySpark is not actually a pure Python engine. Apache Spark is written in Scala and Java.
When you write Python code, you are actually using a "wrapper." Under the hood, PySpark uses a tool called **Py4J** to create a bridge between your Python script and a hidden Java Virtual Machine (JVM) running in the background.

For your code to work, Python and Java have to constantly text each other over your Mac's internal network using "Sockets" (ports).

### Part 2: The Root Cause (The 20-Second Hang)
When you call `.getOrCreate()`, Spark begins booting up the Java engine. Part of that boot-up process requires the "Driver" (the main brain of Spark) to figure out its own network address so it can tell Python and the worker nodes, *"Here is where you can send data back to me."*

Here is exactly where it failed:
* Java asked your Mac's operating system: *"What is my hostname?"*
* Your Mac replied: *"Your name is SW-LPI3499."*
* Java then asked the Mac's DNS resolver: *"Okay, what IP address is SW-LPI3499?"*
* **The Trap:** Macs can sometimes be notoriously bad at resolving local computer names that don't end in `.local`. The Mac's DNS system essentially stared blankly at Java and hung.
* Java waited patiently... for exactly **20 seconds**.
* Finally, the network request timed out. Java panicked, the `TransportResponseHandler` crashed, and the JVM abruptly shut down.
* Back on the Python side, your script was waiting for Java to say hello. When Java crashed, Python threw the `JAVA_GATEWAY_EXITED` and `Connection from <unknown remote> closed` errors. Python was basically saying, *"The Java guy hung up the phone before we even started talking!"*

### Part 3: The Steps We Took to Fix It

#### Step 1: The Ivy Cache Clear (`rm -rf ~/.ivy2/cache`)
Initially, because this happened right after we added the Delta Lake packages, the most logical assumption was that the Delta download got corrupted. Spark uses a tool called "Ivy" to download Java packages. Sometimes, if a download is interrupted, Ivy leaves a broken file in its cache, causing Spark to crash on startup. Deleting this cache forced Spark to redownload the files cleanly.
* **Result:** It proved the download wasn't the issue, because it crashed again.

#### Step 2: Analyzing the Logs
When analyzing the second set of logs, looking specifically at the timestamps revealed that the Delta packages downloaded successfully in about 2 seconds. Then, there was a massive 20-second gap of silence before the crash. In distributed computing, a clean 20-second gap almost always points to a hardcoded network timeout.

#### Step 3: The `bindAddress` vs. `host` Override
Earlier, we added `spark.driver.bindAddress = 127.0.0.1`. This told Java to listen on the local loopback network.

However, we also needed to add `spark.driver.host = 127.0.0.1`. **This was the silver bullet.** This command told Java: *"Do not ask the Mac for your hostname. Do not try to resolve SW-LPI3499. Your name is officially 127.0.0.1."* By hardcoding the host address, we completely bypassed the Mac's buggy DNS resolver. Java instantly knew its own IP, Python connected to it in milliseconds, and the engine successfully booted up!

---

## 📁 Repository Structure

```text
.
├── data/
│   ├── bronze/                  # Raw Delta tables
│   ├── silver/                  # Cleansed Delta tables
│   └── checkpoints/             # Structured Streaming state logs
├── cybertron_server.py        # Python socket server generating mock e-commerce data
├── xx2_2_medallion_bronze.py  # PySpark script for raw stream ingestion
├── xx2_3_medallion_silver.py  # PySpark script for JSON parsing & data quality
├── xx2_4_medallion_gold.py    # PySpark script for sliding window aggregations
└── README.md






## 🏁 Getting Started & How to Run

To see the real-time Medallion Architecture in action, you will need to run the scripts in a specific sequence. Because this is a streaming application, each layer depends on the upstream data feed being active.



### Prerequisites
* **Python:** 3.10+
* **Spark:** Apache Spark 3.5.1
* **Dependencies:** Make sure you have PySpark installed in your virtual environment:
  ```bash
  pip install pyspark==3.5.1



Execution Steps
Open four separate terminal windows and execute the following commands in order. Leave each process running.
Terminal 1: Start the Cybertron Server
This script acts as our mock e-commerce backend, generating a continuous live socket stream of JSON clickstream data.

python cybertron_server.py

(Wait a few seconds for the server to initialize and start broadcasting on the designated port).
Terminal 2: Ingest the Bronze Layer
This script connects to the live socket stream and lands the raw data into Delta format.

python xx2_2_medallion_bronze.py

Terminal 3: Process the Silver Layer
Once the Bronze layer is actively writing data, start the Silver job to handle schema enforcement, cleansing, and type casting.

python xx2_3_medallion_silver.py

Terminal 4: Aggregate the Gold Layer
Finally, start the Gold job to perform sliding window aggregations and calculate real-time business metrics (like live revenue).

python xx2_4_medallion_gold.py

Stopping the Pipeline
To gracefully shut down the pipeline, use Ctrl + C in each terminal window, ideally in reverse order (Gold -> Silver -> Bronze -> Server) to prevent broken stream states, though Delta Lake's checkpointing will ensure you don't lose data regardless










