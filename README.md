Airline Performance and Flight Delay Analysis 🛫📊
This project leverages the power of Kubernetes, Apache Spark, and Cassandra to analyze airline performance and flight delays. We use .parquet files as our primary data format.

Prerequisites 📋
Ensure you have kubectl, minikube, and helm installed on your local machine to execute the steps outlined below.

Setup Instructions 🛠
Step 1: Install Necessary Libraries
Run the following bash script to install all required libraries:

bash
Copy code
./requirements.sh
Step 2: Set Up Kubernetes Cluster
Initialize the Minikube cluster with specified resources:

bash
Copy code
minikube start --cores=8 --memory=5120
Step 3: Interact with Kubernetes Cluster
Utilize kubectl to interact with your cluster. Check all pods:

bash
Copy code
kubectl get po -A
For a GUI overview, launch the Minikube dashboard:

bash
Copy code
minikube dashboard
Step 4: Set Up Cassandra in Kubernetes
Navigate to the config folder and execute the script to create Cassandra pods and stateful set:

bash
Copy code
cd config
sh create-cassandra-pod.sh
Step 5: Install Spark
From the config directory, use Helm to install Spark using the custom values from sparkValues.yaml:

bash
Copy code
helm install my-spark bitnami/spark --version 8.1.6 -f sparkValues.yaml
Step 6: Import Data and Application to Spark
Copy the application JAR and .parquet data to the Spark master pod:

bash
Copy code
kubectl cp [path-to-the-jarfile] default/my-spark-master-0:/opt/bitnami/spark
kubectl cp [path-to-the-parquet-data] default/my-spark-master-0:/opt/bitnami/spark
Step 7: Submit Spark Job
Submit the Spark job to process the flight data:

bash
Copy code
./bin/spark-submit \
--class org.example.FlightDelayAnalysis \
--master spark://my-spark-master-0.my-spark-headless.default.svc.cluster.local:7077 \
--num-executors 2 \
--driver-memory 1g \
--driver-cores 1 \
--executor-memory 3g \
--executor-cores 3 \
FlightDelayAnalysis-1.0-SNAPSHOT.jar "10.244.0.6" "9042"
Step 8: Analysis Workflow
The application performs the following steps:

Establishes connection with Spark and Cassandra.
Reads the .parquet file into a Spark DataFrame.
Conducts ETL operations on the DataFrame to prepare it for analysis.
Performs analysis and stores the results back into Cassandra.
Step 9: Visualize Results
Use Cassandra port forwarding to access Cassandra from outside the cluster, then run a Python script to visualize the results using Matplotlib:

bash
Copy code
# Example Python script to plot data
python plot_results.py
