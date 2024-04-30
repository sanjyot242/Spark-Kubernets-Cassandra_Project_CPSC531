from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import pandas as pd
import matplotlib.pyplot as plt

# Set up the connection to Cassandra
auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
cluster = Cluster(['127.0.0.1'], port=9042, auth_provider=auth_provider)
session = cluster.connect()

try:
    rows = session.execute("SELECT keyspace_name FROM system_schema.keyspaces;")
    keyspaces = [row.keyspace_name for row in rows]
    
    if 'flight_delay_analysis' in keyspaces:
        session.set_keyspace('flight_delay_analysis')
        
        # Query data from the tables
        airline_query = """
        SELECT marketing_airline_network, average_departure_delay, average_arrival_delay
        FROM airline_delay_stats;
        """
        airport_query = """
        SELECT origin_city_name, average_departure_delay, average_arrival_delay
        FROM airport_delay_stats;
        """
        monthly_query = """
        SELECT flight_month, average_departure_delay, average_arrival_delay
        FROM monthly_delay_stats;
        """
        
        # Execute queries and convert to DataFrames
        airline_results = session.execute(airline_query)
        airport_results = session.execute(airport_query)
        monthly_results = session.execute(monthly_query)
        
        airline_df = pd.DataFrame(list(airline_results))
        airport_df = pd.DataFrame(list(airport_results))
        monthly_df = pd.DataFrame(list(monthly_results))
        
        # Create a figure with subplots
        fig, axes = plt.subplots(nrows=3, ncols=1, figsize=(12, 18))
        
        # Plot average delays by airline
        axes[0].bar(airline_df['marketing_airline_network'], airline_df['average_departure_delay'], color='b', label='Average Departure Delay')
        axes[0].set_xlabel("Marketing Airline Network")
        axes[0].set_ylabel("Average Delay (minutes)")
        axes[0].set_title("Average Departure Delay by Airline")
        axes[0].tick_params(axis='x', rotation=45)
        axes[0].legend()
        axes[0].grid(axis='y', linestyle='--', alpha=0.7)
        
        # Plot average delays by airport
        axes[1].bar(airport_df['origin_city_name'], airport_df['average_departure_delay'], color='r', label='Average Departure Delay')
        axes[1].set_xlabel("Origin City Name")
        axes[1].set_ylabel("Average Delay (minutes)")
        axes[1].set_title("Average Departure Delay by Airport")
        axes[1].tick_params(axis='x', rotation=45)
        axes[1].legend()
        axes[1].grid(axis='y', linestyle='--', alpha=0.7)
        
        # Plot average delays by month
        axes[2].plot(monthly_df['flight_month'], monthly_df['average_departure_delay'], color='g', label='Average Departure Delay', marker='o')
        axes[2].set_xlabel("Flight Month")
        axes[2].set_ylabel("Average Delay (minutes)")
        axes[2].set_title("Average Departure Delay by Month")
        axes[2].legend()
        axes[2].grid(axis='y', linestyle='--', alpha=0.7)
        
        # Adjust layout to not cut off labels
        plt.tight_layout()
        plt.show()

    else:
        print("Keyspace 'flight_delay_analysis' does not exist.")

finally:
    session.shutdown()
    cluster.shutdown()
