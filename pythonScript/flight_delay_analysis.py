from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import pandas as pd
import matplotlib.pyplot as plt

# Set up the connection to Cassandra
auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')  # adjust credentials as necessary
cluster = Cluster(['127.0.0.1'], port=9042, auth_provider=auth_provider)  # assuming port forwarding is set to 9042
session = cluster.connect()

try:
    # Attempt to retrieve the list of keyspaces to verify the connection
    rows = session.execute("SELECT keyspace_name FROM system_schema.keyspaces;")
    keyspaces = [row.keyspace_name for row in rows]
    print("Connected to Cassandra, available keyspaces:")
    for ks in keyspaces:
        print(ks)

    # Check if the 'flight_delay_analysis' keyspace exists
    if 'flight_delay_analysis' in keyspaces:
        print("Keyspace 'flight_delay_analysis' exists.")
        session.set_keyspace('flight_delay_analysis')  # Connect to the keyspace

        # Query data from a table
        query = """
        SELECT marketing_airline_network, average_departure_delay, average_arrival_delay,
               average_carrier_delay, average_late_aircraft_delay, average_nas_delay,
               average_security_delay, average_weather_delay
        FROM airline_delay_stats;
        """
        results = session.execute(query)

        # Convert results to a Pandas DataFrame
        df = pd.DataFrame(list(results))

        # Check if DataFrame is not empty and contains the required columns
        if not df.empty and 'average_departure_delay' in df.columns:
            # Plotting the data
            plt.figure(figsize=(12, 6))  # Set the figure size for better readability
            plt.bar(df['marketing_airline_network'], df['average_departure_delay'], color='b', label='Average Departure Delay')

            plt.xlabel("Marketing Airline Network")  # X-axis label
            plt.ylabel("Average Departure Delay (minutes)")  # Y-axis label
            plt.title("Average Departure Delay by Airline")  # Title of the plot
            plt.xticks(rotation=45, ha='right')  # Rotate x-axis labels for better visibility
            plt.legend()  # Show legend
            plt.tight_layout()  # Adjust layout to not cut off labels
            plt.grid(axis='y', linestyle='--', alpha=0.7)  # Add a grid for the y-axis
            plt.show()
        else:
            print("No data available to plot or missing necessary column.")

    else:
        print("Keyspace 'flight_delay_analysis' does not exist.")

finally:
    session.shutdown()
    cluster.shutdown()
