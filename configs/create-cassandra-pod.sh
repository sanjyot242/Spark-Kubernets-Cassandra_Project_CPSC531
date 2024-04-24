#!/bin/bash

# Create a namespace for Cassandra
kubectl create namespace cassandra

# Apply the Cassandra service configuration
kubectl create -f cassandra-service.yaml

# Apply the Cassandra statefulset configuration
kubectl create -f cassandra-statefulset.yaml