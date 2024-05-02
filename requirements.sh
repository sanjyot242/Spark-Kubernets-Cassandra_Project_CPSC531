#!/bin/bash

# Check if Homebrew is installed
if ! command -v brew &> /dev/null; then
    echo "Homebrew not found. Installing Homebrew..."
    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
fi

# Install Minikube
echo "Installing Minikube..."
brew install minikube

# Install kubectl
echo "Installing kubectl..."
brew install kubectl


# Install Helm
echo "Installing Helm..."
brew install helm

# Install Maven
echo "Installing Maven..."
brew install maven


# Add Bitnami Helm repository
echo "Adding Bitnami Helm repository..."
helm repo add bitnami https://charts.bitnami.com/bitnami

echo "Installation complete."