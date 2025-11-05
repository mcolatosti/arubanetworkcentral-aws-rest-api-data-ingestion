#!/bin/bash

# Deploy script for the Aruba Central CDK Ingestion project

echo "🔧 Deploying Aruba Central CDK Ingestion Stack"
echo "================================================="

# Check if AWS CLI is configured
if ! aws sts get-caller-identity &> /dev/null; then
    echo "❌ AWS CLI is not configured. Please configure it using 'aws configure'."
    exit 1
fi

# Install CDK if not already installed
if ! command -v cdk &> /dev/null; then
    echo "🔍 Installing AWS CDK..."
    npm install -g aws-cdk
fi

# Build the project
echo "🔨 Building the CDK project..."
npm run build

# Deploy the stack
echo "🚀 Deploying the stack..."
cdk deploy --require-approval never

echo "✅ Deployment completed successfully!"
echo "💡 You can view your stack in the AWS Management Console."