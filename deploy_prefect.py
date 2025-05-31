#!/usr/bin/env python3
"""
Prefect Deployment Script for Clinical Trials ETL

Sets up and deploys Prefect workflows for scheduled execution.
"""

import os
import time
import subprocess
import sys
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

def wait_for_prefect_server(max_retries=30, delay=5):
    """Wait for Prefect server to be ready"""
    print("Waiting for Prefect server to be ready...")
    
    for i in range(max_retries):
        try:
            result = subprocess.run(
                ["prefect", "server", "health-check"],
                capture_output=True,
                text=True,
                timeout=10
            )
            if result.returncode == 0:
                print("✅ Prefect server is ready!")
                return True
        except Exception as e:
            pass
        
        print(f"⏳ Waiting for server... ({i+1}/{max_retries})")
        time.sleep(delay)
    
    print("❌ Prefect server not ready after waiting")
    return False

def setup_work_pool():
    """Create work pool if it doesn't exist"""
    print("Setting up work pool...")
    
    try:
        result = subprocess.run([
            "prefect", "work-pool", "create", 
            "--type", "process", 
            "default-agent-pool"
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ Work pool created/verified")
            return True
        elif "already exists" in result.stderr.lower():
            print("✅ Work pool already exists")
            return True
        else:
            print(f"❌ Failed to create work pool: {result.stderr}")
            return False
    except Exception as e:
        print(f"❌ Error creating work pool: {e}")
        return False

def deploy_flow():
    """Deploy the clinical trials ETL flow"""
    print("Deploying Clinical Trials ETL flow...")
    
    try:
        # Import and deploy the flow
        from prefect_flows import create_deployment
        
        deployment = create_deployment()
        deployment_id = deployment.apply()
        
        print(f"✅ Deployment created with ID: {deployment_id}")
        return True
        
    except Exception as e:
        print(f"❌ Failed to deploy flow: {e}")
        return False

def start_deployment():
    """Start the deployment schedule"""
    print("Starting deployment schedule...")
    
    try:
        result = subprocess.run([
            "prefect", "deployment", "run", 
            "clinical-trials-etl/clinical-trials-daily-etl"
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ Deployment schedule started")
            return True
        else:
            print(f"❌ Failed to start deployment: {result.stderr}")
            return False
    except Exception as e:
        print(f"❌ Error starting deployment: {e}")
        return False

def show_status():
    """Show Prefect status and deployment info"""
    print("\n" + "="*60)
    print("PREFECT CLINICAL TRIALS ETL STATUS")
    print("="*60)
    
    try:
        # Show deployments
        print("\n📋 Deployments:")
        subprocess.run(["prefect", "deployment", "ls"], timeout=10)
        
        # Show work pools
        print("\n🏊 Work Pools:")
        subprocess.run(["prefect", "work-pool", "ls"], timeout=10)
        
        # Show recent flow runs
        print("\n🏃 Recent Flow Runs:")
        subprocess.run(["prefect", "flow-run", "ls", "--limit", "5"], timeout=10)
        
    except Exception as e:
        print(f"Error getting status: {e}")

def main():
    """Main deployment function"""
    print("🚀 Clinical Trials ETL Prefect Deployment")
    print("="*50)
    
    # Check if we're in Docker or local environment
    in_docker = os.path.exists('/app/workspace')
    if in_docker:
        os.chdir('/app/workspace')
    
    # Step 1: Wait for Prefect server
    if not wait_for_prefect_server():
        print("❌ Cannot proceed without Prefect server")
        sys.exit(1)
    
    # Step 2: Setup work pool
    if not setup_work_pool():
        print("❌ Failed to setup work pool")
        sys.exit(1)
    
    # Step 3: Deploy flow
    if not deploy_flow():
        print("❌ Failed to deploy flow")
        sys.exit(1)
    
    # Step 4: Show status
    show_status()
    
    print("\n" + "="*60)
    print("✅ DEPLOYMENT COMPLETED SUCCESSFULLY!")
    print("="*60)
    print("🌐 Prefect UI: http://localhost:4200")
    print("📊 You can now monitor and manage your ETL pipelines via the web UI")
    print("\nTo run the ETL immediately:")
    print("  prefect deployment run clinical-trials-etl/clinical-trials-daily-etl")
    print("\nTo check status:")
    print("  prefect deployment ls")
    print("  prefect flow-run ls")

if __name__ == "__main__":
    main() 