#!/bin/bash

# Complete DNSE Data Pipeline Startup Script
# Author: David
# Description: Manages the 3-step DNSE data pipeline

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}🚀 DNSE Data Pipeline Manager${NC}"
echo -e "${BLUE}========================================${NC}"

# Function to display pipeline steps
show_pipeline() {
    echo -e "\n${YELLOW}📊 Pipeline Architecture:${NC}"
    echo -e "STEP 0: ${GREEN}messages.json${NC} → Kafka topic '${YELLOW}dnse.raw${NC}'"
    echo -e "STEP 1: ${GREEN}Transform${NC} '${YELLOW}dnse.raw${NC}' → '${YELLOW}dnse.transform${NC}'"
    echo -e "STEP 2: ${GREEN}Sink${NC} '${YELLOW}dnse.transform${NC}' → ${YELLOW}PostgreSQL${NC}"
    echo -e "\n${BLUE}📋 Data Types:${NC}"
    echo -e "• ${GREEN}SI${NC} - Stock Info (company information, prices)"
    echo -e "• ${GREEN}ST${NC} - Stock Tick (trade data)"
    echo -e "• ${GREEN}TP${NC} - Top Price (bid/ask order book)"
    echo -e "• ${GREEN}PB${NC} - Price Board (price changes)"
    echo -e "• ${GREEN}OH${NC} - OHLC (candlestick data)"
}

# Function to check requirements
check_requirements() {
    echo -e "\n${BLUE}🔍 Checking requirements...${NC}"
    
    if ! command -v docker &> /dev/null; then
        echo -e "${RED}❌ Docker is not installed${NC}"
        exit 1
    fi
    
    if ! command -v docker-compose &> /dev/null; then
        echo -e "${RED}❌ Docker Compose is not installed${NC}"
        exit 1
    fi
    
    if [ ! -f "messages.json" ]; then
        echo -e "${RED}❌ messages.json file not found${NC}"
        exit 1
    fi
    
    echo -e "${GREEN}✅ All requirements satisfied${NC}"
}

# Function to start the pipeline
start_pipeline() {
    echo -e "\n${BLUE}🚀 Starting DNSE Pipeline...${NC}"
    
    # Pull latest images
    echo -e "${YELLOW}📦 Pulling Docker images...${NC}"
    docker-compose pull
    
    # Build application images
    echo -e "${YELLOW}🔧 Building application...${NC}"
    docker-compose build
    
    # Start infrastructure first (PostgreSQL, Kafka)
    echo -e "${YELLOW}🗄️ Starting infrastructure...${NC}"
    docker-compose up -d postgres redpanda
    
    # Wait for services to be healthy
    echo -e "${YELLOW}⏳ Waiting for services to be ready...${NC}"
    sleep 30
    
    # Start pipeline services
    echo -e "${YELLOW}📊 Starting pipeline services...${NC}"
    docker-compose up -d
    
    echo -e "\n${GREEN}✅ Pipeline started successfully!${NC}"
    show_status
}

# Function to show status
show_status() {
    echo -e "\n${BLUE}📊 Service Status:${NC}"
    docker-compose ps
    
    echo -e "\n${BLUE}🌐 Access Points:${NC}"
    echo -e "• PostgreSQL: ${YELLOW}localhost:5432${NC} (user: david, db: DavidDB)"
    echo -e "• Kafka: ${YELLOW}localhost:19092${NC}"
    echo -e "• Redpanda Console: ${YELLOW}http://localhost:8080${NC}"
    
    echo -e "\n${BLUE}📋 Useful Commands:${NC}"
    echo -e "• View logs: ${YELLOW}docker-compose logs -f [service_name]${NC}"
    echo -e "• Check DB: ${YELLOW}docker exec -it postgres_db psql -U david -d DavidDB${NC}"
    echo -e "• Stop pipeline: ${YELLOW}./start_pipeline.sh stop${NC}"
}

# Function to stop the pipeline
stop_pipeline() {
    echo -e "\n${RED}🛑 Stopping DNSE Pipeline...${NC}"
    docker-compose down
    echo -e "${GREEN}✅ Pipeline stopped${NC}"
}

# Function to view logs
view_logs() {
    echo -e "\n${BLUE}📋 Available services for logs:${NC}"
    echo -e "• ${YELLOW}step0_dnse_producer${NC} - DNSE data producer (messages.json)"
    echo -e "• ${YELLOW}step1_transform${NC} - DNSE data transformer"
    echo -e "• ${YELLOW}step2_sink${NC} - PostgreSQL sink"
    echo -e "• ${YELLOW}postgres${NC} - Database"
    echo -e "• ${YELLOW}redpanda${NC} - Message broker"
    echo -e "• ${YELLOW}redpanda-console${NC} - Kafka web UI"
    
    read -p "Enter service name (or 'all' for all services): " service
    
    if [ "$service" = "all" ]; then
        docker-compose logs -f
    else
        docker-compose logs -f "$service"
    fi
}

# Function to clean up
cleanup() {
    echo -e "\n${RED}🧹 Cleaning up (removes volumes and data)...${NC}"
    read -p "Are you sure? This will delete all data (y/N): " confirm
    
    if [ "$confirm" = "y" ] || [ "$confirm" = "Y" ]; then
        docker-compose down -v
        docker system prune -f
        echo -e "${GREEN}✅ Cleanup completed${NC}"
    else
        echo -e "${YELLOW}❌ Cleanup cancelled${NC}"
    fi
}

# Function to test pipeline
test_pipeline() {
    echo -e "\n${BLUE}🧪 Testing DNSE Pipeline...${NC}"
    
    # Check if services are running
    echo -e "${YELLOW}📊 Checking service status...${NC}"
    docker-compose ps
    
    # Check Kafka topics
    echo -e "\n${YELLOW}📋 Checking Kafka topics...${NC}"
    docker exec redpanda_broker rpk topic list || echo "❌ Could not list topics"
    
    # Check database tables
    echo -e "\n${YELLOW}🗄️ Checking database tables...${NC}"
    docker exec -it postgres_db psql -U david -d DavidDB -c "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';" || echo "❌ Could not check database"
    
    # Check data in topics
    echo -e "\n${YELLOW}📊 Checking data in dnse.raw topic (last 5 messages)...${NC}"
    docker exec redpanda_broker rpk topic consume dnse.raw --num 5 --offset -5 || echo "❌ No data in dnse.raw"
    
    echo -e "\n${YELLOW}📊 Checking data in dnse.transform topic (last 5 messages)...${NC}"
    docker exec redpanda_broker rpk topic consume dnse.transform --num 5 --offset -5 || echo "❌ No data in dnse.transform"
}

# Main menu
main_menu() {
    show_pipeline
    echo -e "\n${BLUE}📋 Available Commands:${NC}"
    echo -e "1. ${GREEN}start${NC}   - Start the complete pipeline"
    echo -e "2. ${YELLOW}status${NC}  - Show pipeline status"
    echo -e "3. ${BLUE}logs${NC}    - View service logs"
    echo -e "4. ${BLUE}test${NC}    - Test pipeline functionality"
    echo -e "5. ${RED}stop${NC}    - Stop the pipeline"
    echo -e "6. ${RED}cleanup${NC} - Clean up (removes data)"
    echo -e "7. ${GREEN}help${NC}    - Show this menu"
}

# Handle command line arguments
case "${1:-help}" in
    "start")
        check_requirements
        start_pipeline
        ;;
    "stop")
        stop_pipeline
        ;;
    "status")
        show_status
        ;;
    "logs")
        view_logs
        ;;
    "test")
        test_pipeline
        ;;
    "cleanup")
        cleanup
        ;;
    "help")
        main_menu
        ;;
    *)
        echo -e "${RED}❌ Unknown command: $1${NC}"
        main_menu
        ;;
esac 