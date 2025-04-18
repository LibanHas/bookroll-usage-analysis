.PHONY: build up down restart logs ps clean prune help up-% down-% restart-% logs-% build-% prod-build prod-up prod-down prod-restart prod-logs prod-ps prod-clean

# Default target
.DEFAULT_GOAL := help

# Docker Compose Files
COMPOSE_DEV = docker-compose.yml
COMPOSE_PROD =  -f docker-compose.prod.yml

# Colors for help message
GREEN  := $(shell tput -Txterm setaf 2)
YELLOW := $(shell tput -Txterm setaf 3)
WHITE  := $(shell tput -Txterm setaf 7)
RESET  := $(shell tput -Txterm sgr0)

help:
	@echo "Available targets:"
	@echo "${YELLOW}Development Commands:${RESET}"
	@echo "${YELLOW}build${RESET}                  - Build all Docker images for development"
	@echo "${YELLOW}build-<service>${RESET}        - Build specific service (e.g., build-school_dashboard)"
	@echo "${YELLOW}up${RESET}                     - Start all services in detached mode (development)"
	@echo "${YELLOW}up-<service>${RESET}           - Start specific service (e.g., up-db)"
	@echo "${YELLOW}down${RESET}                   - Stop and remove all containers (development)"
	@echo "${YELLOW}down-<service>${RESET}         - Stop specific service (e.g., down-memcached)"
	@echo "${YELLOW}restart${RESET}                - Restart all services (development)"
	@echo "${YELLOW}restart-<service>${RESET}      - Restart specific service"
	@echo "${YELLOW}logs${RESET}                   - View logs from all services"
	@echo "${YELLOW}logs-<service>${RESET}         - View logs from specific service"
	@echo "${YELLOW}ps${RESET}                     - List running containers"
	@echo ""
	@echo "${GREEN}Production Commands:${RESET}"
	@echo "${GREEN}prod-build${RESET}              - Build all Docker images for production"
	@echo "${GREEN}prod-up${RESET}                 - Start all services in production mode"
	@echo "${GREEN}prod-down${RESET}               - Stop and remove all production containers"
	@echo "${GREEN}prod-restart${RESET}            - Restart all production services"
	@echo "${GREEN}prod-logs${RESET}               - View logs from all production services"
	@echo "${GREEN}prod-ps${RESET}                 - List running production containers"
	@echo "${GREEN}prod-clean${RESET}              - Remove stopped production containers"
	@echo ""
	@echo "${YELLOW}Maintenance Commands:${RESET}"
	@echo "${YELLOW}clean${RESET}                  - Remove stopped containers"
	@echo "${YELLOW}prune${RESET}                  - Remove all unused Docker resources"
	@echo ""
	@echo "Available services: school_dashboard, db, memcached"

# Development commands
build:
	docker-compose -f $(COMPOSE_DEV) build --no-cache

build-%:
	docker-compose -f $(COMPOSE_DEV) build --no-cache $*

up:
	docker-compose -f $(COMPOSE_DEV) up -d

up-%:
	docker-compose -f $(COMPOSE_DEV) up -d $*

down:
	docker-compose -f $(COMPOSE_DEV) down

down-%:
	docker-compose -f $(COMPOSE_DEV) stop $*
	docker-compose -f $(COMPOSE_DEV) rm -f $*

restart:
	docker-compose -f $(COMPOSE_DEV) restart

restart-%:
	docker-compose -f $(COMPOSE_DEV) restart $*

logs:
	docker-compose -f $(COMPOSE_DEV) logs -f

logs-%:
	docker-compose -f $(COMPOSE_DEV) logs -f $*

ps:
	docker-compose -f $(COMPOSE_DEV) ps

# Production commands
prod-build:
	docker-compose $(COMPOSE_PROD) build --no-cache

prod-up:
	docker-compose $(COMPOSE_PROD) up -d

prod-down:
	docker-compose $(COMPOSE_PROD) down

prod-restart:
	docker-compose $(COMPOSE_PROD) restart

prod-logs:
	docker-compose $(COMPOSE_PROD) logs -f

prod-ps:
	docker-compose $(COMPOSE_PROD) ps

prod-clean:
	docker-compose $(COMPOSE_PROD) rm -f

# Maintenance commands
clean:
	docker-compose -f $(COMPOSE_DEV) rm -f

prune:
	docker system prune -af
	docker volume prune -f
