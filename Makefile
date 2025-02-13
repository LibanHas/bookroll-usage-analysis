.PHONY: build up down restart logs ps clean prune help up-% down-% restart-% logs-% build-%

# Default target
.DEFAULT_GOAL := help

# Colors for help message
GREEN  := $(shell tput -Txterm setaf 2)
YELLOW := $(shell tput -Txterm setaf 3)
WHITE  := $(shell tput -Txterm setaf 7)
RESET  := $(shell tput -Txterm sgr0)

help:
	@echo "Available targets:"
	@echo "${YELLOW}build${RESET}                  - Build all Docker images"
	@echo "${YELLOW}build-<service>${RESET}        - Build specific service (e.g., build-school_dashboard)"
	@echo "${YELLOW}up${RESET}                     - Start all services in detached mode"
	@echo "${YELLOW}up-<service>${RESET}           - Start specific service (e.g., up-db)"
	@echo "${YELLOW}down${RESET}                   - Stop and remove all containers"
	@echo "${YELLOW}down-<service>${RESET}         - Stop specific service (e.g., down-memcached)"
	@echo "${YELLOW}restart${RESET}                - Restart all services"
	@echo "${YELLOW}restart-<service>${RESET}      - Restart specific service"
	@echo "${YELLOW}logs${RESET}                   - View logs from all services"
	@echo "${YELLOW}logs-<service>${RESET}         - View logs from specific service"
	@echo "${YELLOW}ps${RESET}                     - List running containers"
	@echo "${YELLOW}clean${RESET}                  - Remove stopped containers"
	@echo "${YELLOW}prune${RESET}                  - Remove all unused Docker resources"
	@echo ""
	@echo "Available services: school_dashboard, db, memcached"

build:
	docker-compose build --no-cache

# Pattern rule for building specific services
build-%:
	docker-compose build --no-cache $*

up:
	docker-compose up -d

# Pattern rule for starting specific services
up-%:
	docker-compose up -d $*

down:
	docker-compose down

# Pattern rule for stopping specific services
down-%:
	docker-compose stop $*
	docker-compose rm -f $*

restart:
	docker-compose restart

# Pattern rule for restarting specific services
restart-%:
	docker-compose restart $*

logs:
	docker-compose logs -f

# Pattern rule for viewing logs of specific services
logs-%:
	docker-compose logs -f $*

ps:
	docker-compose ps

clean:
	docker-compose rm -f

prune:
	docker system prune -af
	docker volume prune -f
