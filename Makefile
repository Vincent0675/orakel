# Orakel — Project Makefile
# Run `make help` to see all available targets.

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PYTHON       := uv run python
PYTEST       := uv run pytest
STREAMLIT    := uv run streamlit
DAGSTER      := uv run dagster
MINIO_IMAGE  := minio/minio:latest
MINIO_PORT   := 9000
MINIO_CONSOLE := 9001
MLFLOW_PORT  := 5000
DAGSTER_PORT := 3000
DASHBOARD_PORT := 8501

# Color output (disable with NO_COLOR=1)
ifdef NO_COLOR
    RESET  :=
    BOLD   :=
    GREEN  :=
    YELLOW :=
    RED    :=
else
    RESET  := \033[0m
    BOLD   := \033[1m
    GREEN  := \033[32m
    YELLOW := \033[33m
    RED    := \033[31m
endif

.DEFAULT_GOAL := help

.PHONY: help setup run stop logs test test-fast coverage pipeline dashboard clean docker-build docker-up docker-down docker-logs docker-ps

# ---------------------------------------------------------------------------
# Help
# ---------------------------------------------------------------------------
help: ## Show this help message
	@echo "$(BOLD)Orakel — WoW Mythic+ analytics pipeline$(RESET)"
	@echo ""
	@echo "$(BOLD)Local development (no Docker):$(RESET)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	    awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-15s$(RESET) %s\n", $$1, $$2}'
	@echo ""
	@echo "$(BOLD)Docker-based deployment:$(RESET)"
	@echo "  $(GREEN)docker-build$(RESET)    Build all Docker images"
	@echo "  $(GREEN)docker-up$(RESET)       Start all services in Docker (background)"
	@echo "  $(GREEN)docker-down$(RESET)     Stop all Docker services"
	@echo "  $(GREEN)docker-logs$(RESET)     Follow logs from all Docker services"
	@echo "  $(GREEN)docker-ps$(RESET)       Show running Docker containers"
	@echo ""
	@echo "$(BOLD)Examples:$(RESET)"
	@echo "  make setup        # First time: install deps and create .env"
	@echo "  make run          # Start MinIO, MLflow, Dagster in background (host)"
	@echo "  make test         # Run the full test suite"
	@echo "  make docker-up    # Start everything in Docker (one command)"

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
setup: ## Install dependencies and create .env from .env.example
	@echo "$(GREEN)▶ Installing dependencies...$(RESET)"
	uv sync
	@if [ ! -f .env ]; then \
	    echo "$(GREEN)▶ Creating .env from .env.example...$(RESET)"; \
	    cp .env.example .env; \
	    echo "$(YELLOW)⚠ Edit .env with your WCL_CLIENT_ID and WCL_CLIENT_SECRET$(RESET)"; \
	else \
	    echo "$(YELLOW)⚠ .env already exists, skipping$(RESET)"; \
	fi
	@echo "$(GREEN)✓ Setup complete. Run 'make run' to start the pipeline.$(RESET)"

# ---------------------------------------------------------------------------
# Run / Stop / Logs
# ---------------------------------------------------------------------------
run: ## Start MinIO, MLflow, and Dagster in background
	@echo "$(GREEN)▶ Starting MinIO...$(RESET)"
	@docker compose up -d minio
	@echo "$(GREEN)▶ Waiting for MinIO to be ready...$(RESET)"
	@until docker exec orakel-minio curl -sf http://localhost:9000/minio/health/live > /dev/null 2>&1; do \
	    sleep 1; \
	done
	@echo "$(GREEN)✓ MinIO ready at http://localhost:$(MINIO_PORT)$(RESET)"
	@echo "  $(BOLD)Console:$(RESET) http://localhost:$(MINIO_CONSOLE) (orakel / orakel123)"
	@echo ""
	@echo "$(GREEN)▶ Starting MLflow tracking server...$(RESET)"
	@mkdir -p mlruns
	@$(PYTHON) -m mlflow server \
	    --backend-store-uri ./mlruns \
	    --default-artifact-root ./mlruns \
	    --host 0.0.0.0 \
	    --port $(MLFLOW_PORT) > /tmp/orakel-mlflow.log 2>&1 &
	@echo "  $(BOLD)MLflow:$(RESET) http://localhost:$(MLFLOW_PORT) (PID $$!)"
	@echo ""
	@echo "$(GREEN)▶ Starting Dagster webserver...$(RESET)"
	@$(DAGSTER) dev -m orakel.pipeline.definitions -p $(DAGSTER_PORT) > /tmp/orakel-dagster.log 2>&1 &
	@echo "  $(BOLD)Dagster:$(RESET) http://localhost:$(DAGSTER_PORT) (PID $$!)"
	@echo ""
	@echo "$(GREEN)✓ All services running.$(RESET)"
	@echo "  $(YELLOW)Tip: 'make logs' to follow service output, 'make stop' to shut down.$(RESET)"

stop: ## Stop all background services (MinIO, MLflow, Dagster)
	@echo "$(YELLOW)▶ Stopping MLflow and Dagster...$(RESET)"
	@-pkill -f "mlflow server" 2>/dev/null || true
	@-pkill -f "dagster dev" 2>/dev/null || true
	@echo "$(YELLOW)▶ Stopping MinIO...$(RESET)"
	@docker compose down
	@echo "$(GREEN)✓ All services stopped.$(RESET)"

logs: ## Follow logs from background services
	@echo "$(BOLD)MLflow:$(RESET) /tmp/orakel-mlflow.log"
	@echo "$(BOLD)Dagster:$(RESET) /tmp/orakel-dagster.log"
	@echo ""
	@tail -f /tmp/orakel-mlflow.log /tmp/orakel-dagster.log

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
test: ## Run the full test suite (146 tests)
	@echo "$(GREEN)▶ Running full test suite...$(RESET)"
	@$(PYTEST) tests/ -v

test-fast: ## Run Tier 1 tests only (no Spark, faster)
	@echo "$(GREEN)▶ Running Tier 1 tests (no Spark)...$(RESET)"
	@$(PYTEST) tests/ -v -m "not spark"

coverage: ## Generate HTML coverage report
	@echo "$(GREEN)▶ Generating coverage report...$(RESET)"
	@$(PYTEST) tests/ --cov=orakel --cov-report=html --cov-report=term-missing
	@echo "$(GREEN)✓ HTML report: htmlcov/index.html$(RESET)"

# ---------------------------------------------------------------------------
# Pipeline / Dashboard
# ---------------------------------------------------------------------------
pipeline: ## Run the Dagster pipeline (one-shot materialize all assets)
	@echo "$(GREEN)▶ Materializing all Dagster assets...$(RESET)"
	@$(DAGSTER) asset materialize --all -m orakel.pipeline.definitions

dashboard: ## Launch the Streamlit dashboard at localhost:8501
	@echo "$(GREEN)▶ Starting Streamlit dashboard...$(RESET)"
	@$(STREAMLIT) run dashboard/app.py --server.port=$(DASHBOARD_PORT)

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------
clean: ## Remove caches, build artifacts, and local data
	@echo "$(YELLOW)▶ Cleaning caches and data...$(RESET)"
	@find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	@find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	@find . -type d -name .mypy_cache -exec rm -rf {} + 2>/dev/null || true
	@find . -type d -name htmlcov -exec rm -rf {} + 2>/dev/null || true
	@rm -f .coverage
	@rm -f /tmp/orakel-mlflow.log /tmp/orakel-dagster.log
	@echo "$(GREEN)✓ Cleaned.$(RESET)"

clean-data: clean ## Stop services AND remove MinIO data + MLflow runs
	@echo "$(RED)▶ Removing all local data (MinIO + MLflow)...$(RESET)"
	@docker compose down -v
	@rm -rf data/ mlruns/ mlflow.db
	@echo "$(GREEN)✓ All data removed.$(RESET)"

# ---------------------------------------------------------------------------
# Docker targets — full stack as containers
# ---------------------------------------------------------------------------
docker-build: ## Build all Docker images (base, dagster, dashboard)
	@echo "$(GREEN)▶ Building Docker images...$(RESET)"
	@docker compose build
	@echo "$(GREEN)✓ Images built. Run 'make docker-up' to start the stack.$(RESET)"

docker-up: ## Start all services in Docker (MinIO, MLflow, Dagster, Dashboard)
	@echo "$(GREEN)▶ Starting Orakel stack in Docker...$(RESET)"
	@docker compose up -d --build
	@echo ""
	@echo "$(GREEN)✓ Stack is starting. Services:$(RESET)"
	@echo "  $(BOLD)MinIO$(RESET)       http://localhost:9001 (orakel / orakel123)"
	@echo "  $(BOLD)MLflow$(RESET)      http://localhost:5000"
	@echo "  $(BOLD)Dagster$(RESET)     http://localhost:3000"
	@echo "  $(BOLD)Dashboard$(RESET)   http://localhost:8501"
	@echo ""
	@echo "$(YELLOW)Tip: 'make docker-logs' to follow service output, 'make docker-down' to stop.$(RESET)"

docker-down: ## Stop all Docker services (data volumes preserved)
	@echo "$(YELLOW)▶ Stopping Docker stack...$(RESET)"
	@docker compose down
	@echo "$(GREEN)✓ All services stopped.$(RESET)"

docker-logs: ## Follow logs from all Docker services
	@docker compose logs -f

docker-ps: ## Show running Docker containers
	@docker compose ps
