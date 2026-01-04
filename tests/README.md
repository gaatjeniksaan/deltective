# Deltective Tests

Comprehensive test suite for Deltective, covering unit tests, integration tests, and Azure storage tests.

## Running Tests

### Run All Tests (Excluding Azure)

```bash
# Run all tests except Azure integration (no Docker required)
pytest tests/ --ignore=tests/test_azure_integration.py -v

# With coverage report
pytest tests/ --ignore=tests/test_azure_integration.py --cov=src/deltective --cov-report=html
```

### Run Specific Test Categories

```bash
# CLI tests only
pytest tests/test_cli.py -v

# Inspector integration tests
pytest tests/test_inspector.py -v

# Insights analyzer tests
pytest tests/test_insights.py -v

# Utility function tests
pytest tests/test_utilities.py -v
```

### Run Azure Integration Tests

Azure tests require Docker to be running (uses Azurite testcontainer):

```bash
# Ensure Docker is running, then:
pytest tests/test_azure_integration.py -v
```

### Run All Tests Including Azure

```bash
# Requires Docker running
pytest tests/ -v
```

## Test Structure

### Test Files

- **test_cli.py**: CLI interface and command-line argument handling
- **test_inspector.py**: DeltaTableInspector integration tests (high-level)
- **test_insights.py**: DeltaTableAnalyzer and insight generation tests
- **test_utilities.py**: Unit tests for utility functions and data processing
- **test_azure_integration.py**: Azure storage integration with Azurite (requires Docker)

### Fixtures (conftest.py)

- `temp_delta_table`: Partitioned Delta table with multiple versions
- `simple_delta_table`: Non-partitioned simple Delta table
- `delta_table_with_history`: Table with extensive version history
- `delta_table_with_small_files`: Table with many small files for insights testing
- `azurite_container`: Azurite testcontainer for Azure storage mocking

## Coverage

Current test coverage:

- **inspector.py**: 85% (core Delta table inspection logic)
- **insights.py**: 70% (table health analysis)
- **cli.py**: 94% (CLI interface)
- **TUI components**: Low coverage (requires terminal interaction, tested manually)

View detailed coverage report:

```bash
pytest tests/ --ignore=tests/test_azure_integration.py --cov=src/deltective --cov-report=html
open htmlcov/index.html
```

## Test Philosophy

1. **High-level first**: Integration tests verify end-to-end functionality
2. **Work down to units**: Unit tests verify individual functions
3. **Real Delta tables**: Tests use actual Delta tables created with deltalake
4. **Mock external dependencies**: Azure storage mocked with Azurite testcontainer

## Adding New Tests

When adding new features:

1. Start with integration tests in `test_inspector.py`
2. Add insight detection tests in `test_insights.py` if applicable
3. Add unit tests for utility functions in `test_utilities.py`
4. Update fixtures in `conftest.py` if new test data patterns needed

## CI/CD

For CI environments without Docker:

```bash
# Run without Azure tests
pytest tests/ --ignore=tests/test_azure_integration.py -v --cov=src/deltective
```

For CI with Docker:

```bash
# Run all tests including Azure
pytest tests/ -v --cov=src/deltective
```
