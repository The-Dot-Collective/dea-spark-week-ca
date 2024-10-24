all: format linting

format:
	isort .
	black .

linting:
	pylint .

testcov:
	pytest --cov-report=html:cov_html --cov=dea_api_ingestion tests/test_main.py -v
