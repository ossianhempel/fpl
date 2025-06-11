
# run just "make" to run the first label in the file
# run "make <label>" to execute a specific label


.PHONY: all setup lint test clean # tells make these are not files, just commands

.PHONY: quality style test docs

check_dirs := examples src tests utils

# Check code quality of the source code
quality:
	ruff check $(check_dirs)
	ruff format --check $(check_dirs)

# Format source code automatically
style:
	ruff check $(check_dirs) --fix
	ruff format $(check_dirs)
	
# Run smolagents tests
test:
	pytest ./tests/