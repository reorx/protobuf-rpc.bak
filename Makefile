test:
	protoc --python_out=test test/test_suite.proto
	trial test/test_service.py
