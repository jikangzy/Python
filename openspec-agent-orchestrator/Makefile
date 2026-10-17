.PHONY: test smoke

test:
	python -m unittest discover -s tests -v

smoke:
	python orchestrator.py run --workspace examples/demo --change demo --adapter mock --timeout 10
