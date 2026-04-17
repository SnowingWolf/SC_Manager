.PHONY: install install-dev install-editable clean test lint format check help

help:
	@echo "SC_Manager - 可用命令:"
	@echo "  make install          - 安装生产依赖"
	@echo "  make install-dev      - 安装开发依赖"
	@echo "  make install-editable - 可编辑模式安装（开发推荐）"
	@echo "  make test             - 运行测试"
	@echo "  make lint             - 代码检查"
	@echo "  make format           - 代码格式化"
	@echo "  make check            - 语法检查"
	@echo "  make clean            - 清理缓存文件"

install:
	pip install -r requirements.txt

install-dev:
	pip install -r requirements-dev.txt

install-editable:
	pip install -e .

test:
	python3 -m pytest tests/ -v

lint:
	python3 -m flake8 sc_reader/ --max-line-length=100

format:
	python3 -m black sc_reader/ example/ --line-length=100

check:
	@echo "检查 Python 语法..."
	@python3 -m py_compile sc_reader/*.py
	@echo "检查导入..."
	@python3 -c "from sc_reader import SCReader, TableSpec; print('✓ 导入成功')"

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
	rm -rf build/ dist/
