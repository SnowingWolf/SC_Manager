# SC_Manager 安装指南

## 快速安装

### 方式 1：可编辑模式安装（推荐用于开发）
```bash
pip install -e .
```

### 方式 2：使用 requirements.txt
```bash
pip install -r requirements.txt
```

### 方式 3：使用 Makefile
```bash
make install-editable  # 可编辑模式
make install          # 仅安装依赖
make install-dev      # 安装开发依赖
```

## 验证安装

```bash
python3 -c "from sc_reader import SCReader, TableSpec; print('安装成功')"
```

或使用 Makefile：
```bash
make check
```

## 开发工具

```bash
make test      # 运行测试
make lint      # 代码检查
make format    # 代码格式化
make clean     # 清理缓存
make help      # 查看所有命令
```

## 依赖说明

核心依赖：
- pymysql: MySQL 数据库连接
- pandas: 数据处理
- matplotlib, seaborn, plotly: 数据可视化
- numpy: 数值计算
- sqlalchemy: SQL 工具包
- pyarrow: Parquet 文件支持

开发依赖（可选）：
- pytest: 测试框架
- black: 代码格式化
- flake8: 代码检查
- mypy: 类型检查
