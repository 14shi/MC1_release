# Oceanus Folk — Rising Star Analysis Workbench

MC1 音乐知识图谱子集（Oceanus Folk）上的新星识别、多维刻画与交互可视化。离线管道产出 JSON，浏览器端用 D3.js 渲染。

## 仓库结构

| 路径 | 说明 |
|------|------|
| `MC1_graph.json` | 原始知识图谱数据 |
| `viz/preprocess_refactor.py` | 预处理脚本：图遍历、六维评分、导出 JSON |
| `viz/index_dense.html` | 主看板（`index.html` 会跳转至此） |
| `viz/data/` | 预处理生成的 JSON（已包含，可直接使用） |

## 环境

- Python 3.x

## 快速开始

```bash
# 1. 重新生成数据（可选，viz/data/ 下已有现成数据可跳过）
cd viz
python preprocess_refactor.py

# 2. 启动本地服务
cd ..
python -m http.server 8080 --directory viz
# 浏览器访问 http://127.0.0.1:8080/index_dense.html
```

## 作者

维护者：[@14shi](https://github.com/14shi)
