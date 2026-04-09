# Oceanus Folk — Rising Star Analysis Workbench

MC1 音乐知识图谱子集（Oceanus Folk）上的新星识别、多维刻画与交互可视化。离线管道产出 JSON，浏览器端用 D3.js 渲染。

## 仓库结构

| 路径 | 说明 |
|------|------|
| `MC1_graph.json` | 原始图数据（根目录，与 `viz/preprocess_refactor.py` 约定一致） |
| `viz/preprocess_refactor.py` | 预处理、评分、导出 `artists.json` / `network.json` |
| `viz/index_dense.html` | 主看板（`index.html` 会跳转至此） |
| `viz/data/` | 管道生成的 JSON（可重新运行脚本覆盖） |
| `tests/` | Playwright 冒烟测试 |
| `docs/` | 图文说明（截图可放在 `docs/images/`） |

## 环境

- Python 3.x（运行预处理）
- Node.js（安装 Playwright 与运行测试）

## 快速开始

```bash
# 1. 重新生成数据（可选，已有 viz/data/*.json 时可跳过）
cd viz
python preprocess_refactor.py

# 2. 本地打开看板
cd ..
python -m http.server 4173 --directory viz
# 浏览器访问 http://127.0.0.1:4173/index_dense.html
```

## 测试

```bash
npm ci
npx playwright install chromium
npx playwright test
```

## 作者

维护者：[@14shi](https://github.com/14shi)

## 许可

数据与课程材料归属以课程/数据集要求为准；代码按仓库许可使用。
