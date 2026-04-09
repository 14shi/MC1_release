# Oceanus Folk Rising Star — 演示说明

---

## 一、任务

### 1.1 总体任务

基于 VAST Challenge 2025 MC1 提供的音乐知识图谱（16,000+ 节点），构建可视分析系统，对 Oceanus Folk 流派进行艺人职业分析、影响力传播追踪与新星预测。

### 1.2 子任务

| 编号 | 任务 | 目标 |
|------|------|------|
| Q1 | 探索 Sailor Shift 的职业生涯 | 分析她的影响来源、合作者网络、对 Oceanus Folk 社区的影响方式 |
| Q2 | 可视化 Oceanus Folk 的影响力传播 | 判断影响力是渐进式还是间歇式上升，识别受影响最深的流派和艺人 |
| Q3 | 定义"新星"画像并预测 | 对比三位代表性艺人的崛起路径，预测未来5年三位潜力新星 |

### 1.3 数据概况

- **数据文件：** MC1_graph.json（6.5 MB，NetworkX JSON 格式）
- **节点类型：** Person、MusicalGroup、Song、Album、RecordLabel
- **边类型：**
  - 创作关系：PerformerOf、ComposerOf、LyricistOf、ProducerOf
  - 影响关系：InStyleOf、CoverOf、InterpolatesFrom、DirectlySamples、LyricalReferenceTo
  - 结构关系：MemberOf、RecordedBy、DistributedBy

---

## 二、初始设计

### 2.1 系统架构

```
┌──────────────────────────────────────────┐
│       MC1_graph.json（原始知识图谱）       │
│        16,000+ 节点 · 多种边类型          │
└──────────────────┬───────────────────────┘
                   │
                   │  Python 预处理管线
                   │  (preprocess_refactor.py)
                   │
        ┌──────────┼──────────┐
        ▼          ▼          ▼
  artists.json  network.json  timeline.json
  (艺人画像     (影响力网络   (时间线
   评分排名)     拓扑数据)    弦图数据)
        └──────────┼──────────┘
                   │
                   │  D3.js v7 前端渲染
                   │  (index_dense.html)
                   ▼
┌──────────────────────────────────────────┐
│     Rising Star Analysis Workbench       │
│                                          │
│   Section 1          Section 2           │
│   职业对比            新星画像 & 预测      │
│   ┌────────────┐    ┌─────────────────┐  │
│   │ 时间线      │    │ 平行坐标 (PCP)  │  │
│   │ 影响力网络  │    │ 复合评分散点图   │  │
│   │ 雷达图      │    │                 │  │
│   └────────────┘    └─────────────────┘  │
└──────────────────────────────────────────┘
```

### 2.2 页面布局

```
┌─────────────────────────────────────────────┐
│ [Header] Compare Careers ←→ Predictions     │  固定导航栏
├─────────────────────────────────────────────┤
│                                             │
│  ┌───────────────────────────────────────┐  │
│  │  Career Timeline                      │  │  堆叠柱状图 + 累积影响力折线
│  │  三位艺人年度产出 + 影响力趋势          │  │
│  └───────────────────────────────────────┘  │
│                                             │
│  ┌───────────────────────────────────────┐  │
│  │  Influence Network                    │  │  力导向图
│  │  节点=艺人(大小=歌曲数) 边=影响关系     │  │
│  └───────────────────────────────────────┘  │
│                                             │
│  ┌─────────────────────┐                    │
│  │  Radar Chart         │                   │  六维雷达图叠加对比
│  │  六维百分位画像       │                   │
│  └─────────────────────┘                    │
│                                             │
├─────────────────────────────────────────────┤
│                                             │
│  ┌───────────────────────────────────────┐  │
│  │  Parallel Coordinates                 │  │  7轴平行坐标，支持刷选
│  │  6维度 + 趋势分                        │  │
│  └───────────────────────────────────────┘  │
│                                             │
│  ┌───────────────────────────────────────┐  │
│  │  Composite Score Scatter              │  │  X=趋势 Y=维度 Size=一致性
│  │  候选人综合定位                        │  │
│  └───────────────────────────────────────┘  │
│                                             │
└─────────────────────────────────────────────┘
```

### 2.3 评分模型设计

采用六维度嵌套乘积模型（非线性加权）来量化"新星画像"：

```
六维度
├── OF Momentum    — 近期 vs 历史产出强度
├── Quality Signal — Notable 歌曲比例稳定性
├── Influence Reach— 出向影响 + 0.8×入向影响
├── Genre Bridge   — 流派分布信息熵
├── Collaboration  — 合作者数 × (1+0.2×角色多样性)
└── Industry Traction — 唱片公司/发行覆盖

最终评分 = √(dimension × trend) × √(balance × consistency) × breadth
```

---

## 三、实施计划

### 3.1 技术栈

| 层 | 工具 | 用途 |
|----|------|------|
| 数据预处理 | **Python 3** | 知识图谱遍历、六维评分计算、百分位排名 |
| 可视化引擎 | **D3.js v7** | 5种交互式 SVG 图表渲染 |
| 前端 | **HTML + CSS + JS**（无框架） | 轻量单页应用，零构建步骤 |
| 字体/样式 | **Inter** + CSS Custom Properties | 统一视觉语言 |

### 3.2 五种可视化图表

| 图表 | D3 组件 | 对应任务 | 分析目标 |
|------|---------|---------|---------|
| Career Timeline | 堆叠柱状图 + 折线 | Q1/Q3 | 年度产出节奏与影响力增长趋势 |
| Influence Network | 力导向图 (d3-force) | Q1/Q2 | 影响力拓扑结构与传播路径 |
| Radar Profile | 雷达/蛛网图 | Q3 | 三位艺人六维画像直观对比 |
| Parallel Coordinates | 平行坐标图 + brush | Q3 | 多维度候选人筛选 |
| Composite Scatter | 散点图 | Q3 | 趋势 vs 维度的综合定位 |

### 3.3 交互设计

- **悬停 Tooltip：** 显示详细指标数据，跟随鼠标定位
- **点击聚焦：** 网络图高亮选中节点及其邻居，其余淡出至 6% 透明度
- **PCP 轴刷选：** 在任意轴上拖拽范围，实时筛选候选人子集
- **导航锚点：** 顶栏固定，点击平滑滚动至对应板块
- **动画过渡：** 150ms transition 平滑切换

---

## 四、初步结果

### 4.1 Career Timeline — 职业轨迹对比

三位对比艺人代表三种不同的崛起路径：

| 艺人 | 原型 | OF歌曲 | Notable比 | 活跃时间 |
|------|------|--------|----------|---------|
| Sailor Shift | Sustained Producer（持续产出型） | 17首 | 29.4% | 2007—2040 |
| Embers of Wrath | Influence Magnet（影响力吸引型） | 3首 | 100% | 2007—2013 |
| Orla Seabloom | Rapid Riser（快速崛起型） | 7首 | 100% | 2029（单年爆发） |

**发现：**
- Sailor Shift 跨越 33 年持续产出，累积影响力曲线稳步上升
- Embers of Wrath 仅 3 首歌但全部 Notable，停止产出后影响力仍缓慢积累（长尾效应）
- Orla Seabloom 2029 年一次性产出 7 首全 Notable 作品，爆发力最强

### 4.2 Influence Network — 影响力网络

- Sailor Shift（金色大圆）位于网络中心，与 Copper Canyon Ghosts、Ivy Echoes 等多个节点存在影响力边
- 节点大小编码 OF 歌曲数量，非对比艺人以灰色淡化显示
- 边类型覆盖 InStyleOf / CoverOf / InterpolatesFrom / DirectlySamples / LyricalReferenceTo 五种

### 4.3 Radar Profile — 六维画像对比

| 维度 | Sailor Shift | Embers of Wrath | Orla Seabloom |
|------|---|---|---|
| OF Momentum | 19.6% | 17.4% | **95.7%** |
| Quality Signal | 17.4% | **100%** | **97.8%** |
| Influence Reach | **97.8%** | **100%** | 80.4% |
| Genre Bridge | 60.9% | 58.7% | 56.5% |
| Collaboration Network | 28.3% | **84.8%** | 26.1% |
| Industry Traction | 30.4% | **78.3%** | 28.3% |

**发现：**
- Sailor Shift：影响力辐射极强，但质量和动量偏低（大量产出中精品比例不高）
- Embers of Wrath：质量、影响力、协作三项接近满分（少而精 + 强合作网络）
- Orla Seabloom：动量和质量双高（新人冲击力最强，但协作和行业认可有待提升）

### 4.4 Parallel Coordinates — 多维筛选与预测

从 44 位候选人中筛选出 Top 3 预测新星：

| 排名 | 艺人 | 综合得分 | 趋势 | 维度强度 | 突出特征 |
|------|------|---------|------|---------|---------|
| #1 | **Zacharie Martins** | 0.92 | 97.7% | 97.7% | 六维最均衡，协作网络 93.5% |
| #2 | **Yong Dong** | 0.67 | 81.4% | 88.4% | 跨流派桥梁 97.8% |
| #3 | **Filippo Pelli** | 0.62 | 90.7% | 72.1% | 协作网络 97.8%，职业跨度 8 年 |

### 4.5 Composite Score Landscape — 综合定位

- X 轴 = 趋势百分位，Y 轴 = 维度强度百分位，圆大小 = 画像一致性
- 右上角为最佳候选区域
- **Zacharie Martins** 位于右上角极点（双 97.7%），画像一致性最高
- Yong Dong 和 Filippo Pelli 位于右侧中上方
- Sailor Shift 趋势高（90.7%）但维度仅 39.5%，说明影响力广但画像不够均衡

### 4.6 核心结论

1. **三种新星路径：** 持续产出型 / 影响力吸引型 / 快速崛起型
2. **"新星"不等于产出多** — 质量一致性和协作网络同样关键
3. **预测结果：** Zacharie Martins（得分 0.92）六维最均衡，是最可能的下一位 Oceanus Folk 新星
