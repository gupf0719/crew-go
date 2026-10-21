---
name: comment-audit
description: crew-go 校验**存量**代码注释/godoc 是否与代码一致的工作流——注释是文档错误的上游(v2 文档体系 100+ HIGH 很多从注释 drift 开始)。改大量符号/删改 API/重命名后、commit 触及大量注释、或用户说"校验注释/审注释/查注释 drift/注释和代码对不对/godoc 准不准"时用。三层方法:第1层 commentaudit 脚本(机械对账,可挂 Stop hook gate)/第2层契约 vs 实现(LLM 读语境)/第3层语义偏差(引用 doc-writing)。**核心铁律:机械层只产"候选",判真假必须看语义语境,绝不单独判错**(否则就是"看代码无防御就报缺陷"的过度防御误判)。区别于 doc-writing(写时防偏差,生成模式);本 skill 是事后校验存量(判别模式)。不要跳过——注释 drift 会传播成文档 HIGH。
---

# crew-go 注释/godoc 校验纪律

**核心问题**:文档错误很多**从注释开始**——godoc 写错/过时,文档抄 godoc,错误传播。在注释层堵,比在文档层抓更早、更便宜。注释校验和 [doc-writing](../doc-writing/SKILL.md) 互补:doc-writing 管"写文档时防偏差"(生成模式),本 skill 管"事后校验存量注释/godoc"(判别模式)。

## 为什么注释校验 ≠ 文档校验:它有 ground truth

文档离代码远,校验靠"读+理解找偏差"。**注释贴着代码,代码就是 ground truth**——所以主轴是**用代码反向对账注释**,而非通读注释找语病。大量工作可机械化,信噪比远高于文档审计。这是第 1 层脚本能成立的根基。

## 三层方法(ROI 从高到低,先做机械的)

| 层 | 抓什么 | 谁来做 | 能否当 gate |
|---|---|---|---|
| **第 1 层 机械对账** | 失效符号引用 / 数字声明冲突 / 失效路径行号 | `commentaudit` 脚本 | ✅ 可挂 Stop hook |
| **第 2 层 契约 vs 实现** | godoc 声明的行为("自动 X"/"跳过 Z")是否真做 | LLM 抽约束→读实现验证 | ❌ 人/LLM 判定 |
| **第 3 层 语义偏差** | 夸大 / 范围错配 / "打算做"写成"已做" | 引用 [doc-writing](../doc-writing/SKILL.md) 6 类偏差,不重写 | ❌ 多轮 audit |

## 第 1 层:`commentaudit` 脚本

源码 `.claude/scripts/commentaudit/main.go`(`go/ast` 解析,符号检查比 grep 准——能区分定义点 vs 引用点)。

### 跑法

```bash
go build -o /tmp/commentaudit .claude/scripts/commentaudit/main.go
/tmp/commentaudit -root .                    # 全仓(默认跳 _test.go)
/tmp/commentaudit -root . -include-tests     # 含测试文件注释
/tmp/commentaudit -files a.go,b.go           # 只查指定文件注释(hook 增量,符号表仍全仓建)
/tmp/commentaudit -root . -warn=false        # 只看 ERROR(关掉数字冲突 WARN)
```

### 三类检查 + 分级

- **失效符号引用(ERROR)**:注释里反引号单标识符(``` `WithModel` ```)是否在全仓符号表。失效 = 重命名/删 API 没同步注释。
- **失效路径/行号(ERROR)**:含目录的 `pkg/xxx.go:行号` 与 `doc/*.md` 链接是否存在、行号是否越界。误报极低。
- **数字声明冲突(WARN)**:同量词出现多个不同数字(如 "8 编排"/"9 编排")。**不自动判错**——同量词常指代不同集合(含/不含 BasicAgent),是给人扫的 review 辅助视图,**不进 gate**。

### 退出码

ERROR>0 返 `1`(供 Stop hook gate);仅 WARN 或干净返 `0`;扫描目录出错返 `2`。

## 核心铁律:机械层只产候选,不单独判错

实测教训(印证 CLAUDE.md "过度防御误判"):**"符号不在代码里" ≠ "注释错了"**。注释常在**否定/历史/对照**语境里正确地提到一个不存在的符号:

- `// v1 已删 IsResumeTarget 自检` —— 注释对的,在讲历史
- `// eino 有 GetSessionValue/AddSessionValue, 我们只 SetValue/GetValue` —— 对照外部框架
- `// 原名 cpPendingMember 误导,已 rename` —— 自己讲改名史

所以第 1 层 ERROR 是**待人工/第 2 层确认的候选**,不是判决。**判真假必须读语境**。且 sanity:不一致时**先判哪边对**——可能是注释对而代码退化了(顺手抓出代码 bug,这是注释校验比文档校验额外的回报)。

## 降噪规则(为什么这么设计)

符号检查最易假阳,降噪分两档作用域(`main.go` 实测调出):

- **`externalFrameworks`**(eino/langgraph/langchain/claude code)→ **CommentGroup 级**豁免。外部框架名业务几乎不出现,跨行安全(抓"eino 范式 ``` `WithX` ```"这种信号在上一行的情况)。
- **`lineSignals`**(v1/已删/废弃/deprecated/曾/不再/退化/原名/rename/改名/重命名/otlp/proto)→ **当行**豁免。这些可能与业务词共块,只豁免出现信号的当行,不放大整块。
- **故意排除的词**(放大作用域会放过真 drift):
  - **"对照/vs"**——"vs 低阶 Router"是对照**本项目** API,符号**应存在**,误豁免会放过低阶 API 名漂移(drift 富矿)。
  - **"删除/移除"**(业务高频:redis Cleanup 块"删除 sid")、**"spec"**(agentdock-spec 生成类型遍地引用)——共块会吞掉同块真缺失符号。

其余机械降噪(均在 `checkSymbols`):缩进代码块行 / `//go:` 等指令 / 含空格(代码片段) / 含下划线(字符串值·JSON·env·metric·测试名) / 全大写(SQL/Redis 关键字 `LIKE`/`BGSAVE`) / 前导 `.`(方法引用) / 外部包 qualified(首段小写包名不在符号表) / 纯小写 base(ctx/err 通用词) / 占位符前缀(My/Foo/Bar/Baz/Example/Xxx) / 内置白名单(error/Context/Reader…)。**路径与数字不受任何语境豁免**——本仓文档移动 / 数字漂移仍是真 drift。

符号表(`collectSymbols`,`ast.Inspect` 递归)宽收:函数/类型/常量/变量/字段/接口方法/import 包名 + **函数参数 / 命名返回值 / 局部 `:=` 变量 / range 变量 / 标识符形态的字符串常量值**。后半是泛化教训——英文 godoc 大量反引号引用参数/局部变量("when ``` `wasInterrupted` ``` is true"),不收会假阳爆炸。

## 能力边界(诚实)

- **项目特异配置硬编码**:中文 `lineSignals` / 中文数字量词(编排/层/维) / `externalFrameworks` 的具体框架名都是 crew-go 专属。复用到别的 Go 仓需改这几处(理想是提成 flag)。英文项目数字冲突检查基本不工作。
- **召回有意放宽**:纯小写未导出符号引用不查(降误报代价);字符串常量值进符号表,理论上真 drift 符号若恰等于某字符串字面量会漏报(概率低)。
- **只覆盖机械可对账的**:godoc 描述的**行为是否属实**(第 2 层)、**语义是否夸大**(第 3 层)第 1 层都抓不到,必须 LLM 接力。

## 准确率怎么验证(换项目测是必须的)

**只在 crew-go 上调出"0 假阳"是过拟合**——crew-go 中文注释风格不引用局部变量,换 eino(英文)立刻崩到 0% 精确率(11 假阳全是参数/返回值/局部变量引用)。必须**换没调过参的项目盲测**。当前验证:

| 场景 | 假阳 | 召回 |
|---|---|---|
| crew-go 非 test(262 文件) | 0 | 报 5 真 drift(已修) |
| crew-go 含 test(489 文件) | 0 | 注入 sanity 抓到 |
| eino 盲测(291 文件,英文框架) | 0 | — |
| net/http 盲测(标准库) | 0 | — |
| `-files` 增量(hook 路径) | — | 注入抓到 |

**改了降噪规则后必做两件事**:① 换一个没调过的项目重跑看精确率;② 召回 sanity——临时注入一个不存在的符号引用,确认仍被抓(防降噪过度把真 drift 也吞了)。

## 工作流

1. **触发**:改大量符号/删改/重命名 API 后、commit 触及大量注释、或专门审注释时,跑第 1 层全仓(或 `-files` 增量)。
2. **第 1 层 ERROR 候选**:逐个**读语境**判真假(历史/对照语境=假阳;裸引用不存在符号=真 drift)。**不要见 ERROR 就改**。
3. **WARN 数字冲突**:人工扫一遍,确认每处数字指代对(尤其"编排/层/维"这类多集合量词)。
4. **第 2/3 层**:对核心 godoc 做契约 vs 实现 + 语义偏差 audit——**多 agent 单视角并发**(单 agent 多视角是假绿),每个 finding **grep 验证真假**(宽范围审 67% 误判)。
5. **修复**:注释错则改注释;**代码退化则改代码**(先判哪边对)。修一处 grep 全仓+跨文档防同病。
6. **不承诺"扫干净"**——说"已知未发现,可继续审 X 角度"。

## 触发场景(应主动调本 skill)

- 重命名/删除导出 API、大规模重构后(注释最易 stale)
- commit 改动含大量注释/godoc
- 用户说"校验注释 / 审注释 / 查注释 drift / godoc 准不准 / 注释和代码对不对"
- 写完长 godoc 想反向核一遍(此时先 doc-writing 写、再 comment-audit 校验)
