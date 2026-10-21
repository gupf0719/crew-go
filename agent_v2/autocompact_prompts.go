package agent_v2

// AutoCompact 提示词模板（ISSUE-67）
//
// 设计参考 Claude Code 摘要提取流程，做了以下本地化适配：
//
//  1. NO_TOOLS_PREAMBLE 双放（前后各放一次）——Sonnet 4.6+ 实测会忽略弱尾部
//     指令尝试调用工具，maxTurns:1 时无文本输出会落到流式备用路径。我们没有
//     maxTurns 概念但同样的"前后强提醒"提升摘要 model 不调工具的概率。
//  2. <analysis> chain-of-thought + <summary> 双块输出——LLM 先思考再产出
//     摘要，质量比裸提示更高；后处理删 <analysis> 只保留 <summary>。
//  3. 9 段结构化输出（标题保英文便于程序化解析，正文按 model 自然语言）
//  4. 默认中文（plan_chat_web 用户群体）；通过 AutoCompactLanguage 切换 EN

// noToolsPreambleCN / NoToolsTrailerCN 中文版双放禁工具指令。
const noToolsPreambleCN = `严格要求：仅返回纯文本，不调用任何工具。

- 不要使用 Read、Bash、Grep、Glob、Edit、Write 或任何其他工具。
- 你已经从上面的对话中拥有所需的全部上下文。
- 工具调用会被拒绝并消耗你唯一的回合——任务将失败。
- 你的整个回复必须是纯文本：一个 <analysis> 块，紧跟一个 <summary> 块。`

const NoToolsTrailerCN = `提醒：不要调用任何工具。仅以纯文本回复——一个 <analysis> 块跟一个 <summary> 块。工具调用会被拒绝，任务会失败。`

// noToolsPreambleEN / NoToolsTrailerEN 英文版（与 Claude Code 原文保持等价）。
const noToolsPreambleEN = `CRITICAL: Respond with TEXT ONLY. Do NOT call any tools.

- Do NOT use Read, Bash, Grep, Glob, Edit, Write, or ANY other tool.
- You already have all the context you need in the conversation above.
- Tool calls will be REJECTED and will waste your only turn — you will fail the task.
- Your entire response must be plain text: an <analysis> block followed by a <summary> block.`

const NoToolsTrailerEN = `REMINDER: Do NOT call any tools. Respond with plain text only — an <analysis> block followed by a <summary> block. Tool calls will be rejected and you will fail the task.`

// MainExtractionPromptCN 中文 9 段摘要主体提示词。
const MainExtractionPromptCN = `请阅读上面的整段对话，按下面 9 个固定段落产出结构化摘要。

先在 <analysis> 块里逐段思考你将放入 <summary> 的内容（可以分析对话脉络、决策依据、用户重点关注点等），然后在 <summary> 块里按 9 段输出最终摘要。

格式要求：
<analysis>
（你的思考过程，可以较长。最终会被丢弃，只用于提升摘要质量。）
</analysis>

<summary>
1. Primary Request and Intent
（用户的主要请求和意图——他们想完成什么）

2. Key Technical Concepts
（涉及的关键技术概念、框架、设计模式）

3. Files and Code Sections
（提到 / 修改 / 阅读过的文件路径与对应代码段）

4. Errors and fixes
（遇到的错误及对应的修复方式）

5. Problem Solving
（已解决的问题——结论而非过程）

6. All user messages
（用户的所有原始消息按顺序简略列出，每条不超过一行）

7. Pending Tasks
（明确剩余 / 待办的任务）

8. Current Work
（在被压缩之前正在做什么）

9. Optional Next Step
（如有自然延续的下一步可写；不确定就写"无明确下一步"）
</summary>`

// MainExtractionPromptEN 英文版主体提示词。
const MainExtractionPromptEN = `Read the entire conversation above and produce a structured summary in the 9 fixed sections below.

First reason inside an <analysis> block about what you will place in <summary> (you may analyze the conversation flow, decision rationale, what the user cared about, etc.), then output the final summary in the <summary> block in 9 sections.

Format requirements:
<analysis>
(Your reasoning, may be long. Will be discarded, only used to improve summary quality.)
</analysis>

<summary>
1. Primary Request and Intent

2. Key Technical Concepts

3. Files and Code Sections

4. Errors and fixes

5. Problem Solving

6. All user messages
(List ALL user messages in order, each one line max.)

7. Pending Tasks

8. Current Work

9. Optional Next Step
</summary>`

// AutoCompactLanguage 选择摘要提示词语言。
type AutoCompactLanguage int

const (
	AutoCompactLangCN AutoCompactLanguage = iota
	AutoCompactLangEN
)
