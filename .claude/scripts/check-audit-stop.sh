#!/bin/bash
# Stop hook: 检查最近 commit 是否需要 audit + cross-verify，缺则 block Claude 收尾。
#
# 强制规则（与 user memory feedback_audit_trigger_rule 对应）：
#   高风险 commit (改 sess / cp / channel / sync / agent_v2/_agent.go) 必须:
#     1. 做 audit (touch .claude/audit/<sha>.audit.done)
#     2. 做 cross-verify (touch .claude/audit/<sha>.crossverify.done)
#   未做 → 输出 JSON decision=block 让 Claude 不能 stop
#
# 跳过条件:
#   - 工作树有未 commit 改动 → 不算"已 commit"，跳过检查
#   - 最近 commit 不涉及高风险文件 → 不需要 audit
#   - 最近 commit 只改文档/非代码 (无 .go 文件) → 不需要代码 audit
#     (高风险判定只对 .go 代码文件 — 否则 channel-engine.md / interrupt-resume.md
#      这类文档名含 channel/interrupt 会误报 block)
#
# 使用: 配在 .claude/settings.json hooks.Stop[].command

set -e
cd "$(git rev-parse --show-toplevel 2>/dev/null)" || exit 0

# 工作树脏 → 还没 commit 完，跳过
if ! git diff-index --quiet HEAD -- 2>/dev/null; then
    exit 0
fi

# 最近 commit hash
last_sha=$(git log -1 --pretty=%H 2>/dev/null) || exit 0

# 根提交（导入快照）= 整个代码库一次性进来，没有「这次改了什么」可审；按文件名判高风险只会误报 block
git rev-parse --verify -q "${last_sha}^" >/dev/null 2>&1 || exit 0

# 检查是否高风险 commit (改了 session / cp / sync / 编排等 .go 代码文件)
# 先 grep '\.go$' 只留代码文件 — 文档 (.md) 即使文件名含 channel/interrupt 也不触发
risky_files=$(git log -1 --name-only --pretty=format: "$last_sha" | grep -E '\.go$' | grep -E "session\.go|checkpointer|graph_loop|_agent\.go|sync|channel|interrupt" || true)

if [ -z "$risky_files" ]; then
    # 不是高风险 commit, 通过
    exit 0
fi

audit_marker=".claude/audit/${last_sha}.audit.done"
crossverify_marker=".claude/audit/${last_sha}.crossverify.done"

# 检查 audit marker
if [ ! -f "$audit_marker" ]; then
    cat <<EOF
{"decision":"block","reason":"高风险 commit ${last_sha:0:7} 涉及 $(echo "$risky_files" | head -3 | tr '\n' ',' | sed 's/,$//') 等需要 audit。请：\n1. spawn 单视角审计 agent 并发审（个数按这次改动真正涉及的视角定；派单附已证清单、只列新风险、附代码地图、约 15 分钟先交，见 CLAUDE.md「派审计别让它重做已做过的事」）\n2. 逐条核实 finding（audit 命中率 33-67%）\n3. 通过后跑: bash .claude/scripts/mark-audit.sh audit\n后 Stop。"}
EOF
    exit 0
fi

# 检查 cross-verify marker (CLAUDE.md 规则: audit 报告必交叉验证)
if [ ! -f "$crossverify_marker" ]; then
    cat <<EOF
{"decision":"block","reason":"高风险 commit ${last_sha:0:7} 已做 audit 但未交叉验证 (CLAUDE.md '审计纪律' 步骤 6)。请：\n1. 逐条核实 audit 报告的 finding：行为类写探针复现（跑红才算真），文档 / 注释类按 file:line 对照代码核\n2. 通过后跑: bash .claude/scripts/mark-audit.sh crossverify\n后 Stop。"}
EOF
    exit 0
fi

# 全通过, allow stop
exit 0
