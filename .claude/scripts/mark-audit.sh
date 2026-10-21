#!/bin/bash
# helper: 标记 commit 已做 audit / cross-verify
# 用法:
#   bash .claude/scripts/mark-audit.sh audit                  # 标最近 commit audit 已做
#   bash .claude/scripts/mark-audit.sh audit <sha>            # 标指定 sha audit 已做
#   bash .claude/scripts/mark-audit.sh crossverify [<sha>]    # 标 cross-verify 已做
#   bash .claude/scripts/mark-audit.sh commentaudit [<sha>]  # 标注释 drift 已处理 (comment-audit)
#   bash .claude/scripts/mark-audit.sh all [<sha>]            # 一次标全部 (audit + crossverify + commentaudit)
#   bash .claude/scripts/mark-audit.sh status [<sha>]         # 查 commit 状态
#
# **why <sha> 参数**：mark-audit.sh 历史只 mark 最新 sha，导致 audit 实际跑在原
# 高风险 commit 但 marker 落在 follow-up commit 上 — 原 commit marker 缺失。
# 加 <sha> 参数支持回填。

set -e
cd "$(git rev-parse --show-toplevel)"
mkdir -p .claude/audit

# 第二参数 = 可选 sha，默认最新 commit
target_sha="${2:-$(git log -1 --pretty=%H)}"
# 解析 short sha → full sha (业务可能传短 sha 如 233644d)
if ! full_sha=$(git rev-parse --verify "$target_sha" 2>/dev/null); then
    echo "ERR: 无效 sha: $target_sha" >&2
    exit 1
fi
short=${full_sha:0:7}

case "${1:-}" in
    audit)
        touch ".claude/audit/${full_sha}.audit.done"
        echo "✓ commit ${short} 标记 audit 已做"
        ;;
    crossverify)
        touch ".claude/audit/${full_sha}.crossverify.done"
        echo "✓ commit ${short} 标记 cross-verify 已做"
        ;;
    commentaudit)
        touch ".claude/audit/${full_sha}.commentaudit.done"
        echo "✓ commit ${short} 标记 comment-audit 注释 drift 已处理"
        ;;
    all)
        touch ".claude/audit/${full_sha}.audit.done"
        touch ".claude/audit/${full_sha}.crossverify.done"
        touch ".claude/audit/${full_sha}.commentaudit.done"
        echo "✓ commit ${short} 标记 audit + cross-verify + comment-audit 全做"
        ;;
    status)
        echo "commit: ${short}"
        if [ -f ".claude/audit/${full_sha}.audit.done" ]; then
            echo "  audit:        ✓ done"
        else
            echo "  audit:        ✗ missing"
        fi
        if [ -f ".claude/audit/${full_sha}.crossverify.done" ]; then
            echo "  crossverify:  ✓ done"
        else
            echo "  crossverify:  ✗ missing"
        fi
        if [ -f ".claude/audit/${full_sha}.commentaudit.done" ]; then
            echo "  commentaudit: ✓ done"
        else
            echo "  commentaudit: ✗ missing/不适用"
        fi
        ;;
    *)
        echo "用法: $0 {audit|crossverify|commentaudit|all|status} [<sha>]"
        exit 1
        ;;
esac
