#!/bin/bash
# Stop hook: commit message 三方字样 gate（对应 user memory feedback_no_three_way_in_commits）。
#
# 规则：未 push 的本地 commit message（subject + body）不得含**框架对照**字样——
#   commit / 自评 / 决策只讲我们自己的设计 + 第一性理由，对照结论沉淀进
#   doc/description/three-way-comparison/（那里是对照文档、可提）。命中 → block，逼改干净再收尾。
#
# ⚠️ 触发词**不是裸的「三方」**（2026-09-20 收窄，见下面判据处的理由）。拿全仓 1743 个
#   commit 对照验过：放行的 19 条逐条读过全是真阴（14 条是路径引用 three-way-comparison/，
#   其余是「三个当事人 / 三个位置 / 三个来源」），同时补回 langchain 把 3 条真阳捞了回来。
#
# 只查 commit message，**不查文件内容**——three-way-comparison/*.md 的内容本就该含
#   eino/langgraph，只有 commit message 不行，故按 message 检查不误伤文档。
#
# 跳过：工作树脏（还没 commit 完）。
# 使用：配在 .claude/settings.json hooks.Stop[].command。

set -e
cd "$(git rev-parse --show-toplevel 2>/dev/null)" || exit 0

# 工作树脏 → 还没 commit 完，跳过
if ! git diff-index --quiet HEAD -- 2>/dev/null; then
    exit 0
fi

# 未 push 的本地 commit 范围。
#
# ⚠️ **别用「无 upstream 就退到 HEAD~1..HEAD」**（这里曾这么写）：那个回退两个方向都错，
# 而且**恰好在本仓最常见的形态上错**——worktree 里的 feature 分支通常没有 upstream，
# 内容是经 dev 推出去的。
#   - 假阳：已经推出去的 commit 照样被查。实测一次 block 的理由是「未 push 的 commit」，
#     而那个 commit 就是 origin/dev 的 tip；照它建议的 `filter-branch` 做就是重写已发布历史。
#   - 假阴：只查 1 个。没有 upstream 的分支上攒了 5 个未推送 commit 时，漏掉 4 个。
#
# 正解是直接问「这个 commit 在不在任何远端分支上」——`--not --remotes` 就是这个语义，
# 不依赖 upstream 配得对不对。
range_args="HEAD --not --remotes"

# 已知例外（按提交号放行，2026-10-14 用户拍板）：这些提交说明里有一句框架名，但它们已经快进进本地 dev、
# 后面又有几十个提交引用它们的提交号，改写历史的代价远大于这一句。新提交不要往这里加——写干净。
#   - 354b3b18：一句「MAF .NET 路径长度 0」
known_exceptions="354b3b18"

# 逐个 commit 查 message（subject + body）
bad=""
for sha in $(git log $range_args --format='%h' 2>/dev/null); do
    case " $known_exceptions " in *" ${sha:0:8} "*) continue ;; esac
    # 统一小写（tr 只动 ASCII 字节，中文不受影响），再剔几类**跟框架对照无关**的同形串：
    #   - `第三方`：指外部主体/攻击者
    #   - `three-way-comparison`：那是**路径**。改该目录的 commit 自然会写到它
    #     （实测 269 个相关 commit 里 81 个 message 带这个串），拦它等于拦正常的 docs commit
    #   - `三方对照文档` / `三方对照：`：同一个目录的中文叫法（「三方对照文档里那条决策记录……」「三方对照：
    #     handoff-control-signal 加一行」），跟路径同理放行（2026-10-14 收窄：6 个提交被拦，5 个是这种）。
    #     真在提交说明里做对照时，框架名会被第 1 档兜住
    #   - `three-way merge`：git 自己的术语
    msg=$(git log -1 "$sha" --format='%s%n%b' 2>/dev/null | tr 'A-Z' 'a-z' \
        | sed -e 's/第三方//g' -e 's/three-way-comparison//g' -e 's/three-way merge//g' \
              -e 's/三方对照文档//g' -e 's/三方对照：//g' -e 's/三方对照://g')

    # 判据分两档——**「三方」不再裸匹配**（2026-09-20 收窄，两次 block 都栽在它上面）：
    #   1. 框架名裸词即命中：它们在本仓没有第二个意思
    #   2. 「三方」只在**对照词组**里命中：这个词最常见的中文意思是「三个当事人」，
    #      实测历史上裸「三方」约一半是这个（三方收敛 / 三方独立踩中 / 三方一致用点号），
    #      拦它拦的是正常表述；而真正要拦的那些（三方对照 / 照三方规则 / 三方框架引用）
    #      都带对照词，且多半同时出现框架名、被第 1 档兜住
    #   框架名里补了 maf（2026-10-14：一个提交写了「MAF .NET」而词表里没有它）；按整词匹配，免得撞上别的单词
    if printf '%s' "$msg" | grep -qE "eino|langgraph|langchain|crewai|autogen|(^|[^a-z])maf([^a-z]|$)|three-way|三方(对照|对比|调研|框架|表|规则|实现|源码|方案|做法|精华|都不用|都用)|(照|参考|对照|对比|抄)三方"; then
        bad="$bad $sha"
    fi
done

if [ -n "$bad" ]; then
    cat <<EOF
{"decision":"block","reason":"commit message 含框架对照字样（违反 memory feedback_no_three_way_in_commits）：commit${bad}。只讲我们自己的设计 + 第一性理由，删掉框架名与对照句；调研结论放 doc/description/three-way-comparison/（那里可提，本 gate 不查文件内容）。这些 commit 已确认**不在任何远端分支上**，最近一个用 git commit --amend、更早的用 git rebase -i 改 message 即可，**不需要 force push、更不要 filter-branch**（本 gate 曾因判据 bug 把已发布的 commit 报成未推送，照那个建议做会重写已发布历史）。改完即放行。"}
EOF
    exit 0
fi

exit 0
