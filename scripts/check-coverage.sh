#!/usr/bin/env bash
# 核心包覆盖率门禁 (OKR 9.4)。
#
# 10 个核心包覆盖率须 >= 阈值, 任一低于则 CI fail (门禁防回退 —— 当前都已达标)。
# 短板包 (tool / mcp / a2a / model — 靠真 LLM 或外部依赖测, 默认单测覆盖不到)
# 仅展示覆盖率, 不纳入硬门禁, 让短板可见不被忽略。
#
# GitLab CI 经 job 的 coverage 正则抓末行 "total: (statements) X%" 出覆盖率徽章。
#
# 本地: make cover    CI: coverage job script。
set -uo pipefail

THRESHOLD=${COVERAGE_THRESHOLD:-75}

# 纳入门禁的核心包 (当前都 >= 75%)。
CORE=(
  compose_v2 agent_v2 hook skill store prompt
  fallback observability a2a/registry a2a/server
)
# 短板包: 展示不门禁。
WEAK=(tool mcp a2a model model/openai)

corepkgs=$(printf 'crew-go/%s ' "${CORE[@]}")

echo "==== 核心包覆盖率门禁 (>= ${THRESHOLD}%) ===="
# 一次跑: stdout 给 per-package coverage, 合并 profile 给 total。
OUT=$(go test -coverprofile=coverage.out $corepkgs 2>&1)
if [ $? -ne 0 ]; then
  echo "$OUT" | grep -E "FAIL|panic" || true
  echo "❌ 核心包测试失败, 门禁不通过"
  exit 1
fi

fail=0
for pkg in "${CORE[@]}"; do
  cov=$(echo "$OUT" | awk -v p="crew-go/$pkg" '$2==p{for(i=1;i<=NF;i++)if($i=="coverage:"){gsub(/%/,"",$(i+1));print $(i+1)}}')
  if [ -z "$cov" ]; then
    echo "  ❌ $pkg: 无覆盖率数据"
    fail=1
    continue
  fi
  ok=$(awk -v c="$cov" -v t="$THRESHOLD" 'BEGIN{print (c+0>=t)?1:0}')
  if [ "$ok" = 1 ]; then
    echo "  ✅ $pkg: ${cov}%"
  else
    echo "  ❌ $pkg: ${cov}% < ${THRESHOLD}%"
    fail=1
  fi
done

echo ""
echo "==== 短板包 (不纳入门禁, 仅展示 — 靠真 LLM / 外部依赖测) ===="
weakout=$(go test -cover $(printf 'crew-go/%s ' "${WEAK[@]}") 2>&1)
for pkg in "${WEAK[@]}"; do
  cov=$(echo "$weakout" | awk -v p="crew-go/$pkg" '$2==p{for(i=1;i<=NF;i++)if($i=="coverage:")print $(i+1)}')
  echo "  ⚠️  $pkg: ${cov:-N/A}"
done

# GitLab coverage 抓末行 (核心包合并 total)。
echo ""
go tool cover -func=coverage.out | tail -1

if [ $fail -ne 0 ]; then
  echo ""
  echo "❌ 覆盖率门禁未通过 (上方标 ❌ 的包低于 ${THRESHOLD}%)"
  exit 1
fi
echo ""
echo "✅ 覆盖率门禁通过 (${#CORE[@]} 个核心包均 >= ${THRESHOLD}%)"
