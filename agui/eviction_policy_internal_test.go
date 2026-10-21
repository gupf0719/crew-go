package agui

import (
	"context"
	"testing"

	"crew-go/agent_v2"
	"crew-go/hook"
)

// 淘汰策略的白盒枚举——黑盒那侧（lru_owner_test.go）走真 HTTP 证明攻击链，这侧管
// **纯输入→输出**：谁该付账、全 busy 怎么办、以及**同一张表反复选必须选出同一个**
// （map 迭代无序，定序漏一档就是「同一场景每次淘汰的会话都不同」）。

type seedEntry struct {
	owner, threadID string
	busy            bool
	at              int64  // touchedAt
	sid             string // 空 = 空壳（还没挂起过，没有 cp 要清）
}

func newPolicyHandler(max int) *handler {
	return Handler(
		func(hook.Hooks) agent_v2.Agent { return nil },
		WithOwnerResolver(func(context.Context) string { return "unused" }),
		WithMaxSessions(max),
	).(*handler)
}

func (h *handler) seed(entries []seedEntry) {
	h.sessions = make(map[sessionKey]*sessionEntry, len(entries))
	for _, s := range entries {
		e := &sessionEntry{busy: s.busy, touchedAt: s.at}
		if s.sid != "" {
			e.sess = &agent_v2.RunSession{RunID: s.sid}
		}
		h.sessions[sessionKey{owner: s.owner, threadID: s.threadID}] = e
	}
}

// 表里少了哪个键（"" = 一个都没淘汰）。
func missingKey(before []seedEntry, after map[sessionKey]*sessionEntry) sessionKey {
	for _, s := range before {
		k := sessionKey{owner: s.owner, threadID: s.threadID}
		if _, ok := after[k]; !ok {
			return k
		}
	}
	return sessionKey{}
}

func TestEvictPolicy(t *testing.T) {
	cases := []struct {
		name       string
		max        int
		entries    []seedEntry
		inserter   string
		wantVictim sessionKey // 零值 = 不该淘汰任何人
		wantSIDs   []string
	}{
		{
			// 单租户部署（resolver 返固定值）：只有一个分区 ⟹ 它恒是「份额最大」⟹
			// 行为跟全局 LRU 一模一样，最旧的空闲会话出账。可用会话数不打折。
			name: "单租户超限_自己最旧的出账",
			max:  2, inserter: "solo",
			entries: []seedEntry{
				{owner: "solo", threadID: "z-oldest", at: 1, sid: "s1"},
				{owner: "solo", threadID: "a-newer", at: 2, sid: "s2"},
				{owner: "solo", threadID: "b-busy", at: 3, busy: true},
			},
			wantVictim: sessionKey{"solo", "z-oldest"}, wantSIDs: []string{"s1"},
		},
		{
			// 攻击者「跑完即走」的形状：它的 entry 转眼被 removeEntry，份额恒 1，跟
			// 受害者持平。**平局归到达者**，而它手上只有刚插入的那条 busy entry ⟹
			// 谁也不淘汰。老代码在这儿回落全局最旧 ⟹ 淘汰 A。
			name: "份额持平_到达者不赢平局",
			max:  2, inserter: "E",
			entries: []seedEntry{
				{owner: "A", threadID: "a1", at: 1, sid: "sa"},
				{owner: "V", threadID: "v1", at: 2, sid: "sv"},
				{owner: "E", threadID: "e1", at: 3, busy: true},
			},
			wantVictim: sessionKey{}, wantSIDs: nil,
		},
		{
			// 同一个租户自己开到超限：它的份额（含刚插入那条）严格更大 ⟹ 自己付账。
			name: "到达者份额更大_淘汰自己最旧的",
			max:  2, inserter: "B",
			entries: []seedEntry{
				{owner: "A", threadID: "a1", at: 1, sid: "sa"},
				{owner: "B", threadID: "b1", at: 2, sid: "sb1"},
				{owner: "B", threadID: "b2", at: 3, busy: true},
			},
			wantVictim: sessionKey{"B", "b1"}, wantSIDs: []string{"sb1"},
		},
		{
			// 别人囤了一堆（份额**严格**大于到达者）⟹ 由囤的那个出账，新来的小租户
			// 不必为别人的囤积买单。没有这一档的话，占坑的那个永远占着。
			name: "别人份额严格更大_由它付账",
			max:  3, inserter: "newcomer",
			entries: []seedEntry{
				{owner: "hog", threadID: "z-oldest", at: 1, sid: "s1"},
				{owner: "hog", threadID: "a-mid", at: 2, sid: "s2"},
				{owner: "hog", threadID: "b-newest", at: 3, sid: "s3"},
				{owner: "newcomer", threadID: "n1", at: 4, busy: true},
			},
			wantVictim: sessionKey{"hog", "z-oldest"}, wantSIDs: []string{"s1"},
		},
		{
			// 在途的动不得（正被一个请求驱动着）。全在途 ⟹ 谁也淘汰不了，表暂时超出
			// 上限——这是 WithMaxSessions 那句「上限是软的」的一半。
			name: "全 busy_谁也不淘汰",
			max:  2, inserter: "A",
			entries: []seedEntry{
				{owner: "A", threadID: "a1", at: 1, busy: true},
				{owner: "A", threadID: "a2", at: 2, busy: true},
				{owner: "A", threadID: "a3", at: 3, busy: true},
			},
			wantVictim: sessionKey{}, wantSIDs: nil,
		},
		{
			// 空壳（RunStarted 写失败 / Goexit 那类，entry 在但没挂起过）：照样占位子、
			// 照样该被淘汰，只是没有 cp 要清 ⟹ 返回的 sid 列表是空的。
			name: "空壳被淘汰_但没有 cp 要清",
			max:  1, inserter: "A",
			entries: []seedEntry{
				{owner: "A", threadID: "a1", at: 1}, // sid 空 = 空壳
				{owner: "A", threadID: "a2", at: 2, busy: true},
			},
			wantVictim: sessionKey{"A", "a1"}, wantSIDs: nil,
		},
		{
			name: "没超上限_不动",
			max:  3, inserter: "A",
			entries: []seedEntry{
				{owner: "A", threadID: "a1", at: 1, sid: "s1"},
				{owner: "B", threadID: "b1", at: 2, sid: "s2"},
			},
			wantVictim: sessionKey{}, wantSIDs: nil,
		},
		{
			name: "max<=0 不限_不动",
			max:  0, inserter: "A",
			entries: []seedEntry{
				{owner: "A", threadID: "a1", at: 1, sid: "s1"},
				{owner: "A", threadID: "a2", at: 2, sid: "s2"},
				{owner: "A", threadID: "a3", at: 3, sid: "s3"},
			},
			wantVictim: sessionKey{}, wantSIDs: nil,
		},
		{
			// 两个分区在最大份额上打平、且都严格大于到达者：**两个都有资格出账**，
			// 得有个说法定下来是谁——比最旧。没有这一档的话 map 迭代顺序说了算。
			name: "两个分区并列最大_比最旧",
			max:  4, inserter: "small",
			entries: []seedEntry{
				{owner: "A", threadID: "a1", at: 5, sid: "sa1"},
				{owner: "A", threadID: "a2", at: 6, sid: "sa2"},
				{owner: "B", threadID: "b1", at: 2, sid: "sb1"}, // 全场最旧
				{owner: "B", threadID: "b2", at: 7, sid: "sb2"},
				{owner: "small", threadID: "s1", at: 8, busy: true},
			},
			wantVictim: sessionKey{"B", "b1"}, wantSIDs: []string{"sb1"},
		},
		{
			// touchedAt 撞了（粗时钟平台上真会发生）：再往下比键，别把决定权交给
			// map 的迭代顺序。
			name: "同份额同时间_比键",
			max:  2, inserter: "A",
			entries: []seedEntry{
				{owner: "A", threadID: "t2", at: 100, sid: "s2"},
				{owner: "A", threadID: "t1", at: 100, sid: "s1"},
				{owner: "A", threadID: "t3", at: 100, busy: true},
			},
			wantVictim: sessionKey{"A", "t1"}, wantSIDs: []string{"s1"},
		},
		{
			// 匿名（resolver 返空串）就是一个普通分区，不享受任何豁免：它囤得最多就它出账。
			name: "匿名分区照样按份额出账",
			max:  2, inserter: "A",
			entries: []seedEntry{
				{owner: "", threadID: "z-oldest", at: 1, sid: "s1"},
				{owner: "", threadID: "a-newer", at: 2, sid: "s2"},
				{owner: "A", threadID: "a1", at: 3, busy: true},
			},
			wantVictim: sessionKey{"", "z-oldest"}, wantSIDs: []string{"s1"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// **同一张表反复选 200 次**：map 迭代顺序每次都不同，定序漏一档就会在这儿现形。
			for i := 0; i < 200; i++ {
				h := newPolicyHandler(tc.max)
				h.seed(tc.entries)
				gotSIDs := h.evictIfNeededLocked(tc.inserter)
				gotVictim := missingKey(tc.entries, h.sessions)

				if gotVictim != tc.wantVictim {
					t.Fatalf("第 %d 次：淘汰了 %+v，期望 %+v", i, gotVictim, tc.wantVictim)
				}
				if len(gotSIDs) != len(tc.wantSIDs) {
					t.Fatalf("第 %d 次：待清 cp 的 sid %v，期望 %v", i, gotSIDs, tc.wantSIDs)
				}
				for j := range gotSIDs {
					if gotSIDs[j] != tc.wantSIDs[j] {
						t.Fatalf("第 %d 次：待清 cp 的 sid %v，期望 %v", i, gotSIDs, tc.wantSIDs)
					}
				}
				// 淘汰至多一条：表大小要么不变要么少 1。
				wantLen := len(tc.entries)
				if tc.wantVictim != (sessionKey{}) {
					wantLen--
				}
				if len(h.sessions) != wantLen {
					t.Fatalf("第 %d 次：表里剩 %d 条，期望 %d", i, len(h.sessions), wantLen)
				}
			}
		})
	}
}

// TestEvictPolicy_ShareCountsBusyEntries 单钉一条**看不见但承重**的判据：份额要把在途
// （busy）entry 算进去。
//
// 到达者刚插入的那条恒是 busy——不算的话它的份额是 0、恒小于所有人，「到达者不赢平局」
// 那条规矩就永远轮不到触发，退回原来的跨租户 DoS。这一条单独立用例，是因为它在
// evictIfNeededLocked 里只体现为「没有 `if e.busy { continue }`」——**一个不存在的语句
// 没法靠读代码盯住**，只能靠一个会红的用例。
func TestEvictPolicy_ShareCountsBusyEntries(t *testing.T) {
	h := newPolicyHandler(2)
	entries := []seedEntry{
		{owner: "A", threadID: "a1", at: 1, sid: "sa"},
		{owner: "V", threadID: "v1", at: 2, sid: "sv"},
		{owner: "E", threadID: "e1", at: 3, busy: true}, // 到达者手上只有这条
	}
	h.seed(entries)
	if got := h.evictIfNeededLocked("E"); got != nil {
		t.Fatalf("到达者只有一条在途 entry 时不该淘汰任何人，却清了 %v", got)
	}
	// 正向：A、V 都还在（不是「表被清空了所以没返 sid」）。
	for _, k := range []sessionKey{{"A", "a1"}, {"V", "v1"}} {
		if _, ok := h.sessions[k]; !ok {
			t.Errorf("在位者 %+v 被淘汰了", k)
		}
	}
}
