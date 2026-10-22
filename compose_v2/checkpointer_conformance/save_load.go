package checkpointer_conformance

import (
	"context"
	"encoding/json"
	"slices"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// confChannelVal 模拟业务跨 cp 边界存的自定义 channel value——SaveNS_Channels_Roundtrip
// subtest 用，验业务 backend 序列化 taggedValue.tag 不丢。
type confChannelVal struct{ V string }

// testSaveLoad 验证 SaveNS / LoadNS round-trip + 自动填 ID/NS.
func testSaveLoad(t *testing.T, factory CheckpointerFactory) {
	t.Run("SaveNS_Then_LoadNS_Roundtrip", func(t *testing.T) {
		cp := factory()
		ctx := context.Background()

		original := newCheckpoint("graph_a", compose_v2.CPStatusPending)
		if err := cp.SaveNS(ctx, "sid_1", "ns_a", original); err != nil {
			t.Fatalf("SaveNS: %v", err)
		}

		loaded, err := cp.LoadNS(ctx, "sid_1", "ns_a")
		if err != nil {
			t.Fatalf("LoadNS: %v", err)
		}
		if loaded == nil {
			t.Fatal("LoadNS expected non-nil, got nil")
		}
		if loaded.GraphID != "graph_a" {
			t.Errorf("expected GraphID=graph_a, got %q", loaded.GraphID)
		}
		if loaded.Status != compose_v2.CPStatusPending {
			t.Errorf("expected Status=Pending, got %v", loaded.Status)
		}
	})

	t.Run("SaveNS_Auto_Fills_ID_And_NS", func(t *testing.T) {
		cp := factory()
		ctx := context.Background()

		// 不预填 cp.ID / cp.NS, SaveNS 应自动覆盖填入参 sid/ns
		c := newCheckpoint("g", compose_v2.CPStatusUnset)
		_ = cp.SaveNS(ctx, "auto_sid", "auto_ns", c)

		loaded, _ := cp.LoadNS(ctx, "auto_sid", "auto_ns")
		if loaded.ID != "auto_sid" {
			t.Errorf("expected loaded.ID='auto_sid', got %q", loaded.ID)
		}
		if loaded.NS != "auto_ns" {
			t.Errorf("expected loaded.NS='auto_ns', got %q", loaded.NS)
		}
	})

	t.Run("SaveNS_Zero_CreatedAt_Not_Filled_By_Backend", func(t *testing.T) {
		cp := factory()
		ctx := context.Background()

		// CreatedAt=0 是**框架的信号**，不是"忘了填"：`guardedSaveNS` 拿它当"这个 cp
		// 还没盖过章"的判据，只在为 0 时盖。backend 若自作主张填（典型踩法：
		// `created_at TIMESTAMPTZ DEFAULT NOW()` 然后回读列值），框架的时间线就被
		// 静默改写——而 `Cleanup`（保留最近 N 个会话）**按它排序**，业务只会看到
		// "GC 删错了会话"，一路查不到这里。
		//
		// 现有用例全部显式填非 0 CreatedAt（还故意让 sid 字典序反着来防假绿），
		// **"caller 给 0"这条路径此前从没被走过**。
		c := newCheckpoint("g_zero_created", compose_v2.CPStatusPending)
		c.CreatedAt = 0
		if err := cp.SaveNS(ctx, "zero_created_sid", "", c); err != nil {
			t.Fatalf("SaveNS: %v", err)
		}

		loaded, err := cp.LoadNS(ctx, "zero_created_sid", "")
		if err != nil {
			t.Fatalf("LoadNS: %v", err)
		}
		if loaded == nil {
			t.Fatal("LoadNS 返 nil")
		}
		if loaded.CreatedAt != 0 {
			t.Errorf("caller 给 CreatedAt=0 时 backend 不该自己填，got %d\n"+
				"  框架靠这个 0 判断'还没盖章'；backend 抢着填会让 Cleanup "+
				"按错误的时间线排序，业务查不到根因",
				loaded.CreatedAt)
		}
	})

	t.Run("LoadNS_NonExistent_Returns_Nil_Nil", func(t *testing.T) {
		cp := factory()
		ctx := context.Background()

		loaded, err := cp.LoadNS(ctx, "no_such_sid", "no_such_ns")
		if err != nil {
			t.Errorf("LoadNS non-existent should return (nil, nil), got err: %v", err)
		}
		if loaded != nil {
			t.Errorf("LoadNS non-existent should return nil cp, got %+v", loaded)
		}
	})

	t.Run("SaveNS_Overwrite_Same_Key", func(t *testing.T) {
		cp := factory()
		ctx := context.Background()

		_ = cp.SaveNS(ctx, "sid", "ns", newCheckpoint("g_old", compose_v2.CPStatusUnset))
		_ = cp.SaveNS(ctx, "sid", "ns", newCheckpoint("g_old", compose_v2.CPStatusPending)) // GraphID 同, 状态变

		loaded, _ := cp.LoadNS(ctx, "sid", "ns")
		if loaded.Status != compose_v2.CPStatusPending {
			t.Errorf("expected overwritten Status=Pending, got %v", loaded.Status)
		}
	})

	t.Run("SaveNS_Nil_Checkpoint_Returns_Error", func(t *testing.T) {
		cp := factory()
		ctx := context.Background()

		if err := cp.SaveNS(ctx, "sid", "ns", nil); err == nil {
			t.Error("SaveNS with nil checkpoint should return error")
		}
	})

	t.Run("SaveNS_Empty_SessionID_Returns_Error", func(t *testing.T) {
		cp := factory()
		ctx := context.Background()

		c := newCheckpoint("g", compose_v2.CPStatusUnset)
		if err := cp.SaveNS(ctx, "", "ns", c); err == nil {
			t.Error("SaveNS with empty sessionID should return error")
		}
	})

	// audit 视角 1 H6 + 视角 3 H2 + 视角 4 HIGH-1/2/3 (3 视角 cross-verify):
	// 原 newCheckpoint 只填 GraphID+Status, 业务 backend 漏序列化 NodeVisits /
	// PendingInterrupts / CurrentFrontier / ParentNS / SchemaVer 任一字段都 PASS
	// 但生产 HITL Resume 时 InterruptID 串台 / 5 维稳定 ID 失效.
	//
	// audit iter-3 视角 1/2/3: helper default 改填全字段是 patch-on-patch 自踩
	// (让 SaveNS auto-fill 路径丢覆盖 + PendingInterrupts 仍是孤岛). 真根因修法:
	// 扩本 subtest 含 NodeVisits zero-value / AskQuery 全字段 + 加新 subtest 验
	// ParentNS auto-fill.
	t.Run("SaveNS_Preserves_All_Plain_Fields", func(t *testing.T) {
		cp := factory()
		ctx := context.Background()

		original := &compose_v2.Checkpoint{
			GraphID:   "graph_full",
			Status:    compose_v2.CPStatusPending,
			ParentNS:  "parent_ns_path",
			SchemaVer: compose_v2.CheckpointSchemaVer, // audit iter-5 H1: 业务 backend Marshal/Unmarshal 校验
			CreatedAt: 12345,
			// ISSUE-96: 成员身份**内联**。空 InstanceID 表达「该成员是静态边节点」，
			// 不是「没值」——两者语义不同，都要 round-trip（backend 别把空串这条整个丢掉）
			// 这一超步已开始执行（中断 / 失败之后的档）：漏了不报错，只是续跑时把开始过的超步当成没开始，
			// 运行期 before 断点在它前面多停一次
			FrontierStarted: true,
			CurrentFrontier: []compose_v2.FrontierMember{
				{NodeID: "node_a"},
				{NodeID: "node_b", InstanceID: "i7"},
			},
			// audit iter-3 视角 1 MED-2: 加 n3:0 zero-value 验业务 backend dropZero
			// 序列化策略 (e.g., protobuf [default=0] / Java Map dropZero) 不破坏
			// NodeVisits 协议 — crew-go 协议 (NodeVisits map[NodeID]int) 0 值
			// 跟未访问语义不同, 必保 round-trip
			NodeVisits: map[compose_v2.NodeID]int{"n1": 3, "n2": 1, "n3": 0},
			PendingInterrupts: []*compose_v2.AskQuery{
				// audit iter-3 视角 3 M6: AskQuery 加 Reason 字段验
				// q1 带工具身份（审批型挂起）；q2 留空（问答型挂起）——**空值也要
				// round-trip**：空表达「这条挂起不指向任何待执行动作」，跟「没存下来」
				// 是两回事，业务 backend 把空串写成 NULL 再读回非空就串档了。
				{InterruptID: "q1", Question: "Q1?", Reason: "因为用户操作",
					ToolCallID: "tc_abc", ToolName: "transfer"},
				{InterruptID: "q2", Question: "Q2?", Options: []string{"yes", "no"}, Reason: "需要确认"},
				// 声明式断点停下的挂起：Breakpoint 漏序列化不报错，但引擎靠它判「上次是不是停在断点上」——
				// 漏了的话，停在 before 断点上的档每次续跑都被重判、在同一个边界再停，图永远过不去（after 断点
				// 那种续跑照常放行）；调用方也从存档里分不出「这是断点、续跑即放行」与「节点在等答案」。
				{InterruptID: "q3", Question: "已在断点处暂停", Reason: compose_v2.BreakpointInterruptReason,
					Breakpoint: &compose_v2.Breakpoint{Before: []compose_v2.NodeID{"nb"},
						After: []compose_v2.NodeID{"na1", "na2"}, NS: "outer/sub"}},
			},
			// PA-H1: 部分失败超步成功成员 + 其 SendDirectives wire 形态——漏序列化
			// 任一字段时发散 fan-out HITL Resume 后成功分支后继静默丢失 graph 假成功
			CompletedFrontier: []compose_v2.FrontierMember{
				{NodeID: "done_a"},                   // 静态边成员：身份空
				{NodeID: "done_b", InstanceID: "i9"}, // fan-out 实例：带 token
			},
			// **承载后继的是这个字段**（出边计算归成员之后）：成功成员当时算好的出边目标。
			// 漏了不报错——只是它们的后继在 Resume 后静默不跑、graph 假成功。
			CompletedTargets: []compose_v2.NodeID{"succ_x", "succ_y"},
			// 已兑付的答案位——漏了不报错，只是「一个答案只能满足一个提问」这道兜底
			// 跨 Run 静默失效（身份算错时又变回静默满足，而不是当场终局失败）
			ConsumedAnswers: []string{"slot_a", "slot_b"},
			// 已提交的上行信号（转交 / 路由这类交给上方的决定）——漏了不报错，只是做出它的
			// 那一步续跑时不再执行，决定静默丢失（转交没发生、路由落到兜底）
			Signals: []compose_v2.SignalRecord{
				{Kind: "conf.transfer", Dest: "desk@outer", Value: "billing", Origin: "call_1#0"},
				{Kind: "conf.route", Dest: "rt@", Value: "math"},
			},
			// 部分失败超步里成功成员先记在一边的信号——漏了不报错，只是成功兄弟做出的决定续跑后
			// 静默丢失
			CompletedSignals: []compose_v2.SignalRecord{
				{Kind: "conf.transfer", Dest: "desk@inner", Value: "sales", Origin: "done_a"},
			},
			// 铸收件人的密钥（任意字节，含 0 与高位）——漏了续跑时重铸、收件人变了
			RecipientKey: []byte{0x00, 0x01, 0xfe, 0xff, 'k', 0x7f},
			Epoch:        "e0f1a2b3c4d5e6f7", // 世代标记（WithStaleWriteGuard 用）
			TaskID:       "t0a1b2c3d4e5f607", // 任务代号（中断寻址里唯一一个跨任务的维度）
			InputDigest:  "d1e2f3a4b5c6d7e8", // 新开时那次调用的输入摘要（续跑时比对）
			DeferredSends: []compose_v2.SerializedSend{
				{Target: "x_node", Owner: "done_a", Payload: []compose_v2.SerializedWrite{
					{Channel: "ch1", Type: "int", Data: json.RawMessage("42")},
				}},
				// ISSUE-96：发出者自己是 fan-out 实例之一 → OwnerInstanceID 非空
				{Target: "y_node", Owner: "done_b", OwnerInstanceID: "i9", InstanceID: "i12"}, // 空 payload（只调度不传参）
			},
			// F-B: 没能提交、待 Resume 补 apply 的 SendDirective payload——
			// 漏了 target（已在 CurrentFrontier 里）Resume 后静默读到零值
			PendingSendWrites: []compose_v2.SerializedWrite{
				{Channel: "ch2", Type: "string", Data: json.RawMessage(`"pending"`)},
			},
		}
		if err := cp.SaveNS(ctx, "sid_full", "ns_full", original); err != nil {
			t.Fatalf("SaveNS: %v", err)
		}

		loaded, err := cp.LoadNS(ctx, "sid_full", "ns_full")
		if err != nil || loaded == nil {
			t.Fatalf("LoadNS: err=%v cp=%v", err, loaded)
		}

		// 字段一一对照。
		//
		// **期望值一律写字面量，不读 `original`**：`SaveNS` 的契约只要求把
		// sessionID/ns 等**填进存储的那份**，从没要求原地改调用方传进来的对象。
		// 跨进程 backend（marshal 后落库）天然不碰调用方指针，而 InMemory 恰好
		// 原地写回——拿 `original` 当期望值等于把「实现细节」当成了契约：一个
		// 完全合规的跨进程 backend 会被判失败，不合规的反而通过。
		//
		// 这个陷阱曾被 fixture 掩盖（下面几个字段都预填了非空值，auto-fill 分支
		// 不触发）；谁把 fixture "简化"掉一个预填就会炸，而且炸的是无辜的 backend。
		if loaded.GraphID != "graph_full" {
			t.Errorf("GraphID: expected %q, got %q", "graph_full", loaded.GraphID)
		}
		if loaded.Status != compose_v2.CPStatusPending {
			t.Errorf("Status: expected %v, got %v", compose_v2.CPStatusPending, loaded.Status)
		}
		if loaded.ParentNS != "parent_ns_path" {
			t.Errorf("ParentNS: expected %q, got %q", "parent_ns_path", loaded.ParentNS)
		}
		if loaded.SchemaVer != compose_v2.CheckpointSchemaVer {
			t.Errorf("SchemaVer: expected %d (framework current), got %d",
				compose_v2.CheckpointSchemaVer, loaded.SchemaVer)
		}
		if loaded.CreatedAt != 12345 {
			t.Errorf("CreatedAt: expected %d, got %d", 12345, loaded.CreatedAt)
		}

		if !loaded.FrontierStarted {
			t.Error("FrontierStarted round-trip lost: want true（业务 backend 漏 frontier_started 字段）")
		}

		// CurrentFrontier 顺序敏感 + 成员身份内联（ISSUE-96）
		if len(loaded.CurrentFrontier) != 2 ||
			loaded.CurrentFrontier[0] != (compose_v2.FrontierMember{NodeID: "node_a"}) ||
			loaded.CurrentFrontier[1] != (compose_v2.FrontierMember{NodeID: "node_b", InstanceID: "i7"}) {
			t.Errorf("CurrentFrontier round-trip lost: got %+v\n"+
				"  want [{node_a } {node_b i7}]（顺序敏感）\n"+
				"  → 漏 NodeID：Resume 无从寻址；漏 InstanceID：多实例场景所有实例身份退化成"+
				"空串、互相冒充 → 静默丢数据（ISSUE-96）。**空 InstanceID 的那条也必须保留**——"+
				"它表达「该成员是静态边节点」，不是「没值」",
				loaded.CurrentFrontier)
		}

		// **CompletedTargets**：成功成员当时算好的出边目标——顺序敏感（下一帧的装配次序）。
		// 漏序列化**不报错**，只是那批成功分支的后继静默不跑、graph 报成功。
		if len(loaded.CompletedTargets) != 2 ||
			loaded.CompletedTargets[0] != "succ_x" || loaded.CompletedTargets[1] != "succ_y" {
			t.Errorf("CompletedTargets round-trip lost: got %v (want [succ_x succ_y])\n"+
				"  出边计算归成员之后，成功成员的后继由本字段承载（不是 CompletedFrontier）——\n"+
				"  漏了它：发散 fan-out 部分失败 Resume 后，成功分支的后继永远不跑且 err=nil",
				loaded.CompletedTargets)
		}

		// **ConsumedAnswers**：已兑付的答案位。漏序列化**不报错**，只是那道
		// 「一个答案只能满足一个提问」的兜底跨 Run 失效——身份算错时又变回静默满足。
		if len(loaded.ConsumedAnswers) != 2 ||
			loaded.ConsumedAnswers[0] != "slot_a" || loaded.ConsumedAnswers[1] != "slot_b" {
			t.Errorf("ConsumedAnswers round-trip lost: got %v (want [slot_a slot_b])\n"+
				"  漏了它：调用方累积传的旧答案会在下一次 Run 里静默满足一个新提问，"+
				"用户根本没被问到而 err == nil", loaded.ConsumedAnswers)
		}

		// **Signals**：已提交的上行信号。漏序列化不报错，只是续跑后那些决定静默丢失。
		wantSignals := []compose_v2.SignalRecord{
			{Kind: "conf.transfer", Dest: "desk@outer", Value: "billing", Origin: "call_1#0"},
			{Kind: "conf.route", Dest: "rt@", Value: "math"},
		}
		if len(loaded.Signals) != len(wantSignals) ||
			loaded.Signals[0] != wantSignals[0] || loaded.Signals[1] != wantSignals[1] {
			t.Errorf("Signals round-trip lost: got %+v (want %+v)\n"+
				"  漏了它：做出转交 / 路由决定的那一步已经提交、续跑时不再执行，"+
				"决定静默丢失（转交没发生、路由落到兜底）而 err == nil", loaded.Signals, wantSignals)
		}
		wantCompletedSig := compose_v2.SignalRecord{Kind: "conf.transfer", Dest: "desk@inner", Value: "sales", Origin: "done_a"}
		if len(loaded.CompletedSignals) != 1 || loaded.CompletedSignals[0] != wantCompletedSig {
			t.Errorf("CompletedSignals round-trip lost: got %+v (want [%+v])\n"+
				"  漏了它：部分失败超步里成功兄弟做出的转交 / 路由决定续跑后静默丢失而 err == nil",
				loaded.CompletedSignals, wantCompletedSig)
		}
		if want := []byte{0x00, 0x01, 0xfe, 0xff, 'k', 0x7f}; string(loaded.RecipientKey) != string(want) {
			t.Errorf("RecipientKey round-trip lost: got %v (want %v)\n"+
				"  漏了它：续跑时密钥重铸、收件人变了，停下前已提交的转交 / 路由决定续跑后静默丢失"+
				"而 err == nil", loaded.RecipientKey, want)
		}

		// PA-H1: CompletedFrontier 顺序敏感（出边计算次序确定性）+ 身份内联
		if len(loaded.CompletedFrontier) != 2 ||
			loaded.CompletedFrontier[0] != (compose_v2.FrontierMember{NodeID: "done_a"}) ||
			loaded.CompletedFrontier[1] != (compose_v2.FrontierMember{NodeID: "done_b", InstanceID: "i9"}) {
			t.Errorf("CompletedFrontier round-trip lost: got %+v\n"+
				"  want [{done_a } {done_b i9}]（顺序敏感）\n"+
				"  → 漏这一段：发散 fan-out HITL Resume 后成功分支后继静默丢；\n"+
				"    漏 InstanceID：重跑成员的旧记录按 {NodeID,\"\"} 匹配不上带身份的重跑成员 → 清不掉 → Target 双发",
				loaded.CompletedFrontier)
		}
		// Epoch 世代标记——漏了不报错，只是 WithStaleWriteGuard 静默失效
		//（守卫读到空 Epoch 就跳过比对，业务以为开了保护其实没有）
		if loaded.Epoch != "e0f1a2b3c4d5e6f7" {
			t.Errorf("Epoch round-trip lost: got %q (want \"e0f1a2b3c4d5e6f7\")\n"+
				"  漏此字段 → WithStaleWriteGuard 恒跳过比对 → 并发写回退回静默覆盖",
				loaded.Epoch)
		}
		// TaskID 任务代号——**漏了同样不报错，但后果有两层，第二层比第一层重**：
		//  1. 中断寻址退回「维度全是位置」，而位置在新任务里全部重置 ⟹ 同一 sid 下两个
		//     任务的中断 ID **确定性相等** ⟹ 给任务 A 的答案被任务 B 的问题消费掉
		//  2. **更重的一层：HITL 闭环本身会瘸**——首跑算 ID 时代号还在（内存里），存档里
		//     被丢了，续跑读回空值 ⟹ 两次算出的 ID 不同 ⟹ 调用方攥着的答案配不上，得多
		//     绕一轮才收敛。
		// 两层都 `err == nil` 全程静默。
		if loaded.TaskID != "t0a1b2c3d4e5f607" {
			t.Errorf("TaskID round-trip lost: got %q (want \"t0a1b2c3d4e5f607\")\n"+
				"  漏此字段的后果（两层，都静默）：\n"+
				"   1. 中断寻址退回纯位置 → 同 sid 下跨任务 ID 必撞 →\n"+
				"      用户批准的是这件事、系统执行的是另一件事（实测过）\n"+
				"   2. 首跑与续跑算出的 ID 不同 → 调用方手里的答案配不上 → HITL 要多绕一轮",
				loaded.TaskID)
		}
		// InputDigest 摘要——漏了不报错，但两件事变得看不见：两次不同的调用撞进同一个存档槽时
		// 认不出来（后一次续上前一次的进度、`err == nil`）；带 key 的调用输入变了的告警不再出现。
		if loaded.InputDigest != "d1e2f3a4b5c6d7e8" {
			t.Errorf("InputDigest round-trip lost: got %q (want \"d1e2f3a4b5c6d7e8\")\n"+
				"  漏此字段 → 撞槽检测与输入对不上的告警都静默失效",
				loaded.InputDigest)
		}

		// PA-H1: DeferredSends 全字段 round-trip（Target + Payload 的 Channel/Type/Data）
		// ISSUE-96 起还含 OwnerInstanceID —— 与 Owner 配对拼出发出者的完整成员身份
		if len(loaded.DeferredSends) != 2 {
			t.Fatalf("DeferredSends len: expected 2, got %d", len(loaded.DeferredSends))
		}
		if loaded.DeferredSends[1].InstanceID != "i12" {
			t.Errorf("DeferredSends[1].InstanceID round-trip lost: got %q (want \"i12\")\n"+
				"  这是**目标**实例的身份（区别于 OwnerInstanceID 的\"谁发的\"）——漏了 Resume 重放出来的\n"+
				"  N 个实例全拿空串彼此无法区分 ⟹ HITL InterruptID 撞车 / 子 cp 互覆 / 观测 span 共槽",
				loaded.DeferredSends[1].InstanceID)
		}
		if loaded.DeferredSends[1].OwnerInstanceID != "i9" {
			t.Errorf("DeferredSends[1].OwnerInstanceID round-trip lost: got %q (want \"i9\")\n"+
				"  漏此字段 → purge 用的匹配键恒为 {Owner,\"\"}，跟带实例身份的重跑成员永远匹配不上\n"+
				"  ⟹ 旧 send 清不掉，Resume 时重放叠上重跑新产 = Target 双发",
				loaded.DeferredSends[1].OwnerInstanceID)
		}
		if loaded.DeferredSends[0].Target != "x_node" || len(loaded.DeferredSends[0].Payload) != 1 {
			t.Errorf("DeferredSends[0] round-trip lost: %+v", loaded.DeferredSends[0])
		} else {
			pw := loaded.DeferredSends[0].Payload[0]
			if pw.Channel != "ch1" || pw.Type != "int" || string(pw.Data) != "42" {
				t.Errorf("DeferredSends[0].Payload[0] 字段丢失: %+v", pw)
			}
		}
		if loaded.DeferredSends[1].Target != "y_node" || len(loaded.DeferredSends[1].Payload) != 0 {
			t.Errorf("DeferredSends[1]（空 payload）round-trip lost: %+v", loaded.DeferredSends[1])
		}
		// issue-95#1: Owner 是这批里最隐蔽的字段——漏了不报错、不丢数据，只是
		// Resume 重跑某节点时它的旧 SendFrom 清不掉 → Target 被调度两次（业务侧
		// 表现为"某个 worker 莫名跑两遍"，极难归因到序列化）
		if loaded.DeferredSends[0].Owner != "done_a" || loaded.DeferredSends[1].Owner != "done_b" {
			t.Errorf("DeferredSends Owner round-trip lost: got[0].Owner=%q got[1].Owner=%q (want done_a / done_b)\n"+
				"  → 重跑目标的旧 SendFrom 清不掉 → Target 双发（issue-95#1）",
				loaded.DeferredSends[0].Owner, loaded.DeferredSends[1].Owner)
		}
		// F-B: PendingSendWrites 全字段 round-trip——漏了 SendFrom target 在 Resume
		// 后静默读到零值（不报错、结果错，同样极难归因到序列化）
		if len(loaded.PendingSendWrites) != 1 {
			t.Errorf("PendingSendWrites len: expected 1, got %d (业务 backend 漏此字段 → SendFrom target Resume 后读零值)",
				len(loaded.PendingSendWrites))
		} else {
			pw := loaded.PendingSendWrites[0]
			if pw.Channel != "ch2" || pw.Type != "string" || string(pw.Data) != `"pending"` {
				t.Errorf("PendingSendWrites[0] 字段丢失: %+v", pw)
			}
		}

		// NodeVisits map 含 zero value n3:0
		if len(loaded.NodeVisits) != 3 {
			t.Errorf("NodeVisits len: expected 3 (含 n3:0), got %d (业务 backend dropZero 序列化破坏协议)",
				len(loaded.NodeVisits))
		}
		// 验每个值, 含 zero value (audit iter-3 视角 1 MED-2)
		expectedVisits := map[compose_v2.NodeID]int{"n1": 3, "n2": 1, "n3": 0}
		for k, v := range expectedVisits {
			if loaded.NodeVisits[k] != v {
				t.Errorf("NodeVisits[%q]: expected %d, got %d (业务 backend 可能 dropZero 或值乱)",
					k, v, loaded.NodeVisits[k])
			}
		}

		// PendingInterrupts slice + InterruptID 是 HITL Resume 关键 — 漏序列化
		// 直接导致 ask_user 答案配对失败 (CLAUDE.md 5 维稳定 ID).
		if len(loaded.PendingInterrupts) != 3 {
			t.Fatalf("PendingInterrupts len: expected 3, got %d", len(loaded.PendingInterrupts))
		}
		// q3：断点信息三个字段都要回来，问答型的 q1 / q2 上必须仍是 nil
		if bp := loaded.PendingInterrupts[2].Breakpoint; bp == nil ||
			!slices.Equal(bp.Before, []compose_v2.NodeID{"nb"}) ||
			!slices.Equal(bp.After, []compose_v2.NodeID{"na1", "na2"}) || bp.NS != "outer/sub" {
			t.Errorf("PendingInterrupts[2].Breakpoint 没有原样回来（业务 backend 漏 breakpoint 字段），got %+v", bp)
		}
		if loaded.PendingInterrupts[0].Breakpoint != nil || loaded.PendingInterrupts[1].Breakpoint != nil {
			t.Error("问答型挂起的 Breakpoint 应为 nil——非 nil 说明 backend 把别处的值串了过来")
		}

		// q1: InterruptID + Question + Reason 必 round-trip
		if loaded.PendingInterrupts[0].InterruptID != "q1" {
			t.Errorf("PendingInterrupts[0].InterruptID: expected q1, got %q",
				loaded.PendingInterrupts[0].InterruptID)
		}
		if loaded.PendingInterrupts[0].Question != "Q1?" {
			t.Errorf("PendingInterrupts[0].Question: expected 'Q1?', got %q (业务 backend 漏 Question 字段)",
				loaded.PendingInterrupts[0].Question)
		}
		if loaded.PendingInterrupts[0].Reason != "因为用户操作" {
			t.Errorf("PendingInterrupts[0].Reason: expected '因为用户操作', got %q (业务 backend 漏 Reason 字段 → MergeInterrupts 聚合时丢)",
				loaded.PendingInterrupts[0].Reason)
		}
		// 工具身份：审批型挂起靠它认出「这条挂起卡的是哪次调用」。漏序列化不报错，
		// 只是挂起被放弃时补不出能配对的合成结果 → 历史里留一条永远悬空的 tool_call。
		if loaded.PendingInterrupts[0].ToolCallID != "tc_abc" {
			t.Errorf("PendingInterrupts[0].ToolCallID: expected tc_abc, got %q (业务 backend 漏 tool_call_id 字段)",
				loaded.PendingInterrupts[0].ToolCallID)
		}
		if loaded.PendingInterrupts[0].ToolName != "transfer" {
			t.Errorf("PendingInterrupts[0].ToolName: expected transfer, got %q (业务 backend 漏 tool_name 字段)",
				loaded.PendingInterrupts[0].ToolName)
		}
		// q2 是问答型：两个字段必须**保持空**——非空说明 backend 把别处的值串了过来
		if loaded.PendingInterrupts[1].ToolCallID != "" || loaded.PendingInterrupts[1].ToolName != "" {
			t.Errorf("PendingInterrupts[1] 工具身份应为空（问答型挂起不指向动作），got (%q, %q)",
				loaded.PendingInterrupts[1].ToolCallID, loaded.PendingInterrupts[1].ToolName)
		}

		// q2: InterruptID + Question + Options 顺序值 + Reason
		if loaded.PendingInterrupts[1].InterruptID != "q2" {
			t.Errorf("PendingInterrupts[1].InterruptID: expected q2, got %q",
				loaded.PendingInterrupts[1].InterruptID)
		}
		if loaded.PendingInterrupts[1].Question != "Q2?" {
			t.Errorf("PendingInterrupts[1].Question: expected 'Q2?', got %q",
				loaded.PendingInterrupts[1].Question)
		}
		if loaded.PendingInterrupts[1].Reason != "需要确认" {
			t.Errorf("PendingInterrupts[1].Reason: expected '需要确认', got %q",
				loaded.PendingInterrupts[1].Reason)
		}
		if len(loaded.PendingInterrupts[1].Options) != 2 {
			t.Fatalf("PendingInterrupts[1].Options len: expected 2, got %d", len(loaded.PendingInterrupts[1].Options))
		}
		if loaded.PendingInterrupts[1].Options[0] != "yes" || loaded.PendingInterrupts[1].Options[1] != "no" {
			t.Errorf("PendingInterrupts[1].Options order lost: expected [yes,no], got %v",
				loaded.PendingInterrupts[1].Options)
		}
	})

	// audit iter-3 视角 1 MED-1: ParentNS auto-fill 协议测试
	// (`InMemoryCheckpointer.SaveNS` 参考实现):
	// `if cp.ParentNS == "" && ns != "" { cp.ParentNS = ParentNS(ns) }`.
	// 业务 backend 实现 SaveNS 时若不调 auto-fill 逻辑, 嵌套
	// 子图 cp 的 ParentNS 字段会丢 → O(1) 反查祖先失败. 单独 subtest 验.
	t.Run("SaveNS_AutoFills_ParentNS_From_NS", func(t *testing.T) {
		cp := factory()
		ctx := context.Background()

		// 嵌套 ns = "parent/child", 业务 backend 应 auto-fill ParentNS="parent"
		c := newCheckpoint("g", compose_v2.CPStatusUnset) // 不预填 ParentNS
		if err := cp.SaveNS(ctx, "sid", "parent/child", c); err != nil {
			t.Fatalf("SaveNS: %v", err)
		}

		loaded, _ := cp.LoadNS(ctx, "sid", "parent/child")
		if loaded == nil {
			t.Fatal("LoadNS expected non-nil")
		}
		if loaded.ParentNS != "parent" {
			t.Errorf("SaveNS should auto-fill ParentNS='parent' from ns='parent/child', got %q (业务 backend 漏 auto-fill 协议)",
				loaded.ParentNS)
		}
	})

	// audit LOW fix（兑现原"改用 framework 内部范式"注释承诺，消除 Skip gap）: Channels
	// round-trip — **最关键字段**（graph state 本体），业务 backend 漏序列化 taggedValue.tag
	// → Resume 时 channel value type assertion 失败 → BasicAgent ReAct 跨 InterruptError 崩。
	// conformance 内跑 framework 范式（cm.Snapshot 产带 Channels 的真 cp，不暴露私有
	// taggedValue），验业务 backend SaveNS/LoadNS 对 Channels round-trip 保真。
	t.Run("SaveNS_Channels_Roundtrip", func(t *testing.T) {
		ctx := context.Background()
		// 幂等注册（同 name 同 type 不 panic）——conformance 不 import agent_v2，自行注册。
		compose_v2.RegisterValueType[confChannelVal]("checkpointer_conformance.confChannelVal")
		cper := factory()
		// 1. cm.Snapshot 产带 Channels 的真 cp（走 framework taggedValue 序列化路径）。
		cm := compose_v2.NewChannelManager()
		ch := compose_v2.LastValue[confChannelVal]("conf_ch")
		if err := compose_v2.RegisterChannel(cm, ch); err != nil {
			t.Fatalf("RegisterChannel: %v", err)
		}
		if err := cm.ApplyAll([]compose_v2.ChannelWrite{compose_v2.Write(ch, confChannelVal{V: "channel-value-v1"})}); err != nil {
			t.Fatalf("ApplyAll: %v", err)
		}
		snap, err := cm.Snapshot()
		if err != nil {
			t.Fatalf("Snapshot: %v", err)
		}
		snap.GraphID = "g-ch"
		// 2. backend SaveNS + LoadNS round-trip。
		if err := cper.SaveNS(ctx, "sid-ch", "", snap); err != nil {
			t.Fatalf("SaveNS: %v", err)
		}
		loaded, err := cper.LoadNS(ctx, "sid-ch", "")
		if err != nil || loaded == nil {
			t.Fatalf("LoadNS: err=%v loaded=%v", err, loaded)
		}
		// 3. Restore 到新 cm 验 channel value 跨 backend round-trip 保真（taggedValue 没丢）。
		cm2 := compose_v2.NewChannelManager()
		ch2 := compose_v2.LastValue[confChannelVal]("conf_ch")
		if err := compose_v2.RegisterChannel(cm2, ch2); err != nil {
			t.Fatalf("RegisterChannel cm2: %v", err)
		}
		if err := cm2.Restore(loaded); err != nil {
			t.Fatalf("Restore（业务 backend 可能漏序列化 taggedValue.tag）: %v", err)
		}
		got, ok := compose_v2.Get(cm2.Channels(), ch2)
		if !ok || got.V != "channel-value-v1" {
			t.Errorf("Channels round-trip 失真: got=%+v ok=%v（期望 V=channel-value-v1）—— backend 漏序列化 Channels/taggedValue", got, ok)
		}
	})

	// 节点私有中断态（SchemaVer 11）：让「一个节点内做了 N 件带副作用的事」在中断重跑时
	// 跳过已完成的那几件。漏序列化**不报错**——只是节点读不到上次进度、已成功的副作用
	// 在 Resume 时重放一遍（下单 / 发消息 / 扣款各来第二遍），是最难查的一档。
	//
	// 走真图跑出 cp（同 Channels 那条的范式）而不是手搓 `taggedValue`——它是未导出类型，
	// 且手搓等于绕开框架的序列化路径、测不到真行为。
	//
	// 同一次节点执行里还按 key 记了一步（SaveStepResult，`agent_v2.RunSubAgent` 靠它记住已
	// 完成的嵌套调用）：它在 NodeStates 里是另一条派生键，backend 要跟前一条一样原样存取。
	// 业务给的 key 故意带 NUL 与非法 UTF-8——框架只把 key 的哈希放进存档，backend 看到的键
	// 恒是可打印字符；曾经派生键里带 NUL，PostgreSQL 的 JSONB 拒掉整份存档（实测）。
	t.Run("SaveNS_NodeStates_Roundtrip", func(t *testing.T) {
		ctx := context.Background()
		compose_v2.RegisterValueType[confChannelVal]("checkpointer_conformance.confChannelVal")
		cper := factory()

		// 1. 一个中断的节点，中断前先 SaveNodeState 记下「已经做到哪儿」
		b := compose_v2.NewBuilder().WithName("conf_ns_graph")
		b.AddNode("worker", func(nodeCtx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			if err := compose_v2.SaveNodeState(nodeCtx, confChannelVal{V: "done-step-1"}); err != nil {
				return nil, err
			}
			if err := compose_v2.SaveStepResult(nodeCtx, confStepKey, confChannelVal{V: "charged"}); err != nil {
				return nil, err
			}
			return nil, &compose_v2.InterruptError{
				Query: &compose_v2.AskQuery{InterruptID: "conf_ns_q", Question: "?"},
			}
		})
		b.AddEdge(compose_v2.StartNode, "worker")
		b.AddEdge("worker", compose_v2.EndNode)
		g, err := b.Compile()
		if err != nil {
			t.Fatalf("Compile: %v", err)
		}
		runErr := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
			compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("sid-ns"))
		if runErr == nil {
			t.Fatal("应中断")
		}

		// 2. backend 存的那份必须带 NodeStates
		loaded, err := cper.LoadNS(ctx, "sid-ns", "")
		if err != nil || loaded == nil {
			t.Fatalf("LoadNS: err=%v loaded=%v——存档没写进 backend（首跑返回：%v）", err, loaded, runErr)
		}
		if len(loaded.NodeStates) == 0 {
			t.Fatalf("NodeStates 丢了——backend 漏序列化 `node_states` 字段。\n" +
				"  后果不报错：节点 Resume 时读不到「上次已经做完哪些」，已成功的副作用重放一遍")
		}

		// 3. Restore 到新 cm 后节点必须读回**同一个值**（tag 没丢）
		cm2 := compose_v2.NewChannelManager()
		if err := cm2.Restore(loaded); err != nil {
			t.Fatalf("Restore: %v", err)
		}
		var seen, step confChannelVal
		var sawState, sawStep bool
		b2 := compose_v2.NewBuilder().WithName("conf_ns_graph")
		b2.AddNode("worker", func(nodeCtx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			seen, sawState = compose_v2.GetNodeState[confChannelVal](nodeCtx)
			step, sawStep = compose_v2.GetStepResult[confChannelVal](nodeCtx, confStepKey)
			return nil, nil
		})
		b2.AddEdge(compose_v2.StartNode, "worker")
		b2.AddEdge("worker", compose_v2.EndNode)
		g2, err := b2.Compile()
		if err != nil {
			t.Fatalf("Compile g2: %v", err)
		}
		if err := compose_v2.Run(ctx, g2, cm2,
			compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("sid-ns"),
			compose_v2.WithResumeAnswers(map[string]*schema.Message{
				"conf_ns_q": schema.UserMessage("ok"),
			})); err != nil {
			t.Fatalf("Resume: %v", err)
		}
		if !sawState || seen.V != "done-step-1" {
			t.Errorf("节点没读回上次的进度: got=%+v ok=%v（期望 V=done-step-1）\n"+
				"  backend 漏序列化 node_states 里的 taggedValue.tag 时就是这个形状",
				seen, sawState)
		}
		if !sawStep || step.V != "charged" {
			t.Errorf("节点没读回按 key 记下的那一步: got=%+v ok=%v（期望 V=charged）\n"+
				"  漏了不报错：已完成的嵌套调用在续跑时重跑一遍、副作用重放", step, sawStep)
		}
	})
}

// confStepKey 是 NodeStates 用例里按 key 记下的那一步的业务 key——故意带 NUL 与非法 UTF-8。
const confStepKey = "charge\x00order-7\xff"
