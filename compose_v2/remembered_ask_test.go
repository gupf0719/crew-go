package compose_v2_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// 不变量：跳过一问不改变后面提问的 ID。
//
// 提问的 ID 里有一维是所在相位的提问序号。按上一次执行记下的进度跳过某一问（答案已经用 SaveStepResult
// 记住），节点体的序号就少 tick 一次，后面那一问落到被跳过那一问的 ID 上：调用方累积传答案时，它拿到的是
// 被跳过那一问的答案，`err == nil`（实测）。推荐写法是把能被跳过的那一问放进自己的相位（EnterPhase）：它有
// 自己的序号，跳不跳过都不动节点体的。

// rememberedTemplateBody 是推荐写法：「用哪个模板」答一次就记住（不带 Rerunnable，被否决重做也保留），
// 记住之后跳过；这一问在自己的相位里问。之后问「批准用模板 X 发出吗」。got 依次记下批准那一问每次拿到的答案。
func rememberedTemplateBody(got *[]string) func(ctx context.Context) (string, error) {
	return func(ctx context.Context) (string, error) {
		tpl, ok := compose_v2.GetStepResult[string](ctx, "tpl")
		if !ok {
			a, err := compose_v2.Ask(compose_v2.EnterPhase(ctx, "tpl"), &compose_v2.AskQuery{Question: "用哪个模板？"})
			if err != nil {
				return "", err
			}
			if err := compose_v2.SaveStepResult(ctx, "tpl", a.Content); err != nil {
				return "", err
			}
			tpl = a.Content
		}
		b, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "批准用模板 " + tpl + " 发出吗？"})
		if err != nil {
			return "", err
		}
		*got = append(*got, b.Content)
		return b.Content, nil
	}
}

// TestAsk_SkippedRememberedQuestion_LaterQuestionKeepsItsID：纯续跑。模板那一问答过、记住，续跑时跳过它；
// 批准那一问拿到的是它自己的答案。
//
// 模板那一问跟批准那一问在同一个相位里问时，跳过前者让后者挪到它的 ID 上，批准那一问拿到的是
// 「答:用哪个模板？」（实测）。
func TestAsk_SkippedRememberedQuestion_LaterQuestionKeepsItsID(t *testing.T) {
	var got []string
	body := rememberedTemplateBody(&got)
	g := retryAskGraph(t, func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		_, err := body(ctx)
		return nil, err
	})
	asked, err := retryAskDrive(t, g)
	if err != nil {
		t.Fatalf("没收敛：%v（问过 %q）", err, asked)
	}
	if want := []string{"用哪个模板？", "批准用模板 答:用哪个模板？ 发出吗？"}; !slices.Equal(asked, want) {
		t.Fatalf("问过 %q，want %q", asked, want)
	}
	if want := []string{"答:批准用模板 答:用哪个模板？ 发出吗？"}; !slices.Equal(got, want) {
		t.Errorf("批准那一问拿到 %q，want %q——它挪到了被跳过那一问的 ID 上", got, want)
	}
}

// TestAsk_RememberedQuestion_KeptAcrossRedo：条件边按 CondFailRerunFrom 否决第一次的结果、节点重做。记住的
// 模板跨重做保留、不再问；批准那一问在重做里是新问题（重做代际变了），重新问、用新的答案。
func TestAsk_RememberedQuestion_KeptAcrossRedo(t *testing.T) {
	var got []string
	body := rememberedTemplateBody(&got)
	resCh := compose_v2.LastValue[string]("res")
	b := compose_v2.NewBuilder().WithName("remembered_redo")
	compose_v2.WithChannel(b, resCh)
	b.AddNode("n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		r, err := body(ctx)
		if err != nil {
			return nil, err
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(resCh, r)}, nil
	})
	b.AddEdge(compose_v2.StartNode, "n")
	rejected := false
	b.AddConditionalEdge("n", func(context.Context, compose_v2.Channels) (compose_v2.NodeID, error) {
		if !rejected {
			rejected = true
			return "", errors.New("第一次的结果不合格")
		}
		return compose_v2.EndNode, nil
	}, compose_v2.WithCondFailPolicy(compose_v2.CondFailRerunFrom))
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	opts := []compose_v2.RunOption{compose_v2.WithCheckpointer(compose_v2.NewInMemoryCheckpointer()), compose_v2.WithSessionID("s")}
	answers := map[string]*schema.Message{}
	var asked []string
	cm := compose_v2.NewChannelManager()
	err = compose_v2.Run(context.Background(), g, cm, opts...)
	// 停在提问上就作答（答#1、答#2……累积着传）；被条件边否决停下时不带新答案续跑一次
	for i := 0; i < 8 && err != nil; i++ {
		if ie, ok := compose_v2.IsInterrupt(err); ok {
			for _, q := range ie.AllQueries() {
				asked = append(asked, q.Question)
				answers[q.InterruptID] = schema.UserMessage(fmt.Sprintf("答#%d", len(asked)))
			}
		} else if errors.Is(err, compose_v2.ErrRunAborted) {
			break
		}
		cm = compose_v2.NewChannelManager()
		err = compose_v2.Run(context.Background(), g, cm, append(opts, compose_v2.WithResumeAnswers(answers))...)
	}
	if err != nil {
		t.Fatalf("没收敛：%v（问过 %q）", err, asked)
	}
	if !rejected {
		t.Fatal("fixture 没搭对：条件边一次都没否决")
	}
	want := []string{"用哪个模板？", "批准用模板 答#1 发出吗？", "批准用模板 答#1 发出吗？"}
	if !slices.Equal(asked, want) {
		t.Errorf("问过 %q，want %q（模板只问一次、批准在重做里重新问）", asked, want)
	}
	// 被否决的那一次用的是第一次问到的批准（答#2），重做那一次用重新问到的（答#3）——任何一次拿到模板那一问
	// 的答案（答#1）都是挪了位
	if want := []string{"答#2", "答#3"}; !slices.Equal(got, want) {
		t.Errorf("批准那一问依次拿到 %q，want %q", got, want)
	}
	if res, _ := compose_v2.Get(cm.Channels(), resCh); res != "答#3" {
		t.Errorf("最终结果 = %q，want 答#3（重做后批准用的是重新问到的答案）", res)
	}
}

// rememberedDone 是下面那条用例记进度用的值（做完的件 → 当时拿到的确认）。
type rememberedDone map[string]string

func init() { compose_v2.RegisterValueType[rememberedDone]("compose_v2_test.rememberedDone") }

// TestAsk_SkippedDoneItems_EachItemKeepsItsQuestion：SaveNodeState 的典型用法——逐件做事、做完一件记一次进度、
// 重跑时跳过做完的——每件做之前问一次确认，提问在每件自己的相位里问。续跑时跳过做完的 A，B 的确认拿到的是
// 它自己的答案。
//
// 两件都在节点体里问时，跳过 A 让 B 的确认挪到 A 那一问的 ID 上，B 拿到的是「答:确认 A 吗？」（实测）。
func TestAsk_SkippedDoneItems_EachItemKeepsItsQuestion(t *testing.T) {
	got := map[string]string{}
	g := retryAskGraph(t, func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		done := rememberedDone{}
		if prev, ok := compose_v2.GetNodeState[rememberedDone](ctx); ok && prev != nil {
			done = prev
		}
		for _, item := range []string{"A", "B"} {
			if _, already := done[item]; already {
				continue
			}
			a, err := compose_v2.Ask(compose_v2.EnterPhase(ctx, "confirm:"+item), &compose_v2.AskQuery{Question: "确认 " + item + " 吗？"})
			if err != nil {
				return nil, err
			}
			got[item] = a.Content // 做这件事用的是这个确认
			done[item] = a.Content
			if err := compose_v2.SaveNodeState(ctx, done); err != nil {
				return nil, err
			}
		}
		return nil, nil
	})
	asked, err := retryAskDrive(t, g)
	if err != nil {
		t.Fatalf("没收敛：%v（问过 %q）", err, asked)
	}
	if want := []string{"确认 A 吗？", "确认 B 吗？"}; !slices.Equal(asked, want) {
		t.Fatalf("问过 %q，want %q", asked, want)
	}
	if got["A"] != "答:确认 A 吗？" || got["B"] != "答:确认 B 吗？" {
		t.Errorf("A 拿到 %q、B 拿到 %q，want 各拿各的确认——B 挪到了跳过的 A 那一问的 ID 上", got["A"], got["B"])
	}
}
