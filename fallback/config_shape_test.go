package fallback_test

import (
	"reflect"
	"testing"

	"crew-go/compose_v2"
	"crew-go/fallback"
)

// TestPerModelRetry_HasNoPredicateField：**这一层的参数表里不能有判据字段。**
//
// 曾经它直接吃 `compose_v2.RetryConfig`，而那个类型带着 `ShouldRetry`——本层不读它。
// 后果实测过：业务照那个字段的文档传了谓词说「别重试」，谓词**一次都没被咨询**，
// 副作用照样重放三次，两边都不报错；而字段文档写着「必填，不给就 panic」，那句话
// 在本层为假。
//
// 文档限定作用域只能让人别踩，拆开类型才能让人**踩不到**。这条哨兵钉住后者：
// 任何人把判据字段加回本层参数表（或让它重新去吃那个带判据的类型），立刻红。
func TestPerModelRetry_HasNoPredicateField(t *testing.T) {
	rt := reflect.TypeOf(fallback.PerModelRetry{})
	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		if f.Type.Kind() == reflect.Func {
			t.Errorf("本层参数表里出现了判据字段 %s %v——判据在这一层由 FailoverPolicy.ShouldFailover 给，"+
				"放一个不被咨询的函数字段进来，业务就会传它、然后静默失效", f.Name, f.Type)
		}
	}

	// 反向：编排层那个类型**该**有判据（两者的差别正是拆分的理由）。
	rc := reflect.TypeOf(compose_v2.RetryConfig{})
	if _, ok := rc.FieldByName("ShouldRetry"); !ok {
		t.Error("编排层的 RetryConfig 应当有 ShouldRetry——那一层的判据只有配重试的人答得上")
	}

	// 共享的那半是**纯数据**：退避参数里不该有任何函数。
	bp := reflect.TypeOf(compose_v2.BackoffPolicy{})
	for i := 0; i < bp.NumField(); i++ {
		if f := bp.Field(i); f.Type.Kind() == reflect.Func {
			t.Errorf("BackoffPolicy 是被两层共享的纯数据参数，不该含函数字段 %s——"+
				"一旦含了，就又回到了「一个类型被部分消费」那个形状", f.Name)
		}
	}
}
