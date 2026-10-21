package agent_v2

import "crew-go/tool"

// frameworkMintedTool 是「这件工具是框架自己铸的」的**封闭标记**——接口方法未导出，业务实现不了。
//
// 框架自带工具豁免的那几道闸都据它判：审批、ToolFilter、渐进披露（绑定侧不延迟、派发侧不拦）、
// 工具中间件与工具超时、结果截断、工具账（不进账、续跑时重跑函数体）、重复调用检测。判的是
// **这次实际会被绑定 / 派发的那件实例**——工具表里这个名字下挂的那件，不是工具名。
//
// # 为什么不能按名字判
//
// 那些名字业务占得到：没配 [WithOutputSchema] 时 final_output 是普通业务工具，没配 [WithAskUser]
// 时 ask_user 也是；plan / respond 只是规划器自己的内部名，从不进 BasicAgent 的工具表。按名字判时，
// 业务工具恰好叫这些名字就绕过审批、兄弟工具中断续跑后再执行一次、跳过中间件与超时（均实测）。
//
// # 新加框架工具时
//
// 有自己类型的在类型上实现 isFrameworkMinted；FuncTool 做的经 [markMinted] 包一层。控制工具
// （transfer_to_agent / route_to）经 [markFrameworkTool]、结构化输出的 final_output 经
// frameworkFinalOutput，两者也都带这个标记。**绑定侧与派发侧各注册各的，两处都要带**——绑定侧
// 不带的话 ToolFilter / 渐进披露会把框架工具当业务工具裁掉。
type frameworkMintedTool interface{ isFrameworkMinted() }

// isFrameworkMinted 判断这件工具是不是框架铸的；nil（名字下没挂工具）返 false。
func isFrameworkMinted(t tool.Tool) bool {
	_, ok := t.(frameworkMintedTool)
	return ok
}

// isFrameworkMintedIn 判断工具表里这个名字下挂的是不是框架铸的那件。
func isFrameworkMintedIn(tk *tool.Toolkit, name string) bool {
	if tk == nil {
		return false
	}
	return isFrameworkMinted(tk.Get(name))
}

// mintedTool 给框架铸的、没有自己类型的工具（FuncTool 做的）打上标记。
type mintedTool struct{ tool.Tool }

func (mintedTool) isFrameworkMinted() {}

// markMinted 给框架铸的工具盖章。
func markMinted(t tool.Tool) tool.Tool { return mintedTool{Tool: t} }

func (*skillTool) isFrameworkMinted()           {}
func (*skillActionTool) isFrameworkMinted()     {}
func (*toolSearchTool) isFrameworkMinted()      {}
func (frameworkTool) isFrameworkMinted()        {}
func (frameworkFinalOutput) isFrameworkMinted() {}
