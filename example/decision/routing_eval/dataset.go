package main

import (
	"bufio"
	"bytes"
	"embed"
	"encoding/json"
	"fmt"
	"strings"
)

//go:embed cases.jsonl cases_hard.jsonl cases_real.jsonl
var caseFiles embed.FS

// tier 是一个样本子集。报告按子集分别出，三个子集的难度与形态不同，混在一起算
// 准确率会掩盖差别。
type tier struct {
	ID    string
	Title string
	File  string
	Note  string
}

var tiers = []tier{
	{ID: "standard", Title: "标准集", File: "cases.jsonl", Note: "单条消息，3 套 7 分支的粗粒度意图，中英对照"},
	{ID: "hard", Title: "困难集", File: "cases_hard.jsonl", Note: "单条消息，20 与 16 个相邻意图的细粒度划分，叠加方言、黑话、双重否定等，中英对照"},
	{ID: "real", Title: "真实会话集", File: "cases_real.jsonl", Note: "带会话元数据与多轮历史，含话题切换、语音转写、提示词注入与超范围请求，只有中文"},
}

func tierByID(id string) (tier, bool) {
	for _, t := range tiers {
		if t.ID == id {
			return t, true
		}
	}
	return tier{}, false
}

// route 是一个路由目标分支。Name 是 RoutingAgent 的分支名，两种语言的职责描述分别
// 喂给中文 / 英文条件下的 classifier 与 Jev。
type route struct {
	Name   string
	DescZH string
	DescEN string
}

// scenario 是一套路由目标。三套覆盖不同的业务面，避免结论只对一套标签成立。
type scenario struct {
	ID     string
	Title  string
	Routes []route
}

// scenarios 的分支描述按业务方写 WithRoutingDescriptions 的口吻写，互不重叠。
// 样本的 gold 以这里的描述为准判定。
var scenarios = []scenario{
	{
		ID:    "ecom",
		Title: "电商客服",
		Routes: []route{
			{"order_logistics", "订单与物流：查询订单状态、发货进度、快递物流轨迹，催发货、修改收货地址", "Orders and shipping: order status, dispatch progress, delivery tracking, rushing a shipment, changing the delivery address"},
			{"refund_return", "退货退款与售后：申请退货、退款、换货，退款进度，商品质量问题的售后处理", "Returns and after-sales: return, refund or exchange requests, refund progress, after-sales handling of defective items"},
			{"payment_invoice", "支付与发票：支付失败、重复扣款、优惠券和红包使用、开具或修改发票", "Payments and invoices: failed payments, double charges, coupons and vouchers, issuing or correcting invoices"},
			{"product_consult", "售前咨询：商品参数、尺码、材质、库存、使用方法、选购推荐", "Pre-sales questions: product specs, sizing, materials, stock, how to use, buying recommendations"},
			{"account_security", "账号与安全：登录注册、找回密码、换绑手机、账号被盗或异常登录、实名认证", "Account and security: login and sign-up, password recovery, changing the bound phone number, hacked accounts or suspicious logins, identity verification"},
			{"complaint_human", "投诉与人工：投诉客服或商家、要求转人工客服、对处理结果不满要求升级", "Complaints and human agents: complaints about support or the seller, requests for a human agent, escalating an unsatisfactory outcome"},
			{"other", "其他：与以上业务都无关的闲聊或问题", "Other: chitchat or questions unrelated to all of the above"},
		},
	},
	{
		ID:    "ops",
		Title: "运维助手",
		Routes: []route{
			{"metrics_query", "指标查询：查看监控指标的数值和趋势（CPU、内存、QPS、延迟、错误率等），编写或解释 PromQL", "Metrics: values and trends of monitoring metrics (CPU, memory, QPS, latency, error rate), writing or explaining PromQL"},
			{"alert_manage", "告警管理：查询当前或历史告警、静默或屏蔽告警、配置告警规则和通知订阅", "Alerts: current or past alerts, silencing or muting alerts, configuring alert rules and notification subscriptions"},
			{"log_search", "日志检索：按关键字、时间或 traceID 查询和分析应用与系统日志", "Logs: searching and analyzing application and system logs by keyword, time range or trace ID"},
			{"k8s_ops", "容器与发布：Kubernetes 上查看或重启 Pod、扩缩容、发布与回滚", "Containers and deployments: viewing or restarting pods on Kubernetes, scaling, rollouts and rollbacks"},
			{"incident", "故障应急：故障上报与定级、拉应急群、查值班人、撰写故障复盘", "Incidents: reporting and grading incidents, opening a war room, finding the on-call engineer, writing postmortems"},
			{"knowledge_qa", "知识问答：内部平台的接入文档、配置说明、规范与最佳实践", "Knowledge: onboarding docs, configuration guides, standards and best practices for internal platforms"},
			{"other", "其他：与运维无关的闲聊或问题", "Other: chitchat or questions unrelated to operations"},
		},
	},
	{
		ID:    "gen",
		Title: "通用助手",
		Routes: []route{
			{"weather", "天气：天气预报、气温、降雨、空气质量、穿衣建议", "Weather: forecasts, temperature, rain, air quality, what to wear"},
			{"travel", "出行：火车票、机票、酒店的查询和预订，路线导航与打车", "Travel: train tickets, flights and hotels, routes, navigation and ride-hailing"},
			{"translate", "翻译：把文本翻译成另一种语言", "Translation: translating text into another language"},
			{"calc", "计算：数学计算、单位换算、汇率换算", "Calculation: math, unit conversion, currency conversion"},
			{"media", "影音：播放音乐、电影、视频、有声书和广播", "Media: playing music, movies, videos, audiobooks and radio"},
			{"schedule", "日程：设置提醒、闹钟、倒计时和日程安排", "Schedule: reminders, alarms, timers and calendar events"},
			{"chat", "闲聊：日常闲聊、情感陪伴，以及不属于以上功能的开放问答", "Chat: casual conversation, emotional support, and open questions not covered above"},
		},
	},
	{
		ID:    "ecomf",
		Title: "电商客服细分",
		Routes: []route{
			{"track_package", "查物流：查询已发货包裹的物流轨迹和预计送达时间", "Tracking: delivery progress and estimated arrival of a shipped parcel"},
			{"urge_shipping", "催发货：已付款但商家迟迟未发货，要求尽快发出", "Rush shipping: paid but the seller hasn't shipped yet; asking to ship soon"},
			{"change_address", "改地址：修改收货地址、收件人或联系电话", "Change address: change the delivery address, recipient or contact phone"},
			{"package_issue", "包裹异常：显示已签收但没收到、包裹丢失、运输途中破损", "Parcel problem: marked delivered but not received, lost parcel, damaged in transit"},
			{"return_goods", "退货：已收到商品，申请把货退回并退款", "Return: item received; send it back for a refund"},
			{"exchange_goods", "换货：已收到商品，申请换尺码、颜色或换新", "Exchange: item received; swap for another size, color or a new unit"},
			{"refund_only", "仅退款：未发货就取消订单，或没收到货、不需要退回商品，只申请退款", "Refund only: cancel before shipping, or refund without returning anything (e.g. never received)"},
			{"refund_progress", "退款进度：已经申请过退款，查询审核与到账进度", "Refund status: refund already requested; asking about approval or when the money arrives"},
			{"price_protect", "价保补差：购买后降价，申请价格保护、退还差价", "Price protection: price dropped after purchase; claim the difference"},
			{"invoice_issue", "开发票：申请开具发票、下载电子发票、开专票", "Issue invoice: request an invoice, download an e-invoice, special VAT invoice"},
			{"invoice_fix", "改发票：已开发票的抬头、税号或金额有误，申请修改或重开", "Fix invoice: wrong title, tax ID or amount on an issued invoice; correct or reissue"},
			{"payment_fail", "支付失败：付款报错、支付页面卡住、银行卡付不了", "Payment failure: errors when paying, payment page stuck, card declined"},
			{"double_charge", "重复扣款：同一订单被扣多次，或已扣款但订单显示未支付", "Double charge: charged more than once, or charged but the order shows unpaid"},
			{"coupon", "优惠券：优惠券或红包领不到、用不了、没抵扣", "Coupons: can't claim or use a coupon or red envelope, discount not applied"},
			{"membership", "会员积分：会员等级与权益、积分查询与兑换", "Membership: tiers and perks, points balance and redemption"},
			{"login_issue", "登录问题：登录不上、忘记密码、收不到验证码", "Login problems: can't log in, forgot password, no verification code"},
			{"account_security", "账号安全：账号被盗、异常登录、更换绑定手机号、注销账号", "Account security: hacked account, suspicious logins, changing the bound phone, closing the account"},
			{"complain_seller", "投诉商家：发错货、虚假宣传、商家态度差，要求投诉商家", "Complain about seller: wrong item, false advertising, rude seller; filing a complaint"},
			{"complain_courier", "投诉快递员：快递员态度差、未经同意放驿站或快递柜、不送货上门", "Complain about courier: rude courier, left at a pickup point without consent, no door delivery"},
			{"human_agent", "转人工：明确要求人工客服或真人服务", "Human agent: explicitly asks for a human agent"},
			{"other", "其他：与以上业务都无关的问题或闲聊", "Other: questions or chitchat unrelated to all of the above"},
		},
	},
	{
		ID:    "opsf",
		Title: "运维助手细分",
		Routes: []route{
			{"cpu_mem", "资源用量：查询 CPU、内存、磁盘等资源使用率", "Resource usage: CPU, memory and disk utilization"},
			{"traffic_latency", "流量与延迟：查询 QPS、响应时间、错误率等服务指标", "Traffic and latency: QPS, response time, error rate and other service metrics"},
			{"promql_help", "PromQL：编写、解释或调试 PromQL 查询", "PromQL: writing, explaining or debugging PromQL queries"},
			{"alert_query", "告警查询：查看当前或历史告警及其详情", "Alert lookup: current or past alerts and their details"},
			{"alert_silence", "告警静默：临时屏蔽或静默告警", "Alert silencing: temporarily mute or silence alerts"},
			{"alert_rule", "告警规则：新增或修改告警规则、阈值和通知方式", "Alert rules: create or change alert rules, thresholds and notification channels"},
			{"log_search", "日志检索：按关键字、时间范围检索日志", "Log search: search logs by keyword or time range"},
			{"trace_query", "链路追踪：按 traceID 查调用链、定位慢调用", "Tracing: look up call chains by trace ID, locate slow calls"},
			{"pod_restart", "重启 Pod：重启或删除重建 Pod", "Pod restart: restart or delete-and-recreate pods"},
			{"scaling", "扩缩容：调整副本数或 HPA 配置", "Scaling: change replica counts or HPA settings"},
			{"deploy_rollback", "发布回滚：发布新版本、回滚到旧版本", "Deploy and rollback: release a new version or roll back"},
			{"incident_report", "故障上报：上报线上故障、定级、拉应急群", "Incident reporting: report a production incident, set severity, open a war room"},
			{"oncall", "值班：查询值班人、值班表、换班", "On-call: who is on call, schedules, shift swaps"},
			{"postmortem", "故障复盘：撰写或查阅故障复盘报告", "Postmortems: write or look up incident postmortem reports"},
			{"access_request", "权限申请：申请平台、集群或数据的访问权限", "Access requests: request access to platforms, clusters or data"},
			{"doc_qa", "文档问答：接入文档、使用说明、规范与最佳实践", "Docs Q&A: onboarding docs, usage guides, standards and best practices"},
			{"other", "其他：与运维无关的问题或闲聊", "Other: questions or chitchat unrelated to operations"},
		},
	},
}

// knownTags 是样本标签的全集。一条样本可以带多个标签，报告按标签分组统计准确率。
var knownTags = map[string]string{
	"direct":      "直述，含该分支的明显关键词",
	"paraphrase":  "转述，不含关键词",
	"colloquial":  "口语或网络用语",
	"implicit":    "隐含意图，需要推断",
	"negation":    "否定掉另一个分支的诉求",
	"multi":       "多诉求，按主诉求路由",
	"distractor":  "带大段无关背景",
	"typo":        "错别字、拼音或缩写",
	"mixed":       "中英混杂",
	"trad":        "繁体中文",
	"short":       "极短，四字以内",
	"trap":        "含指向其他分支的关键词",
	"fine":        "与相邻分支只差一点",
	"dialect":     "粤语等方言",
	"slang":       "网络用语或缩写黑话",
	"double_neg":  "双重否定",
	"context":     "要结合历史对话才能判断",
	"topic_shift": "历史对话在谈别的事，最新消息换了话题",
	"meta":        "要结合会话元数据才能判断",
	"asr":         "语音转写：断句乱、同音错字",
	"emoji":       "带表情或贴图占位",
	"inject":      "消息里夹带改变路由的指令",
	"oos":         "超出所有业务范围",
	"long":        "长篇大段，主诉求埋在后面",
}

// evalCase 是一条评测样本。Gold 是按分支职责描述判定的正确分支。
type evalCase struct {
	ID       string   `json:"id"`
	Scenario string   `json:"sc"`
	Gold     string   `json:"gold"`
	Tags     []string `json:"tags"`
	ZH       string   `json:"zh"`
	EN       string   `json:"en"`
	// Tier 由样本所在的文件决定，不在 JSON 里。
	Tier string `json:"-"`
}

// text 按语言取样本原文。
func (c evalCase) text(lang string) string {
	if lang == langEN {
		return c.EN
	}
	return c.ZH
}

// scenarioByID 按 ID 取场景；不存在返回 false。
func scenarioByID(id string) (scenario, bool) {
	for _, sc := range scenarios {
		if sc.ID == id {
			return sc, true
		}
	}
	return scenario{}, false
}

// hasRoute 判断场景里是否有这个分支。
func (sc scenario) hasRoute(name string) bool {
	for _, r := range sc.Routes {
		if r.Name == name {
			return true
		}
	}
	return false
}

// loadCases 解析三个内嵌样本文件并逐条校验：ID 全局唯一、场景存在、gold 是该场景
// 的分支、中文不为空、标签都在 knownTags 里。真实会话集只有中文，其余两个子集的
// 英文也必须有。
func loadCases() ([]evalCase, error) {
	cases := make([]evalCase, 0, 512)
	seen := make(map[string]bool, 512)
	for _, t := range tiers {
		raw, err := caseFiles.ReadFile(t.File)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", t.File, err)
		}
		sc := bufio.NewScanner(bytes.NewReader(raw))
		sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
		line := 0
		for sc.Scan() {
			line++
			text := strings.TrimSpace(sc.Text())
			if text == "" {
				continue
			}
			var c evalCase
			if err := json.Unmarshal([]byte(text), &c); err != nil {
				return nil, fmt.Errorf("%s line %d: %w", t.File, line, err)
			}
			c.Tier = t.ID
			if err := validateCase(c, seen); err != nil {
				return nil, fmt.Errorf("%s line %d: %w", t.File, line, err)
			}
			seen[c.ID] = true
			cases = append(cases, c)
		}
		if err := sc.Err(); err != nil {
			return nil, fmt.Errorf("scan %s: %w", t.File, err)
		}
	}
	if len(cases) == 0 {
		return nil, fmt.Errorf("no cases found")
	}
	return cases, nil
}

// validateCase 校验单条样本，seen 是已出现的 ID 集合。
func validateCase(c evalCase, seen map[string]bool) error {
	if c.ID == "" || seen[c.ID] {
		return fmt.Errorf("empty or duplicate id %q", c.ID)
	}
	s, ok := scenarioByID(c.Scenario)
	if !ok {
		return fmt.Errorf("case %s: unknown scenario %q", c.ID, c.Scenario)
	}
	if !s.hasRoute(c.Gold) {
		return fmt.Errorf("case %s: gold %q is not a route of scenario %q", c.ID, c.Gold, c.Scenario)
	}
	if strings.TrimSpace(c.ZH) == "" {
		return fmt.Errorf("case %s: zh must be non-empty", c.ID)
	}
	if c.Tier != "real" && strings.TrimSpace(c.EN) == "" {
		return fmt.Errorf("case %s: en must be non-empty outside the real tier", c.ID)
	}
	if len(c.Tags) == 0 {
		return fmt.Errorf("case %s: at least one tag is required", c.ID)
	}
	for _, t := range c.Tags {
		if _, ok := knownTags[t]; !ok {
			return fmt.Errorf("case %s: unknown tag %q", c.ID, t)
		}
	}
	return nil
}
