// Package decision 是决策模型的客户端，以及把它接到 RoutingAgent 上的路由决策方。
//
// 决策模型不生成文本，对一组带类型的问题直接给出判定。三种问题：choice 从给定选项里选
// 一个，返回各选项概率与置信度；score 在有序档位上打分；noul 回答是或否的概率。
//
// 线上格式有两种，表达的是同一套问题与答案，由 [Preset] 的 Protocol 选：TypeSafe 的
// System One（默认）与 OpenAI Decisions API 的格式，见 [Protocol]。[Request]、[Response]
// 与路由决策方只有一套，客户端收发时换格式。
//
// 用哪家的决策模型由调用方选，每家一个子包，构造出的都是 [Client]：
//
//   - TypeSafe 的 Jev：crew-go/decision/jev 的 jev.NewClient；
//   - 阿里云百炼的决策模型 decision-model-preview：crew-go/decision/bailian 的 bailian.NewClient。
//
// 各家的请求校验、重试与路由决策方都一样，不同的只是地址、模型名、API key、请求 ID
// 所在的响应头与线上格式。别的兼容服务用 [NewClient] 配一个 [Preset] 接入。
//
// 路由用法见 [NewRouteDecider]；直接调用见 [Client.Decide]。
//
// 重试沿用 TypeSafe 官方 Python SDK 的默认策略：HTTP 408、429、5xx 与网络层失败（含单次
// 请求超时）重试，最多 2 次；等待时长优先按服务端的 retry-after-ms 与 retry-after 响应头，
// 否则指数退避；单次请求超时 10 秒；一次调用总预算 30 秒，下一次等待会让累计耗时达到
// 预算时不再重试。
package decision

import (
	"bytes"
	"context"
	"encoding"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/rand/v2"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"time"

	"crew-go/compose_v2"
	"crew-go/model"
)

const (
	systemOnePath = "/v1/systemone"
	decisionsPath = "/v1/decisions"

	// 以下默认值对齐官方 Python SDK。
	defaultAttemptTimeout = 10 * time.Second
	defaultMaxRetries     = 2
	defaultBackoffInitial = 500 * time.Millisecond
	defaultBackoffMax     = 5 * time.Second
	defaultRetryBudget    = 30 * time.Second
	// backoffJitter 是每次退避随机减去的最大比例。
	backoffJitter = 0.25
	// maxErrorBody 截断错误响应体的长度。
	maxErrorBody = 200

	// maxResponseBody 是读取响应体的上限，防止异常响应占满内存。
	maxResponseBody = 1 << 20
)

// QuestionType 是问题类型。
type QuestionType string

const (
	// TypeChoice 从给定选项里选一个。
	TypeChoice QuestionType = "choice"
	// TypeScore 在有序档位上打分。
	TypeScore QuestionType = "score"
	// TypeNoul 回答是或否的概率。
	TypeNoul QuestionType = "noul"
)

// Protocol 是客户端发请求用的线上格式。两种格式表达的是同一套问题与答案，[Request] 与
// [Response] 不随格式变。
type Protocol int

const (
	// ProtocolSystemOne 是 TypeSafe 的 System One 格式：请求发到 /v1/systemone，questions 是以问题
	// 名作键的对象，答案按同一个键返回。[Preset] 不填 Protocol 时用它。
	ProtocolSystemOne Protocol = iota
	// ProtocolOpenAIDecisions 是 OpenAI Decisions API 的格式：请求发到 /v1/decisions，待判断的内容放在
	// input 里，questions 是带 name 的数组（是非题在这个格式里叫 predicate，选项与档位也是数组），答案是
	// 按请求顺序排的数组，可以对单个问题拒答（见 [Answer] 的 Refused）。
	//
	// 客户端发请求前把 [Request] 换成这个格式：State 是对象或数组时编成紧凑的 JSON 文本作 input（< > &
	// 原样保留，不转义）；问题按名字排序；Instructions 为空时用问题名代替；是非题的 criteria 拼在
	// instructions 后面（True: …、False: …）；选项说明与档位描述不是文本时同样编成 JSON 文本。答案按
	// name 放回 [Response] 的 Answers，predicate 的概率填进 Noul，打分题的档位编号与 System One 一样从
	// "0" 起。
	//
	// 所以同一个 [Request] 换了格式，模型读到的文本不一定相同。State 与选项、档位说明都是文本、是非题
	// 不带 criteria 时相同，两种格式的答案逐项一致（同一个百炼决策模型实测）。State 或说明是对象、
	// 是非题带了 criteria 时不同：System One 下由服务端把它们转成文本，各家转法不同（百炼把对象转成
	// 「键: 值」的文本；TypeSafe 开源的 system-one-adapter 用普通 LLM 实现 System One，转成 JSON），这里
	// 转成 JSON 文本、是非题的说明按上面的方式拼，概率与置信度会有出入——实测同一个对象形 State，百炼经
	// System One 置信度 0.51、经这个格式 0.43。
	// 置信度门槛按实际使用的格式实测来定。
	ProtocolOpenAIDecisions
)

// Question 是一个问题。常见形状用 [Choice]、[Score]、[Noul] 构造。
type Question struct {
	Type QuestionType `json:"type"`
	// Instructions 是问题本身，可以不填。
	Instructions string `json:"instructions,omitempty"`
	// Criteria 的形状随类型不同，按编成 JSON 之后的形状校验：choice 是非空对象，键是
	// 非空的选项名，值是描述（文本、对象、数组或 null，null 表示只看选项名）；score 是
	// 非空数组，从低到高描述每一档，每档是文本、对象或数组；noul 可以不填或为 null，填时
	// 是对象，只认 true、false 两个键，分别描述是与否。
	//
	// 描述里的 []byte 会被 encoding/json 编成 base64 发出去，这里查不到，要自己转换。
	Criteria any `json:"criteria,omitempty"`
}

// Choice 构造 choice 问题：options 是「选项名 → 选项描述」。
func Choice(instructions string, options map[string]string) Question {
	return Question{Type: TypeChoice, Instructions: instructions, Criteria: options}
}

// Score 构造 score 问题：levels 按从低到高的顺序描述每一档。
func Score(instructions string, levels []string) Question {
	return Question{Type: TypeScore, Instructions: instructions, Criteria: levels}
}

// Noul 构造是非题。
func Noul(instructions string) Question {
	return Question{Type: TypeNoul, Instructions: instructions}
}

// Request 是一次调用：对同一份 State 并行回答全部 Questions。
type Request struct {
	// Model 为空时用客户端的默认模型。
	Model string `json:"model"`
	// State 是待判断的内容，编成 JSON 后须是字符串、对象或数组；数字、布尔值与
	// null 不接受。直接传 []byte（或指向它的指针）也不接受，encoding/json 会把它编成
	// base64：是 JSON 就包成 json.RawMessage，是文本就转成 string。嵌在对象或数组里的
	// []byte 同样会被编成 base64，这里查不到，要自己转换。
	State any `json:"state"`
	// Questions 的键是调用方起的名字，响应的答案按同一个键返回。
	Questions map[string]Question `json:"questions"`
}

// Answer 是一个问题的答案。哪些字段有值取决于问题类型：choice 填 Choice、
// Probabilities、Confidence；score 填 Score、Legend、Probabilities、Confidence；
// noul 只填 Noul。服务端拒答时只有 Type 与 Refused。
type Answer struct {
	Type          QuestionType       `json:"type"`
	Choice        string             `json:"choice,omitempty"`
	Score         *float64           `json:"score,omitempty"`
	Noul          *float64           `json:"noul,omitempty"`
	Confidence    *float64           `json:"confidence,omitempty"`
	Probabilities map[string]float64 `json:"probabilities,omitempty"`
	// Legend 按档位编号（"0"、"1"……）回显请求里 score 的 criteria，值与当时给的
	// 一样是文本、对象或数组。
	Legend map[string]any `json:"legend,omitempty"`
	// Refused 为 true 表示服务端拒绝回答这一问，只有 [ProtocolOpenAIDecisions] 会这样；这时 Type
	// 是请求里这一问的类型，其余字段都为空。
	Refused bool `json:"refused,omitempty"`
}

// Usage 是本次调用的 token 用量。TypeSafe 只按输入 token 计费；百炼的响应只带输入
// token，OutputTokens 为 0。
type Usage struct {
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
}

// Response 是一次调用的响应。Model 是服务端回报的模型：TypeSafe 回报实际服务的版本，
// 如 jev-1.13.0；百炼回显请求里的模型名。
//
// Answers 只含本包认得的三种答案类型。服务端以后新增的类型会被跳过并记一条 warn 日志，
// 其余答案照常返回，与官方 SDK 一致；所以按问题名取答案时要检查 ok。每个答案只解它
// 那一类的字段，别的字段忽略——Go 的字段匹配不区分大小写，所以只差大小写的多余字段仍会
// 参与解码，形状不对时这份响应会被判为解析失败。
type Response struct {
	Model   string            `json:"model"`
	Answers map[string]Answer `json:"answers"`
	Usage   Usage             `json:"usage"`
	// RequestID 是服务端的请求 ID，找服务方排查问题时用；响应没带时为空。
	RequestID string `json:"-"`
}

// Preset 描述一家提供决策模型接口的服务方，[NewClient] 用它构造客户端。接 Jev 与百炼时
// 直接用各自子包的构造函数，不用自己填；接别的兼容服务时才需要。
type Preset struct {
	// Provider 是服务方标识，如 "typesafe"、"bailian"：填进 model.APIError.Provider，也出现在
	// 带请求 ID 的错误文本里。不能为空。
	Provider string
	// BaseURL 是 API 地址，请求发到 BaseURL 加 /v1/systemone（[ProtocolOpenAIDecisions] 是
	// /v1/decisions）。要求同 [WithBaseURL]。
	BaseURL string
	// DefaultModel 是请求没指定模型时用的模型。不能为空。
	DefaultModel string
	// RequestIDHeader 是服务端请求 ID 所在的响应头。为空表示服务端不给请求 ID，
	// [Response] 的 RequestID 恒为空。
	RequestIDHeader string
	// Protocol 是线上格式，零值是 [ProtocolSystemOne]。
	Protocol Protocol
}

// Client 是决策模型 API 的客户端，可并发使用。
type Client struct {
	apiKey  string
	baseURL string
	model   string
	// provider、requestIDHeader 与 protocol 取自 Preset，不随 WithBaseURL 变。
	provider        string
	requestIDHeader string
	protocol        Protocol
	hc              *http.Client
	attemptTimeout  time.Duration
	maxRetries      int
	backoffInitial  time.Duration
	backoffMax      time.Duration
	retryBudget     time.Duration
}

// Option 配置 [Client]。
type Option func(*Client)

// WithBaseURL 替换 API 地址，常用于经网关转发或测试。地址须是带 http 或 https 的
// 绝对地址，可以带路径前缀，不能带查询串或片段（请求路径是直接拼在后面的）。
func WithBaseURL(u string) Option {
	base := normalizeBaseURL("decision.WithBaseURL", u)
	return func(c *Client) { c.baseURL = base }
}

// normalizeBaseURL 去掉首尾空白与结尾的斜杠。不是带 http 或 https 的绝对地址、带查询串
// 或片段时 panic，fn 是 panic 信息里的函数名。
func normalizeBaseURL(fn, u string) string {
	base := strings.TrimRight(strings.TrimSpace(u), "/")
	parsed, err := url.Parse(base)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" ||
		strings.ContainsAny(base, "?#") {
		panic(fmt.Sprintf("%s: %q is not an absolute http(s) URL without query or fragment", fn, u))
	}
	return base
}

// WithModel 替换默认模型，如固定到某个版本 jev-1.13.0。
func WithModel(name string) Option {
	name = strings.TrimSpace(name)
	if name == "" {
		panic("decision.WithModel: empty model name")
	}
	return func(c *Client) { c.model = name }
}

// WithHTTPClient 替换底层 HTTP 客户端。单次请求的超时由 [WithAttemptTimeout] 管，
// 不要在这里设 http.Client.Timeout。
func WithHTTPClient(hc *http.Client) Option {
	if hc == nil {
		panic("decision.WithHTTPClient: nil http client")
	}
	return func(c *Client) { c.hc = hc }
}

// WithAttemptTimeout 设单次 HTTP 请求的超时，默认 10 秒。重试的每一次各自计时。
func WithAttemptTimeout(d time.Duration) Option {
	if d <= 0 {
		panic(fmt.Sprintf("decision.WithAttemptTimeout: timeout must be positive, got %v", d))
	}
	return func(c *Client) { c.attemptTimeout = d }
}

// WithMaxRetries 设首次请求之外最多重试几次，默认 2，0 表示不重试。
func WithMaxRetries(n int) Option {
	if n < 0 {
		panic(fmt.Sprintf("decision.WithMaxRetries: retries must not be negative, got %d", n))
	}
	return func(c *Client) { c.maxRetries = n }
}

// WithBackoff 设指数退避的首次等待与上限，默认 500 毫秒与 5 秒。第 k 次重试（从 0 计）
// 前等首次等待的 2^k 倍、不超过上限，再随机减去至多四分之一。服务端给了等待时长时
// 按服务端的。设 0 表示不退避。
func WithBackoff(initial, max time.Duration) Option {
	if initial < 0 || max < 0 || initial > max {
		panic(fmt.Sprintf("decision.WithBackoff: need 0 <= initial <= max, got initial %v, max %v", initial, max))
	}
	return func(c *Client) { c.backoffInitial, c.backoffMax = initial, max }
}

// WithRetryBudget 设一次 [Client.Decide] 调用的总时长预算，含首次请求与各次等待，
// 默认 30 秒。下一次等待会让累计耗时达到预算时不再重试，返回上一次的错误。
//
// 预算只决定还要不要再试，不打断正在进行的请求，所以最坏耗时约为预算加一次单次
// 超时。要硬上限就给传进来的 ctx 设 deadline。设 0 表示不限，这时服务端要求等多久
// 就等多久，只靠 ctx 兜底。
func WithRetryBudget(d time.Duration) Option {
	if d < 0 {
		panic(fmt.Sprintf("decision.WithRetryBudget: budget must not be negative, got %v", d))
	}
	return func(c *Client) { c.retryBudget = d }
}

// NewClient 用 API key 与服务方描述构造客户端。接 Jev 与百炼时用各自子包的构造函数
// jev.NewClient、bailian.NewClient，它们填好了 Preset。
//
// key 前后的空白会去掉；去掉后为空或含控制字符时 panic：每个请求都会被拒，早报比在调用点
// 才发现好。p 的 Provider 或 DefaultModel 为空、BaseURL 不合法、Protocol 不是已知的格式时同样 panic。opts 在 p 之后
// 应用，能覆盖 p 给的地址与模型；服务方标识与请求 ID 的响应头不随地址变。
func NewClient(apiKey string, p Preset, opts ...Option) *Client {
	key := strings.TrimSpace(apiKey)
	if key == "" {
		panic("decision.NewClient: empty api key")
	}
	if strings.ContainsFunc(key, func(r rune) bool { return r < 0x20 || r == 0x7f }) {
		panic("decision.NewClient: api key contains control characters")
	}
	provider := strings.TrimSpace(p.Provider)
	if provider == "" {
		panic("decision.NewClient: preset has an empty provider")
	}
	defaultModel := strings.TrimSpace(p.DefaultModel)
	if defaultModel == "" {
		panic("decision.NewClient: preset has an empty default model")
	}
	if p.Protocol != ProtocolSystemOne && p.Protocol != ProtocolOpenAIDecisions {
		panic(fmt.Sprintf("decision.NewClient: preset has an unknown protocol %d", p.Protocol))
	}
	c := &Client{
		apiKey:          key,
		baseURL:         normalizeBaseURL("decision.NewClient", p.BaseURL),
		model:           defaultModel,
		provider:        provider,
		requestIDHeader: strings.TrimSpace(p.RequestIDHeader),
		protocol:        p.Protocol,
		hc:              &http.Client{},
		attemptTimeout:  defaultAttemptTimeout,
		maxRetries:      defaultMaxRetries,
		backoffInitial:  defaultBackoffInitial,
		backoffMax:      defaultBackoffMax,
		retryBudget:     defaultRetryBudget,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Decide 发一次调用，线上格式按客户端的 [Protocol]。req.Model 为空时用客户端的默认模型。
//
// 非 2xx 响应返回包着 *model.APIError 的错误，可以用 errors.As 取状态码、响应体和
// 服务端要求的等待时长。请求本身不合法、响应体解析失败或缺必填字段、响应超过
// 1 MiB 时返回的错误都不重试，并实现 compose_v2.NonRetryable，外层编排按默认判据也不会重试。
// 服务端相关的错误文本都带服务端的请求 ID。
//
// 下一次重试前的等待（服务端要求的或指数退避）不短于 ctx 剩余时间时不再等，直接返回
// 上一次的错误，调用方可以用剩下的时间走别的路，比如 RoutingAgent 升级到下一档。等完之后
// 的那次请求仍可能撞上 deadline；要保证超时后能走别的路，给这次调用派生一个带超时的 ctx，
// 见 [NewRouteDecider]。
func (c *Client) Decide(ctx context.Context, req *Request) (*Response, error) {
	if err := validateRequest(req); err != nil {
		return nil, err
	}
	wire := *req
	if wire.Model == "" {
		wire.Model = c.model
	}
	body, err := c.encode(wire)
	if err != nil {
		return nil, invalidRequest("decision: marshal request: %w", err)
	}
	start := time.Now()
	for retry := 0; ; retry++ {
		resp, err := c.post(ctx, body, wire.Questions)
		if err == nil {
			return resp, nil
		}
		// 调用方的 ctx 已结束时不再重试。这里看 ctx 本身，不看错误链：单次请求超时
		// 也会带着 context.DeadlineExceeded，那一种要重试。
		if ctx.Err() != nil || retry >= c.maxRetries || !retryable(err) {
			return nil, err
		}
		delay := c.retryDelay(retry, err)
		// 用「剩余预算」比，不用「已耗时间 + 等待」：等待时长可能饱和到 time.Duration
		// 的最大值，相加会溢出成负数，预算就失效了。
		if c.retryBudget > 0 && delay >= c.retryBudget-time.Since(start) {
			return nil, err
		}
		// 等待本身就放不下时不睡，把剩下的时间留给调用方。下一次请求要多久估不准：失败的
		// 那次常是网关秒回的 429，成功的请求却要几百毫秒，按上一次的耗时估两个方向都会错。
		if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) <= delay {
			return nil, err
		}
		if werr := compose_v2.SleepWithContext(ctx, delay); werr != nil {
			return nil, fmt.Errorf("decision: stopped waiting to retry after %w: %w", err, werr)
		}
	}
}

// retryable 对齐官方 SDK：HTTP 408、429、5xx 重试；没拿到 HTTP 响应的失败（连接失败、
// 单次请求超时、读响应体中断）重试。其余错误重试必然一样。
func retryable(err error) bool {
	var ae *model.APIError
	if !errors.As(err, &ae) {
		return false
	}
	switch s := ae.StatusCode; {
	case s == 0:
		return true
	case s == http.StatusRequestTimeout, s == http.StatusTooManyRequests:
		return true
	default:
		return s >= 500 && s < 600
	}
}

// retryDelay 是第 retry 次重试（从 0 计）前的等待：服务端给了等待时长就按它的，
// 否则指数退避。服务端要求的时长不设上限，太长时由重试预算让调用直接返回。
func (c *Client) retryDelay(retry int, err error) time.Duration {
	if d := compose_v2.RetryAfterFromError(err); d > 0 {
		return d
	}
	return c.backoff(retry)
}

// backoff 是第 retry 次重试（从 0 计）前的指数退避。
func (c *Client) backoff(retry int) time.Duration {
	if c.backoffInitial <= 0 || c.backoffMax <= 0 {
		return 0
	}
	d := c.backoffInitial
	for i := 0; i < retry && d < c.backoffMax; i++ {
		if d > c.backoffMax/2 {
			d = c.backoffMax
			break
		}
		d *= 2
	}
	return time.Duration(float64(d) * (1 - rand.Float64()*backoffJitter))
}

// validateRequest 在发请求之前拦下形状不对的请求。形状按编成 JSON 之后判断，不限定
// Go 类型。规则取自服务端的 OpenAPI schema 与官方 SDK 的校验；选项名不能为空、noul 只认
// true 与 false 两个键这两条比 schema 严，前者是因为答案里的空选项名与「没选」分不开。
func validateRequest(req *Request) error {
	if req == nil {
		return invalidRequest("decision: nil request")
	}
	if isRawBytes(req.State) {
		return invalidRequest("decision: state is a byte slice, which encoding/json sends as base64; " +
			"pass a string, or json.RawMessage if the bytes are JSON")
	}
	state, err := json.Marshal(req.State)
	if err != nil {
		return invalidRequest("decision: encode state: %w", err)
	}
	if !isContent(state) {
		return invalidRequest("decision: state must encode to a JSON string, object or array")
	}
	if len(req.Questions) == 0 {
		return invalidRequest("decision: at least one question is required")
	}
	for name, q := range req.Questions {
		// 服务端要求问题名非空：System One 格式下回 400，OpenAI Decisions 格式下照常作答、计费，答案的
		// name 却是空串，这次响应只能丢掉（2026-10-16 实测）。
		if name == "" {
			return invalidRequest("decision: question name must not be empty")
		}
		if err := validateQuestion(name, q); err != nil {
			return err
		}
	}
	return nil
}

func validateQuestion(name string, q Question) error {
	if q.Type != TypeChoice && q.Type != TypeScore && q.Type != TypeNoul {
		return invalidRequest("decision: question %q has unknown type %q", name, q.Type)
	}
	raw, err := json.Marshal(q.Criteria)
	if err != nil {
		return invalidRequest("decision: encode criteria of question %q: %w", name, err)
	}
	switch q.Type {
	case TypeChoice:
		var opts map[string]json.RawMessage
		if !isObject(raw) || json.Unmarshal(raw, &opts) != nil || len(opts) == 0 {
			return invalidRequest("decision: choice question %q needs criteria that encode to a non-empty JSON object of options", name)
		}
		for opt, desc := range opts {
			if opt == "" {
				return invalidRequest("decision: choice question %q has an empty option name", name)
			}
			if !isContent(desc) && !isNull(desc) {
				return invalidRequest("decision: choice question %q option %q: description must be text, an object, an array or null", name, opt)
			}
		}
	case TypeScore:
		var levels []json.RawMessage
		if !isArray(raw) || json.Unmarshal(raw, &levels) != nil || len(levels) == 0 {
			return invalidRequest("decision: score question %q needs criteria that encode to a non-empty JSON array of levels", name)
		}
		for i, level := range levels {
			if !isContent(level) {
				return invalidRequest("decision: score question %q level %d must be text, an object or an array", name, i)
			}
		}
	case TypeNoul:
		if isNull(raw) {
			return nil
		}
		var outcomes map[string]json.RawMessage
		if !isObject(raw) || json.Unmarshal(raw, &outcomes) != nil {
			return invalidRequest("decision: noul question %q criteria must encode to a JSON object or null", name)
		}
		for key, desc := range outcomes {
			if key != "true" && key != "false" {
				return invalidRequest("decision: noul question %q criteria has key %q; only \"true\" and \"false\" are allowed", name, key)
			}
			if !isContent(desc) && !isNull(desc) {
				return invalidRequest("decision: noul question %q criteria %q must be text, an object, an array or null", name, key)
			}
		}
	}
	return nil
}

// isContent 报告一段 JSON 是不是文本、对象或数组，即服务端 schema 里的 JSON 内容。
func isContent(raw []byte) bool {
	raw = bytes.TrimSpace(raw)
	return len(raw) > 0 && (raw[0] == '"' || raw[0] == '{' || raw[0] == '[')
}

func isObject(raw []byte) bool {
	raw = bytes.TrimSpace(raw)
	return len(raw) > 0 && raw[0] == '{'
}

func isArray(raw []byte) bool {
	raw = bytes.TrimSpace(raw)
	return len(raw) > 0 && raw[0] == '['
}

func isNull(raw []byte) bool {
	return bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}

// isRawBytes 报告 v 是不是会被 encoding/json 编成 base64 的字节切片，包括经指针或接口
// 拿到的。判据照 encoding/json：切片类型自带编码方式（Marshaler、TextMarshaler，比如
// json.RawMessage、net.IP）时不算；方法定义在指针接收者上的，只有值可寻址（经指针拿到）
// 时才算数；元素类型的指针自带编码方式时整个切片编成数组，也不算。
func isRawBytes(v any) bool {
	rv := reflect.ValueOf(v)
	// 记下走过的指针：`type P *P` 这类自引用指针会让解引用永远走不完。encoding/json 同样
	// 靠查环收场（它报「encountered a cycle」），那种值编不出 base64，返 false 即可。
	//
	// 走到底而不是走几层就收手，是因为中间层不可能自带编码方式：Go 不许在指针类型上定义
	// 方法（无名 `*T` 没有方法集，具名指针类型上写方法编译不过），所以 encoding/json 只
	// 可能在**底层类型**上遇到 MarshalJSON / MarshalText 而收手，那正是下面判的地方。
	// 层数封顶反而会分叉：超过上限的链上字节切片被当成普通值放行、以 base64 发出去。
	seen := map[uintptr]bool{}
	for rv.IsValid() && (rv.Kind() == reflect.Pointer || rv.Kind() == reflect.Interface) {
		if rv.IsNil() {
			return false
		}
		if rv.Kind() == reflect.Pointer {
			if p := rv.Pointer(); seen[p] {
				return false
			} else {
				seen[p] = true
			}
		}
		rv = rv.Elem()
	}
	if !rv.IsValid() {
		return false
	}
	t := rv.Type()
	if t.Kind() != reflect.Slice || t.Elem().Kind() != reflect.Uint8 {
		return false
	}
	if marshalsItself(t) || (rv.CanAddr() && marshalsItself(reflect.PointerTo(t))) {
		return false
	}
	return !marshalsItself(reflect.PointerTo(t.Elem()))
}

var (
	jsonMarshalerType = reflect.TypeFor[json.Marshaler]()
	textMarshalerType = reflect.TypeFor[encoding.TextMarshaler]()
)

// marshalsItself 报告 t 是否自带 JSON 或文本编码方式。
func marshalsItself(t reflect.Type) bool {
	return t.Implements(jsonMarshalerType) || t.Implements(textMarshalerType)
}

// invalidRequestError 是发请求之前就能判定的错误，重试拿到的会是一样的结果。
type invalidRequestError struct{ err error }

func invalidRequest(format string, args ...any) error {
	return &invalidRequestError{err: fmt.Errorf(format, args...)}
}

func (e *invalidRequestError) Error() string { return e.err.Error() }

func (e *invalidRequestError) Unwrap() error { return e.err }

// NonRetryable 实现 compose_v2.NonRetryable。
func (e *invalidRequestError) NonRetryable() {}

// encode 把已校验过的请求编成客户端用的线上格式。
func (c *Client) encode(req Request) ([]byte, error) {
	if c.protocol == ProtocolOpenAIDecisions {
		return encodeDecisionsRequest(req)
	}
	return json.Marshal(req)
}

// post 发一次请求，不含重试。questions 是这次请求的问题，按 OpenAI Decisions 格式解析答案时用。
func (c *Client) post(ctx context.Context, body []byte, questions map[string]Question) (*Response, error) {
	actx, cancel := context.WithTimeout(ctx, c.attemptTimeout)
	defer cancel()
	path := systemOnePath
	if c.protocol == ProtocolOpenAIDecisions {
		path = decisionsPath
	}
	httpReq, err := http.NewRequestWithContext(actx, http.MethodPost, c.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("decision: build request: %w", err)
	}
	httpReq.Header.Set("Authorization", "Bearer "+c.apiKey)
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")
	resp, err := c.hc.Do(httpReq)
	if err != nil {
		return nil, &model.APIError{Provider: c.provider, Err: fmt.Errorf("request failed: %w", err)}
	}
	// 只读响应体，Close 的错误不影响已读到的数据。
	defer resp.Body.Close() //nolint:errcheck
	requestID := resp.Header.Get(c.requestIDHeader)
	// 多读一个字节，才分得清「正好到上限」和「超过上限」。
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBody+1))
	if err != nil {
		return nil, withRequestID(c.provider, requestID, &model.APIError{Provider: c.provider, Err: fmt.Errorf("read response: %w", err)})
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, withRequestID(c.provider, requestID, &model.APIError{
			Provider:   c.provider,
			StatusCode: resp.StatusCode,
			Body:       truncateRunes(string(raw), maxErrorBody),
			RetryAfter: parseRetryAfter(resp.Header, time.Now()),
		})
	}
	if len(raw) > maxResponseBody {
		return nil, &responseError{provider: c.provider, requestID: requestID, err: fmt.Errorf("response body exceeds %d bytes", maxResponseBody)}
	}
	var out *Response
	if c.protocol == ProtocolOpenAIDecisions {
		out, err = decodeDecisionsResponse(ctx, raw, requestID, questions)
	} else {
		out, err = decodeResponse(ctx, raw, requestID)
	}
	if err != nil {
		return nil, &responseError{provider: c.provider, requestID: requestID, err: err}
	}
	out.RequestID = requestID
	return out, nil
}

// decodeResponse 按官方 SDK 的规则解析响应：须是 JSON 对象，model 与 usage 必须有；
// answers 可以没有，有的话每个答案须是带 type 的对象。认不得的答案类型跳过并记日志，
// 认得的按其类型检查必填字段。缺字段的响应当成解析失败，不当成功返回。
func decodeResponse(ctx context.Context, raw []byte, requestID string) (*Response, error) {
	var top struct {
		Model   *string         `json:"model"`
		Usage   *Usage          `json:"usage"`
		Answers json.RawMessage `json:"answers"`
	}
	if err := json.Unmarshal(raw, &top); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	switch {
	case top.Model == nil:
		return nil, errors.New("invalid response: missing model")
	case top.Usage == nil:
		return nil, errors.New("invalid response: missing usage")
	case isNull(top.Answers):
		return nil, errors.New("invalid response: answers is null")
	}
	out := &Response{Model: *top.Model, Usage: *top.Usage, Answers: map[string]Answer{}}
	if len(top.Answers) == 0 {
		return out, nil
	}
	var rawAnswers map[string]json.RawMessage
	if err := json.Unmarshal(top.Answers, &rawAnswers); err != nil {
		return nil, fmt.Errorf("decode response answers: %w", err)
	}
	for name, ra := range rawAnswers {
		var probe struct {
			Type *string `json:"type"`
		}
		if err := json.Unmarshal(ra, &probe); err != nil || probe.Type == nil {
			return nil, fmt.Errorf("invalid response: answer %q has no type", name)
		}
		switch QuestionType(*probe.Type) {
		case TypeChoice, TypeScore, TypeNoul:
		default:
			slog.WarnContext(ctx, "decision: ignoring answer with unrecognized type",
				"answer", name, "type", *probe.Type, "request_id", requestID)
			continue
		}
		a, err := decodeAnswer(name, QuestionType(*probe.Type), ra)
		if err != nil {
			return nil, err
		}
		out.Answers[name] = a
	}
	return out, nil
}

// decodeAnswer 按答案类型只解这一类的字段，别的字段忽略，与官方 SDK 一致：别的类型的
// 同名字段形状不同（比如 noul 答案顺带一个对象形的 confidence），混在一个结构体里解会
// 让整个已计费的响应作废。必填字段缺失、概率为 null、档位描述不是文本、对象或数组时报错。
func decodeAnswer(name string, t QuestionType, raw json.RawMessage) (Answer, error) {
	a := Answer{Type: t}
	missing := func(field string) error {
		return fmt.Errorf("invalid response: %s answer %q is missing %s", t, name, field)
	}
	switch t {
	case TypeChoice:
		var c struct {
			Choice        *string             `json:"choice"`
			Confidence    *float64            `json:"confidence"`
			Probabilities map[string]*float64 `json:"probabilities"`
		}
		if err := json.Unmarshal(raw, &c); err != nil {
			return a, fmt.Errorf("decode answer %q: %w", name, err)
		}
		switch {
		case c.Choice == nil || *c.Choice == "":
			return a, missing("choice")
		case c.Confidence == nil:
			return a, missing("confidence")
		case c.Probabilities == nil:
			return a, missing("probabilities")
		}
		probs, err := probabilitiesOf(name, t, c.Probabilities)
		if err != nil {
			return a, err
		}
		a.Choice, a.Confidence, a.Probabilities = *c.Choice, c.Confidence, probs
	case TypeScore:
		var s struct {
			Score         *float64                   `json:"score"`
			Confidence    *float64                   `json:"confidence"`
			Legend        map[string]json.RawMessage `json:"legend"`
			Probabilities map[string]*float64        `json:"probabilities"`
		}
		if err := json.Unmarshal(raw, &s); err != nil {
			return a, fmt.Errorf("decode answer %q: %w", name, err)
		}
		switch {
		case s.Score == nil:
			return a, missing("score")
		case s.Confidence == nil:
			return a, missing("confidence")
		case s.Legend == nil:
			return a, missing("legend")
		case s.Probabilities == nil:
			return a, missing("probabilities")
		}
		legend := make(map[string]any, len(s.Legend))
		for level, desc := range s.Legend {
			if !isContent(desc) {
				return a, fmt.Errorf("invalid response: score answer %q legend %q must be text, an object or an array", name, level)
			}
			var v any
			if err := json.Unmarshal(desc, &v); err != nil {
				return a, fmt.Errorf("decode answer %q legend %q: %w", name, level, err)
			}
			legend[level] = v
		}
		probs, err := probabilitiesOf(name, t, s.Probabilities)
		if err != nil {
			return a, err
		}
		a.Score, a.Confidence, a.Legend, a.Probabilities = s.Score, s.Confidence, legend, probs
	case TypeNoul:
		var n struct {
			Noul *float64 `json:"noul"`
		}
		if err := json.Unmarshal(raw, &n); err != nil {
			return a, fmt.Errorf("decode answer %q: %w", name, err)
		}
		if n.Noul == nil {
			return a, missing("noul")
		}
		a.Noul = n.Noul
	}
	return a, nil
}

// probabilitiesOf 把概率表里的值取出来；有 null 值时报错，不当成 0。
func probabilitiesOf(name string, t QuestionType, in map[string]*float64) (map[string]float64, error) {
	out := make(map[string]float64, len(in))
	for k, v := range in {
		if v == nil {
			return nil, fmt.Errorf("invalid response: %s answer %q has a null probability for %q", t, name, k)
		}
		out[k] = *v
	}
	return out, nil
}

// withRequestID 在错误文本前加上服务方标识与服务端请求 ID，ID 为空时原样返回。
func withRequestID(provider, id string, err error) error {
	if id == "" {
		return err
	}
	return fmt.Errorf("%s request %s: %w", provider, id, err)
}

// responseError 表示服务端回了成功状态，但响应体用不了：不是 JSON 对象、缺必填
// 字段，或超过大小上限。重试拿到的会是一样的结果。
type responseError struct {
	provider  string
	requestID string
	err       error
}

func (e *responseError) Error() string {
	if e.requestID != "" {
		return fmt.Sprintf("decision: %s request %s: %v", e.provider, e.requestID, e.err)
	}
	return "decision: " + e.err.Error()
}

func (e *responseError) Unwrap() error { return e.err }

// NonRetryable 实现 compose_v2.NonRetryable。
func (e *responseError) NonRetryable() {}

// parseRetryAfter 按官方 SDK 的顺序解析等待时长：先 retry-after-ms（毫秒），再
// retry-after（秒数或 HTTP-date）。都没有、不可解析或已过期时返回 0；超出
// time.Duration 能表示的范围时取最大值。
func parseRetryAfter(h http.Header, now time.Time) time.Duration {
	if v := strings.TrimSpace(h.Get("retry-after-ms")); v != "" {
		if d, ok := durationOf(v, time.Millisecond); ok {
			return d
		}
	}
	v := strings.TrimSpace(h.Get("retry-after"))
	if v == "" {
		return 0
	}
	if _, err := strconv.ParseFloat(v, 64); err == nil {
		d, _ := durationOf(v, time.Second)
		return d
	}
	if t, err := http.ParseTime(v); err == nil && t.After(now) {
		return t.Sub(now)
	}
	return 0
}

// durationOf 把非负的有限数字串按 unit 换成时长，超出范围时取 time.Duration 的最大值。
func durationOf(v string, unit time.Duration) (time.Duration, bool) {
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || math.IsNaN(f) || math.IsInf(f, 0) || f < 0 {
		return 0, false
	}
	ns := f * float64(unit)
	if ns >= math.MaxInt64 {
		return time.Duration(math.MaxInt64), true
	}
	return time.Duration(ns), true
}

// truncateRunes 按字符截断，避免切坏多字节字符。
func truncateRunes(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}
