// Package fanout 在同一批内按顺序派发，等待已派发的工作退出。
package fanout

import (
	"context"
	"sync"
)

// Run 返回实际派发的前缀长度。limit 为 0 时全部并发派发；大于 0 时，等待空位期间响应取消。
// 每项独占 goroutine，调用方负责记录结果并处理 panic 或 Goexit。
func Run(ctx context.Context, count, limit int, run func(int)) int {
	var active chan struct{}
	if limit > 0 && limit < count {
		active = make(chan struct{}, limit)
	}
	var wg sync.WaitGroup
	defer wg.Wait()
	for i := range count {
		if active != nil {
			select {
			case active <- struct{}{}:
			case <-ctx.Done():
				return i
			}
		}
		// select 在空位和取消同时就绪时可以选空位，再检查一次以免派发。
		if limit > 0 && ctx.Err() != nil {
			if active != nil {
				<-active
			}
			return i
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if active != nil {
				defer func() { <-active }()
			}
			run(i)
		}()
	}
	return count
}
