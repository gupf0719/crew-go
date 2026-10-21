// Package envloader 提供从项目根目录 .env 加载环境变量的小工具，
// 供 example/* 下的 main 程序和测试入口共享，避免各处重复实现。
//
// 约定：从本文件所在目录向上查找含 go.mod 的目录，取其同级 .env。
// 文件不存在时静默跳过（生产/CI 通过真实环境变量注入即可）。
package envloader

import (
	"bufio"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// Load 从项目根目录读取 .env 文件，将其中 KEY=VALUE 写入进程环境变量。
// 已设置的环境变量优先（CI 注入的值不会被覆盖）。
// 文件不存在或无法打开时静默返回，不报错。
func Load() {
	dir := projectRoot()
	if dir == "" {
		return
	}

	f, err := os.Open(filepath.Join(dir, ".env"))
	if err != nil {
		return
	}
	defer f.Close() //nolint:errcheck

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, val, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		val = strings.TrimSpace(val)
		// 已有环境变量优先（CI 注入的 key 不被覆盖）。
		if os.Getenv(key) == "" {
			_ = os.Setenv(key, val)
		}
	}
}

// projectRoot 从本文件位置向上查找含 go.mod 的目录，返回绝对路径；未找到返回空串。
func projectRoot() string {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		return ""
	}
	dir := filepath.Dir(filename)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return ""
		}
		dir = parent
	}
}
