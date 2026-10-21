//go:build integration

// a2a_zk_list 遍历真 ZK 列出所有注册的 agent（namespace / service / instance），并对每个
// 实例**诊断能否被 Discover 发现**——排查"zk 里明明有节点但 client 发现不到"（Discover
// 静默跳过 malformed / 编码不一致的节点，本工具连被跳过的也列出 + 原因）。
//
//	go run -tags integration ./example/a2a/zk_list -zk 127.0.0.1:2181            # 遍历全部 namespace
//	go run -tags integration ./example/a2a/zk_list -zk 127.0.0.1:2181 -ns example # 只看某 namespace
package main

import (
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"crew-go/a2a/zk"
)

func main() {
	var (
		zkEndpoint = flag.String("zk", "", "ZooKeeper 地址（逗号分隔多节点），如 127.0.0.1:2181")
		nsFilter   = flag.String("ns", "", "只看指定 namespace（默认遍历全部）")
		treeMode   = flag.Bool("tree", false, "raw dump 整棵 service 子树（完整路径 + payload 原文，不假定层级）——排查跨语言对端真实注册结构")
	)
	flag.Parse()
	if *zkEndpoint == "" {
		log.Fatal("必填：-zk <地址>")
	}

	dir, err := zk.New(strings.Split(*zkEndpoint, ","), 10*time.Second)
	if err != nil {
		log.Fatalf("zk.New: %v", err)
	}
	defer func() { _ = dir.Close() }()
	fmt.Printf("已连 ZK %s\n\n", *zkEndpoint)

	var namespaces []string
	if *nsFilter != "" {
		namespaces = []string{*nsFilter}
	} else if namespaces, err = dir.ListNamespaces(); err != nil {
		log.Fatalf("ListNamespaces: %v", err)
	}
	if len(namespaces) == 0 {
		fmt.Println("（/crew/a2a 下没有任何 namespace——无人注册）")
		return
	}

	total, healthy := 0, 0
	for _, ns := range namespaces {
		services, err := dir.ListServiceNames(ns)
		if err != nil {
			log.Printf("ListServiceNames(%s): %v", ns, err)
			continue
		}
		if len(services) == 0 {
			fmt.Printf("【%s】(无 service)\n", ns)
			continue
		}
		for _, svc := range services {
			if *treeMode {
				dumpService(dir, ns, svc)
				continue
			}
			nodes, err := dir.Inspect(ns, svc)
			if err != nil {
				log.Printf("Inspect(%s/%s): %v", ns, svc, err)
				continue
			}
			fmt.Printf("【%s/%s】%d 个实例节点\n", ns, svc, len(nodes))
			for _, n := range nodes {
				total++
				if n.Parsed != nil {
					healthy++
					fmt.Printf("  ✓ agent=%s/%s → instanceId=%s baseURL=%s status=%s weight=%s\n",
						n.AgentName, n.NodeName, n.Parsed.InstanceID, n.Parsed.BaseURL, n.Parsed.Status, weightStr(n.Parsed.Weight))
				} else {
					fmt.Printf("  ✗ agent=%s/%s → Discover 会跳过：%s\n", n.AgentName, n.NodeName, n.ParseError)
				}
			}
		}
	}
	if !*treeMode {
		fmt.Printf("\n共 %d 个实例节点：%d 个可被 Discover 发现，%d 个被跳过（✗ 的原因即 client 发现不到的根因）\n",
			total, healthy, total-healthy)
	}
}

// dumpService raw dump 一个 service 子树（不假定层级）：照实打印每个节点的相对缩进 + 是否带
// payload + payload 原文。用于看清跨语言对端的真实注册层级（统一 4 层
// ns/serviceName/agentName/instanceId）与 payload 字段——不解析、不校验，所见即 zk 里的真实内容。
func dumpService(dir *zk.Directory, ns, svc string) {
	nodes, err := dir.DumpSubtree(ns, svc)
	if err != nil {
		log.Printf("DumpSubtree(%s/%s): %v", ns, svc, err)
		return
	}
	fmt.Printf("【%s/%s】raw 子树共 %d 节点（不假定层级，所见即 zk 真实内容）\n", ns, svc, len(nodes))
	for _, n := range nodes {
		indent := strings.Repeat("  ", n.Depth)
		seg := n.Path[strings.LastIndex(n.Path, "/")+1:]
		switch {
		case n.Err != "":
			fmt.Printf("%s%s  [读取失败: %s]\n", indent, seg, n.Err)
		case n.HasData:
			fmt.Printf("%s%s  [叶子/实例，payload %d bytes]\n", indent, seg, len(n.RawData))
			fmt.Printf("%s    payload: %s\n", indent, n.RawData)
		default:
			fmt.Printf("%s%s/  [中间节点，无 payload]\n", indent, seg)
		}
	}
	fmt.Println()
}

// weightStr 安全解 *int（nil → 默认 100 标记）。
func weightStr(w *int) string {
	if w == nil {
		return "nil(默认100)"
	}
	return fmt.Sprintf("%d", *w)
}
