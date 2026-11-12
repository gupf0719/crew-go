// durable_job 演示独立作业服务的结果在宿主重启后唤醒原任务。
package main

import (
	"context"
	"crypto/rand"
	dbsql "database/sql"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

type buildService struct {
	db *dbsql.DB
}

func newBuildService(ctx context.Context, db *dbsql.DB) (buildService, error) {
	_, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS builds (
		id TEXT PRIMARY KEY, input TEXT NOT NULL, handle TEXT NOT NULL,
		state TEXT NOT NULL DEFAULT 'queued', result TEXT NOT NULL DEFAULT ''
	)`)
	return buildService{db}, err
}

func (s buildService) submit(ctx context.Context, id, input string) (string, error) {
	_, err := s.db.ExecContext(ctx, "INSERT INTO builds (id, input, handle) VALUES (?, ?, ?) ON CONFLICT (id) DO NOTHING",
		id, input, "build-"+rand.Text())
	if err != nil {
		return "", err
	}
	var stored, handle string
	if err := s.db.QueryRowContext(ctx, "SELECT input, handle FROM builds WHERE id = ?", id).Scan(&stored, &handle); err != nil {
		return "", err
	}
	if stored != input {
		return "", errors.New("external build reused a key with different input")
	}
	return handle, nil
}

func (s buildService) lookup(ctx context.Context, id string) (*externalJob, error) {
	var remote externalJob
	var state string
	err := s.db.QueryRowContext(ctx, "SELECT handle, state, result FROM builds WHERE id = ?", id).
		Scan(&remote.handle, &state, &remote.result)
	if errors.Is(err, dbsql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	remote.completed = state == completed
	return &remote, nil
}

func (s buildService) cancel(ctx context.Context, handle string) error {
	_, err := s.db.ExecContext(ctx, "UPDATE builds SET state = ? WHERE handle = ? AND state = 'queued'", cancelled, handle)
	return err
}

func (s buildService) complete(ctx context.Context, id, result string) error {
	_, err := s.db.ExecContext(ctx, "UPDATE builds SET state = ?, result = ? WHERE id = ? AND state = 'queued'", completed, result, id)
	return err
}

func openDatabase(path string) (*dbsql.DB, error) {
	dsn := (&url.URL{Scheme: "file", Path: path, RawQuery: "_pragma=busy_timeout(5000)&_pragma=case_sensitive_like(1)&_txlock=immediate"}).String()
	db, err := dbsql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	return db, nil
}

func sendCallback(ctx context.Context, r *runner, id string) int {
	req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/callback", strings.NewReader(fmt.Sprintf(`{"id":%q}`, id)))
	w := httptest.NewRecorder()
	r.callback(w, req)
	return w.Code
}

func demo(ctx context.Context, dir string) error {
	provider, err := openDatabase(filepath.Join(dir, "builds.db"))
	if err != nil {
		return err
	}
	defer func() { _ = provider.Close() }() // 示例结束后进程会释放连接。
	service, err := newBuildService(ctx, provider)
	if err != nil {
		return err
	}
	path := filepath.Join(dir, "workflow.db")
	hostA, err := openDatabase(path)
	if err != nil {
		return err
	}
	defer func() { _ = hostA.Close() }() // 中途出错时释放连接。
	a, err := newRunner(ctx, hostA, service)
	if err != nil {
		return err
	}
	id := rand.Text()
	if err := a.create(ctx, id, "example release build", time.Now().Add(time.Hour)); err != nil {
		return err
	}
	if err := a.drive(ctx, id); err != nil {
		return err
	}
	before, err := a.load(ctx, id)
	if err != nil {
		return err
	}
	if err := hostA.Close(); err != nil {
		return err
	}
	fmt.Printf("宿主 A 退出，等待 %s（%s）\n", id, before.handle)

	hostB, err := openDatabase(path)
	if err != nil {
		return err
	}
	defer func() { _ = hostB.Close() }() // 示例结束后进程会释放连接。
	b, err := newRunner(ctx, hostB, service)
	if err != nil {
		return err
	}
	if err := service.complete(ctx, id, "artifact: release.tar.gz"); err != nil {
		return err
	}
	for range 2 {
		if code := sendCallback(ctx, b, id); code != http.StatusAccepted {
			return fmt.Errorf("callback returned %d", code)
		}
	}
	after, err := b.load(ctx, id)
	if err != nil {
		return err
	}
	if !after.delivered || after.decision != completed || after.handle != before.handle {
		return errors.New("resumed job did not deliver the original build result")
	}
	fmt.Printf("宿主 B 完成原任务 %s，结果 %s\n", id, after.result)
	return nil
}

func main() {
	dir, err := os.MkdirTemp("", "crew-durable-job-")
	if err != nil {
		log.Fatal(err)
	}
	err = demo(context.Background(), dir)
	if cleanupErr := os.RemoveAll(dir); cleanupErr != nil {
		log.Print(cleanupErr)
	}
	if err != nil {
		log.Fatal(err)
	}
}
