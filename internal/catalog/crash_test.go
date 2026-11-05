package catalog

import (
	"fmt"
	"os"
	"os/exec"
	"testing"
)

// TestEngineCrashRecovery 用子进程模拟「未优雅关闭」（os.Exit 不 Close），
// 父进程重新打开后应完整恢复全部已 fsync 的记录，且无损坏。
func TestEngineCrashRecovery(t *testing.T) {
	if os.Getenv("CRASH_CHILD") == "1" {
		dir := os.Getenv("CRASH_DIR")
		cfg := Config{Dir: dir, Gen: 0, Wal: WalConfig{Dir: dir, SyncEvery: 1}}
		e, err := NewEngine(cfg, nil)
		if err != nil {
			os.Exit(2)
		}
		for i := 0; i < 500; i++ {
			ev := Event{Op: OpPut, Doc: Doc{Bucket: "b", Key: fmt.Sprintf("k%04d", i), VersionID: "v", Size: 1}}
			if err := e.Append(ev); err != nil {
				os.Exit(2)
			}
		}
		// 模拟崩溃：直接退出，不调用 Close
		os.Exit(1)
	}

	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run", "TestEngineCrashRecovery")
	cmd.Env = append(os.Environ(), "CRASH_CHILD=1", "CRASH_DIR="+dir)
	if err := cmd.Run(); err == nil {
		t.Fatal("child should exit non-zero (simulated crash)")
	}

	// 重新打开，应完整恢复 500 条（SyncEvery=1，每条都已 fsync）
	e, err := NewEngine(Config{Dir: dir, Gen: 0, Wal: WalConfig{Dir: dir, SyncEvery: 1}}, nil)
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}
	defer e.Close()
	if docs := e.Status().Docs; docs != 500 {
		t.Fatalf("recovered docs = %d, want 500", docs)
	}
	if seq := e.Status().AppliedSeq; seq != 500 {
		t.Fatalf("recovered appliedSeq = %d, want 500", seq)
	}
}
