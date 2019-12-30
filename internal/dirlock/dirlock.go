// +build !windows,!illumos

package dirlock

import (
	"fmt"
	"os"
	"syscall"
)

// 文件锁结构体
type DirLock struct {
	dir string   // 文件目录
	f   *os.File // 文件对象
}

// 创建一个新的文件锁
func New(dir string) *DirLock {
	return &DirLock{
		dir: dir,
	}
}

// 锁住
func (l *DirLock) Lock() error {
	f, err := os.Open(l.dir)
	if err != nil {
		return err
	}
	l.f = f
	err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		return fmt.Errorf("cannot flock directory %s - %s (possibly in use by another instance of nsqd)", l.dir, err)
	}
	return nil
}

// 解锁
func (l *DirLock) Unlock() error {
	defer l.f.Close()
	return syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
}
