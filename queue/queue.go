package queue

import (
	"errors"
)

type Queue interface {
	New(name string) Queue
	Dequeue() (v string, err error) //添加
	Enqueue(v string) (err error)   //删除
	Len() int                       //大小
}

var (
	ErrEmpty  = errors.New("Queue empty")
	ErrExists = errors.New("Already in queue")
)
