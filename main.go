package main

import (
	"bufio"
	"bytes"
	"fmt"
	zmq "github.com/pebbe/zmq2"
	"io"
	"os"
	"time"
)

type TStmp struct {
	W     io.Writer
	Ctx   *zmq.Context
	Z     *zmq.Socket
	topic string
}

// NewTStmp returns a pointer to TStmp. Remember to run Close() method
// to clean up.
func NewTStmp(o io.Writer, topic, addr string) (*TStmp, error) {
	ts := new(TStmp)
	ts.W = o
	ctx, err := zmq.NewContext(1)
	if err != nil {
		return nil, err
	}
	ts.Ctx = ctx
	sock, err := ctx.NewSocket(zmq.PUB)
	if err != nil {
		return nil, err
	}
	err = sock.Connect(addr)
	if err != nil {
		return nil, err
	}
	ts.Z = sock
	ts.topic = topic
	//time.Sleep(500 * time.Millisecond) // allow zeromq to connect
	return ts, err
}

func (t *TStmp) Write(p []byte) (int, error) {
	b := bytes.NewBuffer(p)
	scn := bufio.NewScanner(b)
	for scn.Scan() {
		bs := scn.Bytes()
		ts := time.Now().Format("2006-01-02 15:04:05.00000 SGT ") + t.topic + ": "
		t.W.Write([]byte(ts))
		t.W.Write(bs)
		t.W.Write([]byte("\n"))
		msg := ts + string(bs) + "\n"
		_, err := t.Z.Send(msg, 0)
		if err != nil {
			return 0, err
		}
	}

	return len(p), scn.Err()
}
func (t *TStmp) Close() {
	t.Z.Close()
	t.Ctx.Term()
}

func main() {
	topic := ""
	addr := "ipc:///home/siuyin/sock/pubhub.ipc"
	if t := os.Getenv("TSTMP_TOPIC"); t != "" {
		topic = t
	}
	if a := os.Getenv("TSTMP_ADDR"); a != "" {
		addr = a
	}
	ts, err := NewTStmp(os.Stdout, topic, addr)
	if err != nil {
		fmt.Println(err)
	}
	defer ts.Close()
	for {
		_, err = io.Copy(ts, os.Stdin)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println("Restarting tstmp")
		time.Sleep(1 * time.Second)
	}
}
