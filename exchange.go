package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os/exec"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	CMD_REQUEST_PORT     = 0
	CMD_REQUEST_RESOURCE = 1
	CMD_ASK_PORT         = 2
	CMD_SPAWN            = 3
	CMD_HEARTBEAT        = 4
)

var (
	spwan_child_num_total int32 = 0
	spwan_child_num       int32 = 0
	deadclock_lock        sync.Mutex
)

type InterProcessHeader struct {
	cmd int32
	len int32
}

func (s *ServerStat) spwan() {

	deadclock_lock.Lock()
	spwan_child_num++
	//just a very large number
	deadclock.Reset(100 * 265 * 24 * time.Hour)
	deadclock_lock.Unlock()
	n := atomic.AddInt32(&spwan_child_num_total, 1)
	cmd := exec.Command("./data_exchange_local_go", strconv.Itoa(s.local_port), strconv.Itoa(s.ask_port), strconv.Itoa(int(n)))
	cmd.Start()
	cmd.Wait()

	deadclock_lock.Lock()
	spwan_child_num--
	//no children .so exit after 3 minutes
	if spwan_child_num == 0 {
		deadclock.Reset(3 * time.Minute)
	}
	deadclock_lock.Unlock()
}
func (s *ServerStat) do_CMD_ASK_PORT(head InterProcessHeader, data []byte) {
	//we don't care
	return
}
func (s *ServerStat) do_CMD_SPAWN(head InterProcessHeader, data []byte) {
	LOG.Printf("we spawn data_exchange_local_go %d %d\n", s.local_port, s.ask_port)
	go s.spwan()
}
func (s *ServerStat) do_CMD_REQUEST_PORT(head InterProcessHeader, data []byte) {
	s.ask_port, _ = strconv.Atoi(string(data))
	LOG.Printf("we get port %s:%s   %d\n", s.conf.ip, data, s.ask_port)
}
func (s *ServerStat) do_cmd(head InterProcessHeader, data []byte) error {
	switch {
	case head.cmd == CMD_ASK_PORT:
		s.do_CMD_ASK_PORT(head, data)
	case head.cmd == CMD_SPAWN:
		s.do_CMD_SPAWN(head, data)
	case head.cmd == CMD_REQUEST_PORT:
		s.do_CMD_REQUEST_PORT(head, data)
	default:
		LOG.Printf("cmd error %d\n", head.cmd)
		return fmt.Errorf("unrecognize cmd %d", head.cmd)
	}

	return nil
}
func (s ServerStat) master_heartbeat() {
	beat := InterProcessHeader{CMD_HEARTBEAT, 0}
	buf := &bytes.Buffer{}
	binary.Write(buf, binary.BigEndian, beat)

	c := time.Tick(20 * time.Second)
	for _ = range c {
		_, err := remote_conn.Write(buf.Bytes())
		if err != nil {
			LOG.Println("master send heartbeat error")
			return
		}
	}
}
func (s *ServerStat) master_main_process(g_wg *sync.WaitGroup) {

	head := InterProcessHeader{CMD_REQUEST_PORT, 0}
	buf := &bytes.Buffer{}

	defer func() {
		LOG.Println("master_main_process done")
		g_wg.Done()
	}()

	err := binary.Write(buf, binary.BigEndian, head)
	if err != nil {
		LOG.Printf("binary.Write error:%s\n", err)
		return
	}
	_, err = remote_conn.Write(buf.Bytes())
	if err != nil {
		LOG.Printf("write error : %s\n", err)
		return
	}
	go s.master_heartbeat()
	for {
		head_buf := make([]byte, 8)
		data_buf := []byte{}
		_, err = io.ReadFull(remote_conn, head_buf)
		if err != nil {
			LOG.Printf("ReadFull:%s\n", err)
			return
		}
		r := bytes.NewReader(head_buf)
		binary.Read(r, binary.BigEndian, &head.cmd)
		binary.Read(r, binary.BigEndian, &head.len)
		if head.len > 0 {
			data_buf = make([]byte, head.len)
			_, err = io.ReadFull(remote_conn, data_buf)
			if err != nil {
				LOG.Printf("ReadFull:%s\n", err)
				return
			}
		}

		s.do_cmd(head, data_buf)
	}

}

func send_data(conn *net.TCPConn, data []byte, wg *sync.WaitGroup, err *error) {
	_, *err = conn.Write(data)
	wg.Done()
}
func (s *ServerStat) pre_remote() error {
	//send CMD_ASK_PORT message
	var err error
	port := strconv.Itoa(s.ask_port)
	buf := &bytes.Buffer{}
	head := InterProcessHeader{CMD_ASK_PORT, int32(len(port))}

	err = binary.Write(buf, binary.BigEndian, head)
	if err != nil {
		LOG.Printf("binary.Write error:%s\n", err)
		return fmt.Errorf("%s\n", err)
	}
	_, err = remote_conn.Write(buf.Bytes())
	if err != nil {
		LOG.Printf("write error : %s\n", err)
		return fmt.Errorf("%s\n", err)
	}

	_, err = remote_conn.Write([]byte(port))
	if err != nil {
		LOG.Printf("write error : %s\n", err)
		return fmt.Errorf("%s\n", err)
	}
	//receive ACK message
	head_buf := make([]byte, 8)
	data_buf := []byte{}
	_, err = io.ReadFull(remote_conn, head_buf)
	if err != nil {
		LOG.Printf("ReadFull:%s\n", err)
		return fmt.Errorf("%s\n", err)
	}
	r := bytes.NewReader(head_buf)
	binary.Read(r, binary.BigEndian, &head.cmd)
	binary.Read(r, binary.BigEndian, &head.len)
	if head.len > 0 {
		data_buf = make([]byte, head.len)
		_, err = io.ReadFull(remote_conn, data_buf)
		if err != nil {
			LOG.Printf("ReadFull:%s\n", err)
			return fmt.Errorf("%s\n", err)
		}
	}
	/*we don't care about the message
	 *do_cmd(head, data_buf)
	 */
	LOG.Printf("remote receive cmd %d\n", head.cmd)

	return nil
}
func (s *ServerStat) local_data_process(sig chan<- int) {

	var wg sync.WaitGroup
	var send_err error
	var buf [2000]byte

	defer func() {
		LOG.Printf("local_data_process done %s", send_err)
		sig <- 0
	}()
	for {
		n, err := local_conn.Read(buf[0:])
		if err != nil || n == 0 {
			LOG.Printf("local_data_process done.local rcv_err:%s len:%d\n", err, n)
			return
		}
		deadclock_lock.Lock()
		deadclock.Reset(3 * time.Minute)
		deadclock_lock.Unlock()

		wg.Add(1)
		go send_data(remote_conn, buf[0:n], &wg, &send_err)
		wg.Wait()
		if send_err != nil {
			LOG.Printf("local_data_process done.remote send_err:%s\n", send_err)
			return
		}
	}
}
func (s *ServerStat) remote_data_process(sig chan<- int) {

	//now we start recv remote data and send to local port
	var wg sync.WaitGroup
	var send_err error
	var bufs [2000]byte

	defer func() {
		LOG.Println("remote_data_process done")
		sig <- 0
	}()

	for {
		n, err := remote_conn.Read(bufs[0:])
		if err != nil || n == 0 {
			LOG.Printf("remote_data_process done.local rcv_err:%s len:%d\n", err, n)
			return
		}
		deadclock_lock.Lock()
		deadclock.Reset(3 * time.Minute)
		deadclock_lock.Unlock()

		wg.Add(1)
		go send_data(local_conn, bufs[0:n], &wg, &send_err)
		wg.Wait()
		if send_err != nil {
			LOG.Printf("remote_data_process done.remote send_err:%s\n", send_err)
			return
		}
	}
}
