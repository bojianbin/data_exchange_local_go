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
	"time"
)

const (
	CMD_REQUEST_PORT     = 0
	CMD_REQUEST_RESOURCE = 1
	CMD_ASK_PORT         = 2
	CMD_SPAWN            = 3
	CMD_HEARTBEAT        = 4
)

type InterProcessHeader struct {
	cmd int32
	len int32
}

func (s ServerStat) spwan() {
	cmd := exec.Command("./data_exchange_local_go", strconv.Itoa(s.local_port), strconv.Itoa(s.ask_port))
	cmd.Start()
	cmd.Wait()
}
func (s ServerStat) do_CMD_ASK_PORT(head InterProcessHeader, data []byte) {
	//we don't care
	return
}
func (s ServerStat) do_CMD_SPAWN(head InterProcessHeader, data []byte) {
	LOG.Printf("we spawn data_exchange_local_go %d %d\n", s.local_port, s.ask_port)
	go s.spwan()
	deadclock.Reset(3 * time.Minute)
}
func (s ServerStat) do_CMD_REQUEST_PORT(head InterProcessHeader, data []byte) {
	LOG.Printf("we get port %s:%s\n", s.conf.ip, data)
}
func (s ServerStat) do_cmd(head InterProcessHeader, data []byte) error {
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
			LOG.Println("master send heartbeat error\n")
			return
		}
	}
}
func (s ServerStat) master_main_process() {

	head := InterProcessHeader{CMD_REQUEST_PORT, 0}
	buf := &bytes.Buffer{}

	defer func() {
		LOG.Println("master_main_process done\n")
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
func (s ServerStat) local_data_process() {

	var wg sync.WaitGroup
	var send_err error
	var buf []byte
	defer func() {
		LOG.Println("local_data_process done\n")
		g_wg.Done()
	}()
	for {
		buf = []byte{}
		_, err := local_conn.Read(buf)
		if err != nil {
			LOG.Printf("local_data_process done.local rcv_err:%s\n", err)
			return
		}

		wg.Add(1)
		go send_data(remote_conn, buf, &wg, &send_err)
		wg.Wait()
		if send_err != nil {
			LOG.Printf("local_data_process done.remote send_err:%s\n", send_err)
			return
		}
	}
}
func (s ServerStat) remote_data_process() {

	//send CMD_ASK_PORT message
	port := strconv.Itoa(s.ask_port)
	head := InterProcessHeader{CMD_ASK_PORT, int32(len(port))}
	buf := &bytes.Buffer{}

	defer func() {
		LOG.Println("remote_data_process done\n")
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

	_, err = remote_conn.Write([]byte(port))
	if err != nil {
		LOG.Printf("write error : %s\n", err)
		return
	}
	//receive ACK message
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
	/*we don't care about the message
	 *do_cmd(head, data_buf)
	 */
	//now we start recv remote data and send to local port
	var wg sync.WaitGroup
	var send_err error
	var bufs []byte
	for {
		bufs = []byte{}
		_, err := remote_conn.Read(bufs)
		if err != nil {
			LOG.Printf("local_data_process done.local rcv_err:%s\n", err)
			return
		}

		wg.Add(1)
		go send_data(local_conn, bufs, &wg, &send_err)
		wg.Wait()
		if send_err != nil {
			LOG.Printf("local_data_process done.remote send_err:%s\n", send_err)
			return
		}
	}
}
