package main

import (
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

var (
	LOG         *Log
	deadclock   *time.Timer
	server      ServerStat
	local_conn  *net.TCPConn
	remote_conn *net.TCPConn
)

const (
	logfilesize = 2 * 1024 * 1024 //2 M
)

type ServerStat struct {
	local_port int
	ask_port   int
	conf       IniConf
}
type Log struct {
	logfile  *os.File
	logger   *log.Logger
	filename string
}

func InitLog(tag string) (*Log, error) {
	var err error
	ret := new(Log)
	ret.filename = "./log"
	ret.logfile, err = os.OpenFile(ret.filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, errors.New("fail")
	}
	ret.logger = log.New(ret.logfile, tag, log.LstdFlags)

	return ret, nil
}
func (l *Log) Printf(format string, v ...interface{}) {

	info, err := os.Stat(l.filename)
	if err != nil || info.Size() > logfilesize {
		l.logfile.Close()
		l.logfile, err = os.OpenFile(l.filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
		if err != nil {
			return
		}
		l.logger.SetOutput(l.logfile)
	}
	l.logger.Printf(format, v...)
}
func (l *Log) Println(v ...interface{}) {

	info, err := os.Stat(l.filename)
	if err != nil || info.Size() > logfilesize {
		l.logfile.Close()
		l.logfile, err = os.OpenFile(l.filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
		if err != nil {
			return
		}
		l.logger.SetOutput(l.logfile)
	}
	l.logger.Println(v...)
}
func (l *Log) Fatalf(format string, v ...interface{}) {
	l.logger.Fatalf(format, v...)
}
func (l *Log) Fatalln(v ...interface{}) {
	l.logger.Fatalln(v...)
}
func killThis() {
	select {
	case <-deadclock.C:
		LOG.Printf("Timeout and we quit\n")
		os.Exit(0)
	}
}
func main() {

	var err error

	if len(os.Args) != 3 && len(os.Args) != 4 {
		fmt.Printf("usage : %s local_port ask_port [num]\n", os.Args[0])
		os.Exit(1)
	}
	server.local_port, _ = strconv.Atoi(os.Args[1])
	server.ask_port, _ = strconv.Atoi(os.Args[2])

	if server.ask_port == 0 {
		LOG, err = InitLog("Master: ")
	} else {
		n, _ := strconv.Atoi(os.Args[3])
		LOG, err = InitLog("Client_" + strconv.Itoa(n) + ": ")
	}
	if err != nil {
		fmt.Println("init log error")
		os.Exit(1)
	}

	LOG.Println(">>>>>>>>>>>>")
	LOG.Printf("server is starting:local_port:%d ask_port:%d\n", server.local_port, server.ask_port)

	server.conf.Initconf()
	err = server.conf.Readconf()
	if err != nil {
		LOG.Fatalf("conf err %s", err)
	}

	LOG.Printf("remote_address:%s:%d\n", server.conf.ip, server.conf.port)
	LOG.Println("<<<<<<<<<<<<")

	LOG.Println("server try connect ...")

	deadclock = time.NewTimer(3 * time.Minute)
	go killThis()
	if server.ask_port != 0 {
		tcpAddr, err := net.ResolveTCPAddr("tcp4", "127.0.0.1:"+strconv.Itoa(server.local_port))
		if err != nil {
			LOG.Printf("resolve tcp addr error %s\n", "127.0.0.1:"+strconv.Itoa(server.local_port))
			os.Exit(1)
		}
		local_conn, err = net.DialTCP("tcp", nil, tcpAddr)
		for err != nil {
			LOG.Printf("connect error %s\n", "127.0.0.1:"+strconv.Itoa(server.local_port))
			time.Sleep(5 * time.Second)
			local_conn, err = net.DialTCP("tcp", nil, tcpAddr)
		}
		LOG.Printf("server connect 127.0.0.1:%d success\n", server.local_port)
	}

	tcpAddr, err := net.ResolveTCPAddr("tcp4", server.conf.ip+":"+strconv.Itoa(server.conf.port))
	if err != nil {
		LOG.Fatalf("ResolveTcpAddr error : %s", err)
	}

	remote_conn, err = net.DialTCP("tcp", nil, tcpAddr)
	for err != nil {
		time.Sleep(5 * time.Second)
		remote_conn, err = net.DialTCP("tcp", nil, tcpAddr)
	}

	LOG.Printf("server connect %s:%d success\n", server.conf.ip, server.conf.port)

	LOG.Println("server started ...")

	if server.ask_port == 0 {
		var g_wg sync.WaitGroup
		g_wg.Add(1)
		go server.master_main_process(&g_wg)
		g_wg.Wait()
	} else {
		err = server.pre_remote()
		if err != nil {
			LOG.Printf("server.pre_remote error:%s\n", err)
			return
		}
		sig := make(chan int)
		//exit when any one complete
		go server.remote_data_process(sig)
		go server.local_data_process(sig)

		select {
		case <-sig:
			break
		}
	}
	LOG.Println("... server end")
	os.Exit(0)
}
