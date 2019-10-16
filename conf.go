package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type IniConf struct {
	ip   string
	port int
}

var confFileName string = "./data_exchange_local.conf"

func pathExists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}

	if os.IsNotExist(err) {
		return false
	}

	return false
}

func readIni(arg map[string]string) error {

	if !pathExists(confFileName) {
		return errors.New("conf file is not exit")
	}

	f, err := os.Open(confFileName)
	if err != nil {
		return errors.New("open conf file error")
	}
	defer f.Close()

	var linestring string
	var sections string

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {

		linestring = scanner.Text()
		if err != nil {
			break
		}

		linestring = strings.TrimSpace(linestring)
		if strings.HasPrefix(linestring, "[") {
			s := strings.Index(linestring, "[")
			if s < 0 {
				continue
			}
			s += len("[")
			e := strings.Index(linestring, "]")
			if e < 0 {
				continue
			}
			sections = linestring[s:e]
		} else if strings.HasPrefix(linestring, "#") {
			//don't care
		} else {
			s := strings.Index(linestring, "#")
			if s > 0 {
				linestring = linestring[:s]
			}

			item := strings.Split(linestring, "=")
			item[0] = strings.TrimSpace(item[0])
			item[1] = strings.TrimSpace(item[1])

			arg[sections+":"+item[0]] = item[1]
		}

	}

	return nil
}

func (ret *IniConf) Readconf() error {

	fileIni := map[string]string{}
	err := readIni(fileIni)
	if err != nil {
		return fmt.Errorf(" read Ini error:%s", err)
	}

	ret.ip = fileIni["COMMON:remote_ip"]
	ret.port, err = strconv.Atoi(fileIni["COMMON:remote_port"])
	if err != nil {
		return fmt.Errorf("strconv error:%s", err)
	}

	return nil
}
func (ret *IniConf) Initconf() {
	ret.ip = "134.175.230.138"
	ret.port = 4000
}
