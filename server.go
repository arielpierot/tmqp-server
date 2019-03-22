package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"github.com/golang/protobuf/proto"
	"net"
	"os"
	"strings"
)

func main() {

	ln, err := net.Listen("tcp", ":7788")
	checkError(err)
	fmt.Println("ONLINE! WAITING NEW CONNECTIONS...")
	for {
		conn, err := ln.Accept()
		checkError(err)
		go handleConn(conn)
	}

	os.Exit(0)
}

const (
	DELIMITER byte = '\n'
	TYPE      uint = 1
	SIZE      uint = 4
	VERSION        = "TMQP 0.1\n"
)

func handleConn(conn net.Conn) {

	fmt.Println("NEW CONNECTION RECEIVED...")

	msg, err := bufio.NewReader(conn).ReadString('\n')

	if strings.Compare(msg, VERSION) == 0 {

		_, err = conn.Write([]byte("START\n"))
		msg, err = bufio.NewReader(conn).ReadString('\n')

		if strings.Compare(msg, "START OK\n") == 0 {
			fmt.Println("CONNECTION ESTABLISHED...")
			go messageParser(conn)
		}

	} else {
		_, err = conn.Write([]byte("INVALID VERSION\n"))
	}
	checkError(err)
}

func messageParser(conn net.Conn) {

	fmt.Println("NEW THREAD CREATED TO LISTEN AND SERVE...")

	// status := make(chan bool)

	for {
		readPackage(conn)
	}

}

func readPackage(conn net.Conn) {

	type_byte := make([]byte, TYPE)
	_, err := conn.Read(type_byte[0:TYPE])

	if err == nil {
		type_package := int(type_byte[0])

		fmt.Println("RECEBENDO PACOTE...")
		fmt.Println("TYPE: ", type_package)

		size_bytes := make([]byte, SIZE)
		_, err := conn.Read(size_bytes[0:SIZE])

		if err != nil {
			fmt.Println("ERROR - SIZE: ", err)
		} else {

			size_package := binary.LittleEndian.Uint32(size_bytes[0:SIZE])

			content_bytes := make([]byte, size_package)

			fmt.Println("SIZE: ", size_package)

			_, err := conn.Read(content_bytes[0:size_package])

			if err != nil {
				fmt.Println("ERROR - CONTEUDO: ", err)
			} else {
				if type_package == 1 {
					newQueue := &QueueDeclare{}
					err := proto.Unmarshal(content_bytes, newQueue)
					if err != nil {
						fmt.Println("ERROR - UNMARSHAL: ", err)
					} else {
						fmt.Println("NEW QUEUE: ", newQueue.GetName())
						// status <- true
					}
				}
			}
		}

	} else {
		fmt.Println("ERROR - TYPE: ", err)
	}

}

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL ERROR: %s \n", err.Error())
		os.Exit(1)
	}
}
