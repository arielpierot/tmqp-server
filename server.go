package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
)

var Version = "TMQP 0.1\n"

func main() {

	ln, err := net.Listen("tcp", ":4000")
	checkError(err)
	fmt.Println("ONLINE! WAITING NEW CONNECTIONS...")
	for {
		conn, err := ln.Accept()
		checkError(err)
		go handleConn(conn)
	}

	os.Exit(0)
}

func handleConn(conn net.Conn) {

	fmt.Println("NEW CONNECTION!")

	msg, err := bufio.NewReader(conn).ReadString('\n')

	if strings.Compare(msg, Version) == 0 {

		_, err = conn.Write([]byte("START\n"))
		msg, err = bufio.NewReader(conn).ReadString('\n')

		if strings.Compare(msg, "START OK\n") == 0 {
			fmt.Println("CONNECTION ESTABLISHED")
			go messageParser(conn)
		}

	} else {
		_, err = conn.Write([]byte("INVALID VERSION\n"))
	}
	checkError(err)
}

func messageParser(conn net.Conn) {

	fmt.Println("NEW THREAD CREATED!")

}

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL ERROR: %s", err.Error())
		os.Exit(1)
	}
}
