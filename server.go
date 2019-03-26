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

var queues = make(map[string](chan []byte))

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

	status := true

	for status == true {
		status = readPackage(conn)
	}

	conn.Close()

}

func readPackage(conn net.Conn) bool {

	type_byte := make([]byte, TYPE)
	_, err := conn.Read(type_byte[0:TYPE])

	if err == nil {
		type_package := int(type_byte[0])

		// fmt.Println("RECEPTING NEW PACKET...")
		// fmt.Println("TYPE: ", type_package)

		size_bytes := make([]byte, SIZE)
		_, err := conn.Read(size_bytes[0:SIZE])

		if err != nil {
			fmt.Println("ERROR - SIZE: ", err)
			os.Exit(1)
		} else {

			size_package := binary.LittleEndian.Uint32(size_bytes[0:SIZE])

			content_bytes := make([]byte, size_package)

			// fmt.Println("SIZE: ", size_package)

			_, err := conn.Read(content_bytes[0:size_package])

			if err != nil {
				fmt.Println("ERROR - CONTEUDO: ", err)
				os.Exit(1)
			} else {
				if type_package == 1 {
					newQueue := &QueueDeclare{}
					err := proto.Unmarshal(content_bytes, newQueue)
					if err != nil {
						fmt.Println("ERROR - UNMARSHAL: ", err)
					} else {
						fmt.Println("NEW QUEUE <= ", newQueue.GetName())
						name := newQueue.GetName()
						ch := make(chan []byte, 10000)
						queues[name] = ch
					}
				} else if type_package == 2 {
					message := &Message{}
					publish_bytes := content_bytes

					err := proto.Unmarshal(content_bytes, message)

					if err != nil {
						fmt.Println("ERROR - UNMARSHAL: ", err)
					} else {
						fmt.Println("MESSAGE RECEIVED FROM => ", message.GetSender())
						name := message.GetQueue()
						queue := queues[name]
						queue <- publish_bytes
					}
				} else if type_package == 3 {
					consumeQueue := &ConsumeQueue{}
					err := proto.Unmarshal(content_bytes, consumeQueue)

					if err != nil {
						fmt.Println("ERROR - UNMARSHAL: ", err)
					} else {
						nameQueue := consumeQueue.GetQueue()
						go listener(conn, nameQueue)
					}
				}
			}
		}

	} else {
		fmt.Println("CONNECTION LOST WITH A CLIENT...")
		return false
	}

	return true
}

func listener(conn net.Conn, nameQueue string) {
	for {
		queue := queues[nameQueue]
		output := <-queue

		var type_package int8
		type_package = 2

		type_byte := make([]byte, TYPE)
		type_byte[0] = (byte(type_package))

		err := binary.Write(conn, binary.LittleEndian, type_byte)

		if err == nil {

			size_package := uint32(len(output))
			size_bytes := make([]byte, SIZE)
			binary.LittleEndian.PutUint32(size_bytes, uint32(size_package))
			err := binary.Write(conn, binary.LittleEndian, size_bytes)

			message := &Message{}
			// fmt.Println("OUT: ", output)
			proto.Unmarshal(output, message)
			// fmt.Println("QUEUE NAME: ", message.GetQueue())
			// fmt.Println("CONTENT: ", string(message.GetContent()))

			if err != nil {

				fmt.Println("ERROR - WRITE SIZE: ", err)

			} else {

				_, err := conn.Write(output)

				if err == nil {
					fmt.Println("LISTENER SENDING MESSAGES FROM", nameQueue, "TO CLIENTS")
				} else {
					fmt.Println("ERROR - TYPEx: ", err)
					os.Exit(1)
				}
			}
		}
	}
}

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL ERROR: %s \n", err.Error())
		os.Exit(1)
	}
}
