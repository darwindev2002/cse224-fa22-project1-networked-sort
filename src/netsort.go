package main

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net"
	"os"
	"slices"
	"strconv"
	"sync"
	"time"

	"gopkg.in/yaml.v2"
)

type ServerConfigs struct {
	Servers []struct {
		ServerId int    `yaml:"serverId"`
		Host     string `yaml:"host"`
		Port     string `yaml:"port"`
	} `yaml:"servers"`
}

func readServerConfigs(configPath string) ServerConfigs {
	f, err := ioutil.ReadFile(configPath)

	if err != nil {
		log.Fatalf("could not read config file %s : %v", configPath, err)
	}

	scs := ServerConfigs{}
	err = yaml.Unmarshal(f, &scs)

	return scs
}

const (
	PROTO                 = "tcp"
	StreamInProgress byte = 0
	StreamCompleted  byte = 1
)

var wg sync.WaitGroup

// var nServers int = 1

func checkErrorWithExit(err error) {
	if err != nil {
		log.Fatalf("Fatal error: %s\n", err)
	}
}

func checkErrorWithoutExit(err error) {
	if err != nil {
		fmt.Printf("Error : %s\n", err)
	}
}

// Listen for connection to receive data
func receiveData(w_ch chan<- []byte, host string, port string) {

	serverAddress := host + ":" + port
	fmt.Printf("Receive - Starting %v server on %v\n", PROTO, serverAddress)

	listener, err := net.Listen("tcp", serverAddress)
	if err != nil {
		log.Fatalf("Receive - Listen error raised: %s\n", err)
		os.Exit(1)
	}
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatalf("Receive - Accept error raised: %s\n", err)
		}
		go handleReceiveConnection(conn, w_ch)
	}

}

// Handle received data connection
func handleReceiveConnection(conn net.Conn, w_ch chan<- []byte) {
	for {
		record := make([]byte, 0, 101)
		bytesAlrdyRead := 0

		for {
			buf := make([]byte, 101-bytesAlrdyRead)
			n, err := conn.Read(buf)

			if err != nil {
				// if buf[0] == 1
				if err == io.EOF {
					// fmt.Println("All data has been read, closing, exiting.")
					// return
					continue
				}
				log.Fatalf("Receive - Read error raised: %s\n", err)
			}
			// fmt.Printf("Receive - Received length-%v data: %v\n", n, hex.EncodeToString(buf[:n]))
			record = append(record, buf[:n]...)
			// fmt.Printf("Receive - Updated record: %v\n", hex.EncodeToString(record))
			bytesAlrdyRead += n
			if bytesAlrdyRead >= 101 {
				break
			}
		}

		streamComplete := (record[0] == 1)
		fmt.Printf("Receive - Stream complete = %v\n", streamComplete)

		w_ch <- record
		// for {
		// 	select {
		// 	case w_ch <- record:
		// 	default:
		// 		fmt.Println("*** !!! Channel full. Wait for 1 sec ... ***")
		// 		time.Sleep(1000 * time.Millisecond)
		// 		continue
		// 	}
		// 	break
		// }

		if !streamComplete {
			fmt.Printf("Receive - Completed receiving data with key[1:] = %v...\n", hex.EncodeToString(record[1:5]))
		} else {
			conn.Close()
			break
		}
	}
}

func collectData(w_ch chan []byte, nServers int) [][]byte {
	records := make([][]byte, 0)
	completed := 0
	fmt.Printf("Collect - starts collecting ...\n")
	for completed < nServers-1 {
		fmt.Printf("Collect - is still collecting ...\n")
		r := <-w_ch
		fmt.Printf("Collect - received one record: %s...\n", hex.EncodeToString(r[:10]))
		if r[0] == StreamCompleted {
			completed += 1
			fmt.Printf("Collect - completed collecting data from %v server(s)\n", completed)
			wg.Done()
		} else {
			records = append(records, r[1:])
		}
	}
	return records
}

func sendData(othersData [][]byte, host string, port string, myServerId int, nMSB int) {
	serverAddress := host + ":" + port
	fmt.Printf("Send - Connecting %v server on %v\n", PROTO, serverAddress)

	conn, _ := handleSendConnection(host, port)
	defer conn.Close()

	for _, d := range othersData {

		_, err := conn.Write([]byte{StreamInProgress})
		checkErrorWithExit(err)

		fmt.Printf("Send - sending data to port %v with key[:10] = %v\n", port, hex.EncodeToString(d[:10]))
		_, err = conn.Write(d)
		checkErrorWithExit(err)

	}

	streamTerminateRecord := make([]byte, 101)
	streamTerminateRecord[0] = byte(StreamCompleted)
	streamTerminateRecord[1] = byte(myServerId << (8 - nMSB))

	_, err := conn.Write(streamTerminateRecord)
	checkErrorWithExit(err)

	wg.Done()
	fmt.Printf("Send - data to %s:%s completed.\n", host, port)
}

func handleSendConnection(host string, port string) (net.Conn, error) {
	serverAddress := host + ":" + port

	// Setup connection
	fmt.Printf("Send Handle - pre for-loop\n")
	for {
		conn, err := net.Dial("tcp", serverAddress)
		fmt.Printf("Send Handle - trying to dial %v\n", serverAddress)
		if err != nil {
			fmt.Printf("Send - Dial to \"%s\" error raised: %s\n", serverAddress, err)
			time.Sleep(1000 * time.Millisecond)
			continue
		}
		fmt.Printf("Send Handle - dial to %v successed\n", serverAddress)
		return conn, nil
	}
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if len(os.Args) != 5 {
		log.Fatal("Usage : ./netsort {serverId} {inputFilePath} {outputFilePath} {configFilePath}")
	}

	// What is my serverId
	serverId, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("Invalid serverId, must be an int %v\n", err)
	}
	fmt.Printf("My server Id: %v\n", serverId)

	// Read server configs from file
	scs := readServerConfigs(os.Args[4])
	fmt.Printf("Got the following server configs: %s\n", scs)

	// Check my server info
	myServerHost := scs.Servers[serverId].Host
	myServerPort := scs.Servers[serverId].Port
	fmt.Printf("My server address: %s:%s\n", myServerHost, myServerPort)

	// Set constants for filtering
	nServers := len(scs.Servers)
	nMSB := int(math.Log2(float64(nServers)))

	/*
		Implement Distributed Sort
	*/

	records := make([][][]byte, nServers)

	// Build connection channels
	w_ch := make(chan []byte)
	defer close(w_ch)

	// Start (goroutine) receiving data
	go receiveData(w_ch, myServerHost, myServerPort)

	// Wait to establish the listener socket
	time.Sleep(1000 * time.Millisecond)

	// Read from input file
	inFile, err := os.Open(os.Args[2])
	if err != nil {
		log.Fatal(err)
	}
	defer inFile.Close()

	reader := bufio.NewReader(inFile)
	for {
		buf := make([]byte, 100)
		n, err := io.ReadFull(reader, buf)
		if err != nil && (err != io.EOF || n != 100) {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}
		firstByte := buf[0]
		toServer := int(firstByte) >> (8 - nMSB)
		// if (toServer == serverId) {
		// 	records = append(records, buf[:n]...)
		// }
		records[toServer] = append(records[toServer], buf[:n])
	}

	for sId, rs := range records {
		fmt.Printf("Need to send Server %v - %v records.\n", sId, len(rs))
	}

	// Send data to other servers
	for _, s := range scs.Servers {
		if s.ServerId != serverId {
			wg.Add(2)

			go sendData(records[s.ServerId], s.Host, s.Port, serverId, nMSB)
		}
	}

	// Start receiving data
	allRecords := collectData(w_ch, nServers) // receives a lst ([][]byte)
	wg.Wait()
	allRecords = append(allRecords, records[serverId]...)
	// Wait to complete receiving data from all servers
	fmt.Printf("This server has a total of %v records.\n", len(allRecords))
	// Sort the data
	slices.SortFunc(allRecords, func(i, j []byte) int {
		return bytes.Compare(i[:10], j[:10])
	})

	// Create output file
	outFile, err := os.OpenFile(os.Args[3], os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	checkErrorWithExit(err)
	defer outFile.Close()

	// Write to output file
	writer := bufio.NewWriter(outFile)
	for _, j := range allRecords {
		_, err := writer.Write(j)
		if err != nil {
			log.Fatal(err)
		}
	}

	writer.Flush()

}
