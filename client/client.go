package main

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"golang.org/x/net/context"

	pb "github.com/bc-vincent-zhao/grpc_tryout/object"
	"google.golang.org/grpc"
)

var (
	loop      = flag.Int("loop", 100, "loop count for reading object")
	http_port = flag.Int("http_port", 10001, "the port http endpoint listens on")
	grpc_port = flag.Int("grpc_port", 10002, "the port grpc endpoint listens on")
)

func main() {
	flag.Parse()

	getHttp(*loop)
	getGrpc(*loop)
	streamGrpc(*loop)
}

func getHttp(loop int) {

	client := &http.Client{}

	// serial run
	start := time.Now()
	r := 0
	for i := 0; i < loop; i++ {
		r = sendHttpRequest(client)
	}
	log.Printf("Through serial http:\t\t size = %d ;duration = %s", r, time.Since(start))

	// parallel run
	rc := make(chan int, loop)
	start = time.Now()
	for i := 0; i < loop; i++ {
		go func() {
			rc <- sendHttpRequest(client)
		}()
	}
	count := 0
	for r := range rc {
		count++
		if count >= loop {
			// we've received all responses, output time and size
			log.Printf("Through parallel http:\t\t size = %d ;duration = %s", r, time.Since(start))
			return
		}
	}
}

func sendHttpRequest(client *http.Client) int {
	req, _ := http.NewRequest("GET", fmt.Sprintf("http://localhost:%d/http", *http_port), nil)
	resp, err := client.Do(req)
	if err != nil {
		log.Fatalf("could not get object: %v", err)
	}
	bytes, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	return len(bytes)
}

func getGrpc(loop int) {
	conn, err := grpc.Dial(fmt.Sprintf("localhost:%d", *grpc_port), grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewObjectAccessorClient(conn)

	// serial run
	start := time.Now()
	r := 0
	for i := 0; i < loop; i++ {
		r = unaryGrpc(c)
	}
	log.Printf("Through serial unary grpc:\t\t size = %d ;duration = %s", r, time.Since(start))

	// parallel run
	start = time.Now()
	rc := make(chan int, loop)
	for i := 0; i < loop; i++ {
		go func() {
			rc <- unaryGrpc(c)
		}()
	}
	count := 0
	for r := range rc {
		count++
		if count >= loop {
			// we've received all responses, output time and size
			log.Printf("Through parallel unary grpc:\t size = %d ;duration = %s", r, time.Since(start))
			return
		}
	}
}

func unaryGrpc(c pb.ObjectAccessorClient) int {
	r, err := c.GetObject(context.Background(), &pb.ObjectIdentifier{"test", "manual", "dummy"})
	if err != nil {
		log.Fatalf("could not get object: %v", err)
	}
	return len(r.Content)
}

func streamGrpc(loop int) {
	conn, err := grpc.Dial(fmt.Sprintf("localhost:%d", *grpc_port), grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewObjectAccessorClient(conn)
	stream, err := c.GetObjectStream(context.Background())
	if err != nil {
		log.Fatalf("can't get stream client %v", err)
	}

	rc := make(chan int, loop)
	start := time.Now()
	// serial run
	go func() {
		var in *pb.ObjectResponse
		var err error
		for {
			in, err = stream.Recv()
			if err == io.EOF {
				close(rc)
				return
			}
			if err != nil {
				log.Fatalf("Failed to receive object %v", err)
			}
			rc <- len(in.Content)
		}
	}()
	for i := 0; i < loop; i++ {
		if err := stream.Send(&pb.ObjectIdentifier{"test", "manual", "dummy"}); err != nil {
			log.Fatalf("Failed to send id: %v", err)
		}
	}
	stream.CloseSend()
	count := 0
	for r := range rc {
		count++
		if count >= loop {
			// we've received all responses, output time and size
			log.Printf("Through streaming grpc:\t\t size = %d; duration = %s", r, time.Since(start))
		}
	}
}
