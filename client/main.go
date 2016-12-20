package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
	"time"

	"golang.org/x/net/context"

	pb "github.com/bc-vincent-zhao/grpc_tryout/object"
	"google.golang.org/grpc"
)

var (
	loop      = flag.Int("loop", 50, "loop count for reading object")
	protocol  = flag.String("protocol", "all", "the protocol to use, http|grpc|all")
	http_port = flag.Int("http_port", 10001, "the port http endpoint listens on")
	grpc_port = flag.Int("grpc_port", 10002, "the port grpc endpoint listens on")
)

func main() {
	if *protocol == "grpc" {
		getGrpc(*loop)
	} else if *protocol == "http" {
		getHttp(*loop)
	} else {
		getHttp(*loop)
		getGrpc(*loop)
	}
}

func getHttp(loop int) {
	start := time.Now()
	client := &http.Client{}
	var bytes []byte
	wg := &sync.WaitGroup{}
	for i := 0; i < loop; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req, _ := http.NewRequest("GET", fmt.Sprintf("http://localhost:%d/http", *http_port), nil)
			resp, err := client.Do(req)
			if err != nil {
				log.Printf("could not get object: %v", err)
			} else {
				bytes, _ = ioutil.ReadAll(resp.Body)
				resp.Body.Close()
				//log.Printf("got object: %v", string(bytes))
			}
		}()
	}
	wg.Wait()
	duration := time.Since(start)
	log.Printf("Through http: size = %v ;duration = %v", len(bytes), duration)
}

func getGrpc(loop int) {
	start := time.Now()
	conn, err := grpc.Dial(fmt.Sprintf("localhost:%d", *grpc_port), grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewObjectAccessorClient(conn)

	var r *pb.ObjectResponse
	wg := &sync.WaitGroup{}
	for i := 0; i < loop; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err = c.GetObject(context.Background(), &pb.ObjectIdentifier{"test", "manual", "large"})
			if err != nil {
				log.Printf("could not get object: %v", err)
			} else {
				_ = r.Content
				//log.Printf("got object: %v", r)
			}
		}()
	}
	wg.Wait()
	duration := time.Since(start)
	log.Printf("Through grpc, size = %v; duration = %v", len(r.Content), duration)
}
