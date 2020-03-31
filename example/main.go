package main

import (
	"flag"
	"fmt"
	"github.com/chaseisabelle/sqsc"
)

func main() {
	bod := flag.String("message", "bla bla bla", "the message body")
	reg := flag.String("region", "", "aws region")
	url := flag.String("url", "", "queue url")
	ep := flag.String("endpoint", "", "aws endpoint")
	id := flag.String("id", "", "aws account id")
	key := flag.String("key", "", "aws account secret")
	sec := flag.String("secret", "", "aws account secret")
	q := flag.String("queue", "", "queue name")
	ret := flag.Int("retries", 0, "max retries")
	to := flag.Int("timeout", 0, "timeout")
	wt := flag.Int("wait", 0, "wait time")

	flag.Parse()

	fmt.Printf("body: %+v\n", *bod)

	cfg := sqsc.Config{
		Region:   *reg,
		URL:      *url,
		Endpoint: *ep,
		ID:       *id,
		Key:      *key,
		Secret:   *sec,
		Queue:    *q,
		Retries:  *ret,
		Timeout:  *to,
		Wait:     *wt,
	}

	cli, err := sqsc.New(&cfg)

	fmt.Printf("client: %+v\n", cli)
	fmt.Printf("error:  %+v\n", err)

	res, err := cli.Produce(*bod, 0)

	fmt.Printf("produce response: %+v\n", res)
	fmt.Printf("produce error:    %+v\n", err)

	res, rh, err := cli.Consume()

	fmt.Printf("consume response:       %+v\n", res)
	fmt.Printf("consume receipt handle: %+v\n", rh)
	fmt.Printf("consume error:          %+v\n", err)

	res, err = cli.Delete(rh)

	fmt.Printf("delete response: %+v\n", res)
	fmt.Printf("delete error:    %+v\n", err)
}
