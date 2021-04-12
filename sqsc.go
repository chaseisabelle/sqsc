package sqsc

import (
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

// SQSC the client
type SQSC struct {
	sqs    *sqs.SQS
	config Config
}

// Config the client configs
type Config struct {
	ID       string //<< aws account id
	Key      string //<< aws auth key - leave blank for no auth
	Secret   string //<< aws account secret - leave blank for no auth
	Region   string //<< aws region
	Queue    string //<< queue name - not needed if url provided
	URL      string //<< queue url - not needed if queue provided
	Endpoint string //<< aws endpoint
	Retries  int    //<< max retries
	Timeout  int    //<< visibility timeout (seconds)
	Wait     int    //<< wait time (seconds)
}

// New creates a new client instance
func New(cfg *Config) (*SQSC, error) {
	// default is no-auth
	crd := credentials.AnonymousCredentials

	// check if we do need to auth
	if cfg.Key != "" && cfg.Secret != "" {
		crd = credentials.NewStaticCredentials(cfg.Key, cfg.Secret, "")
	}

	// build the aws configs
	acf := aws.Config{
		Region:      aws.String(cfg.Region),
		Credentials: crd,
		MaxRetries:  aws.Int(cfg.Retries),
		Endpoint:    &cfg.Endpoint,
	}

	// boot the session
	ses, err := session.NewSession(&acf)

	// build the aws sqs client
	cli := sqs.New(ses, &acf)

	// get the queue url
	if cfg.URL == "" {
		url, err := cli.GetQueueUrl(&sqs.GetQueueUrlInput{
			QueueName:              aws.String(cfg.Queue),
			QueueOwnerAWSAccountId: aws.String(cfg.ID),
		})

		if err != nil {
			return nil, err
		}

		if url == nil {
			return nil, errors.New("failed to get queue url")
		}

		cfg.URL = *url.QueueUrl
	}

	// build the struct
	return &SQSC{
		sqs:    cli,
		config: *cfg,
	}, err
}

// Produce produce a new message on the queue
//
// 	* bod - the message body
// 	* del - the delay in seconds (usually just use 0)
//
// returns
// 	* the message id
// 	* error
func (c *SQSC) Produce(bod string, del int) (string, error) {
	// send message
	inp := sqs.SendMessageInput{
		MessageBody:  aws.String(bod),
		QueueUrl:     aws.String(c.config.URL),
		DelaySeconds: aws.Int64(int64(del)),
	}

	// send it
	res, err := c.sqs.SendMessage(&inp)

	// default message id
	id := ""

	// we get a response?
	if res != nil {
		// get id pointer
		ptr := res.MessageId

		// can we dereference it?
		if ptr != nil {
			// dereference it
			id = *res.MessageId
		}
	}

	// return the message id
	return id, err
}

// Consume consume a single message from the queue
//
// returns
// 	* the message body
// 	* the receipt handle (use for deleting messages)
// 	* any error
func (c *SQSC) Consume() (string, string, error) {
	// receive messages
	bods, rhs, err := c.Receive(1)

	// prep the results
	bod := ""
	rh := ""

	// check error
	if err != nil {
		return bod, rh, err
	}

	// quick validation
	lb := len(bods)
	lrh := len(rhs)

	if lb != lrh {
		err = fmt.Errorf("body count and receipt handle mismatch: %d != %d", lb, lrh)
	}

	// check if we have any results
	if lb > 0 {
		bod = bods[0]
	}

	if lrh > 0 {
		rh = rhs[0]
	}

	// return the results
	return bod, rh, err
}

// Receive consume multiple messages from the queue
//
// 	* n - the number of messages to consume (max 10)
//
// returns
// 	* the message bodies
// 	* the receipt handles (use for deleting messages)
// 	* any error
func (c *SQSC) Receive(n int64) ([]string, []string, error) {
	// receive message
	res, err := c.sqs.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(c.config.URL),
		VisibilityTimeout:   aws.Int64(int64(c.config.Timeout)),
		WaitTimeSeconds:     aws.Int64(int64(c.config.Wait)),
		MaxNumberOfMessages: aws.Int64(n),
	})

	// check the response
	if res == nil && err == nil {
		err = errors.New("received nil response with no error")
	}

	// prep the results
	var bods []string
	var rhs []string

	// did it work?
	if err != nil {
		return bods, rhs, err
	}

	// build the results
	for _, msg := range res.Messages {
		// get the body pointer first
		ptr := msg.Body

		// make sure we can dereference it
		if ptr == nil {
			err = errors.New("received nil message body")

			break
		}

		// dereference and append it
		bods = append(bods, *ptr)

		// get the receipt handle pointer
		ptr = msg.ReceiptHandle

		// make sure we can dereference it
		if ptr == nil {
			err = errors.New("received nil receipt handle")

			break
		}

		// dereference and append it
		rhs = append(rhs, *ptr)
	}

	// we done fam
	return bods, rhs, err
}

// Delete delete a message from the queue
//
// 	* rh - the receipt handle (from sqsc.Consume())
//
// returns
// 	* the response (will be empty if success)
// 	* any error
func (c *SQSC) Delete(rh string) (string, error) {
	// delete that pesky message
	res, err := c.sqs.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      aws.String(c.config.URL),
		ReceiptHandle: &rh,
	}) // no response returned when success

	// default body
	bod := ""

	// did we get a response
	if res != nil {
		// convert to string
		bod = res.String()
	}

	// we done fam
	return bod, err
}
