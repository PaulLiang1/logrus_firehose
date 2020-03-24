package logrus_firehose

import (
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/aws/aws-sdk-go/service/firehose/firehoseiface"
	"github.com/sirupsen/logrus"
)

var DefaultLevels = []logrus.Level{
	logrus.PanicLevel,
	logrus.FatalLevel,
	logrus.ErrorLevel,
	logrus.WarnLevel,
	logrus.InfoLevel,
}

type Option func(*FirehoseHook)

/*
	firehose batch put request can send up to 500 events
*/
const firehoseMaxBatchSize = 500

// FirehoseHook is logrus hook for AWS Firehose.
// Amazon Kinesis Firehose is a fully-managed service that delivers real-time
// streaming data to destinations such as Amazon Simple Storage Service (Amazon
// S3), Amazon Elasticsearch Service (Amazon ES), and Amazon Redshift.
type FirehoseHook struct {

	/*
		firehose client
	*/
	client firehoseiface.FirehoseAPI

	/*
		firehose stream name to write to
	*/
	streamName string

	/*
		levels being hooked
	*/
	levels []logrus.Level

	/*
		should append new line in each msg
	*/
	addNewline bool

	/*
		instance of logrus.Formatter
	*/
	formatter logrus.Formatter

	/*
		mutex to guard formatter
	*/
	mu *sync.Mutex

	/*
		blockingMode specify if the queue is not empty
		should any write being blocked or discard
	*/
	blockingMode bool

	/*
		specify nb of msgs to be send as firehose batch
	*/
	sendBatchSize int

	/*
		async queue used to
	*/
	sendQueue chan *firehose.Record

	/*
		a logger for error operation
		DO NOT use the same logger being hook onto
	*/
	logger logrus.FieldLogger

	/*
		nb of worker to send firehose event
	*/
	numWorker int
}

// NewFirehoseHook returns initialized logrus hook for Firehose with persistent Firehose logger.
func NewFirehoseHook(name string, client firehoseiface.FirehoseAPI, opts ...Option) (*FirehoseHook, error) {
	hk := &FirehoseHook{
		client:        client,
		streamName:    name,
		levels:        DefaultLevels,
		sendBatchSize: firehoseMaxBatchSize,
		sendQueue:     make(chan *firehose.Record, firehoseMaxBatchSize),
		numWorker:     1,
		blockingMode:  false,
		addNewline:    false,
		mu:            &sync.Mutex{},
	}
	for _, opt := range opts {
		opt(hk)
	}
	return hk, nil
}

// WithLevels sets logging level to fire this hook.
func WithLevels(levels []logrus.Level) Option {
	return func(hook *FirehoseHook) {
		hook.levels = levels
	}
}

// WithAddNewline sets if a newline is added to each message.
func WithAddNewLine() Option {
	return func(hook *FirehoseHook) {
		hook.addNewline = true
	}
}

// WithFormatter sets a log entry formatter
func WithFormatter(f logrus.Formatter) Option {
	return func(hook *FirehoseHook) {
		hook.formatter = f
	}
}

// WithLogger set a logger
// DO NOT use the same logger where it's being hooked on
func WithLogger(l logrus.FieldLogger) Option {
	return func(hook *FirehoseHook) {
		hook.logger = l
	}
}

func WithBlockingMode(mode bool) Option {
	return func(hook *FirehoseHook) {
		hook.blockingMode = mode
	}
}

func WithSendBatchSize(size int) Option {
	if size > firehoseMaxBatchSize || size < 1 {
		panic("invalid batch size specified")
	}
	return func(hook *FirehoseHook) {
		hook.sendBatchSize = size
		hook.sendQueue = make(chan *firehose.Record, size)
	}
}

func WithNumWoker(num int) Option {
	if num <= 0 {
		panic("num can not be <= 0")
	}
	return func(hook *FirehoseHook) {
		hook.numWorker = num
	}
}

var newLine = []byte("\n")

/*
	formatEntry formats the log entry.
	this method is not concurrent safe
*/
func (h *FirehoseHook) formatEntry(f logrus.Formatter, entry *logrus.Entry) []byte {
	bytes, err := f.Format(entry)
	if err != nil {
		return nil
	}
	if h.addNewline {
		bytes = append(bytes, newLine...)
	}
	return bytes
}

// Levels returns logging level to fire this hook.
func (h *FirehoseHook) Levels() []logrus.Level {
	return h.levels
}

// Fire is invoked by logrus and sends log to Firehose.
func (h *FirehoseHook) Fire(entry *logrus.Entry) error {
	h.mu.Lock()
	fmtEntry, err := h.formatter.Format(entry)
	h.mu.Unlock()
	if err != nil {
		return err
	}

	for {
		select {
		case h.sendQueue <- &firehose.Record{Data: fmtEntry}:
			return nil
		default:
			if !h.blockingMode {
				if h.logger != nil {
					h.logger.Warn("queue is full and non-blocking mode specified, dropping record")
				}
				return nil
			}
		}
	}
}

func (h *FirehoseHook) SendLoop(tick <-chan time.Time) {
	wg := sync.WaitGroup{}

	for i := 0; i < h.numWorker; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for {
				buf := make([]*firehose.Record, 0, h.sendBatchSize)

				select {
				case <-tick:
					break
				case entry := <-h.sendQueue:
					buf = append(buf, entry)
					if len(buf) >= h.sendBatchSize {
						break
					}
				default:
					if len(buf) >= h.sendBatchSize {
						break
					}
				}
				if len(buf) == 0 {
					continue
				}
				resp, err := h.client.PutRecordBatch(
					&firehose.PutRecordBatchInput{
						DeliveryStreamName: aws.String(h.streamName),
						Records:            buf,
					},
				)
				if err == nil && *resp.FailedPutCount == 0 {
					if h.logger != nil {
						h.logger.WithField("lines-emitted", len(resp.RequestResponses)).
							Debug("log successfully emitted")
					}
					continue
				}
				if h.logger != nil {
					h.logger.WithError(err).
						WithField("failed-rec-count", *resp.FailedPutCount).
						Warn("failed to send logs to firehose")
				}
			}
		}()
	}

	wg.Wait()
}

var _ logrus.Hook = (*FirehoseHook)(nil)
