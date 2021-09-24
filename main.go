package main

import (
	"flag"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-dax-go/dax"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/sirupsen/logrus"
)

func Int8Ptr(n int64) *int64                     { return &n }
func Int16Ptr(n int16) *int16                    { return &n }
func Int32Ptr(n int32) *int32                    { return &n }
func Int64Ptr(n int64) *int64                    { return &n }
func IntPtr(n int) *int                          { return &n }
func Uint8Ptr(n uint8) *uint8                    { return &n }
func Uint16Ptr(n uint16) *uint16                 { return &n }
func Uint32Ptr(n uint32) *uint32                 { return &n }
func Uint64Ptr(n uint64) *uint64                 { return &n }
func UintPtr(n uint) *uint                       { return &n }
func Float64Ptr(f float64) *float64              { return &f }
func Float32Ptr(f float32) *float32              { return &f }
func BytePtr(b byte) *byte                       { return &b }
func BoolPtr(b bool) *bool                       { return &b }
func RunePtr(r rune) *rune                       { return &r }
func StringPtr(s string) *string                 { return &s }
func DurationPtr(d time.Duration) *time.Duration { return &d }
func TimePtr(t time.Time) *time.Time             { return &t }

var flagAddr = flag.String("addr", "dax://keto.e58abl.dax-clusters.us-west-2.amazonaws.com", "dax address")
var flagOrigin = flag.String("origin", "us-west-2", "dax aws origin")
var flagAccounts = flag.Int("accounts", 100, "account number,routine number")
var flagOrders = flag.Int("orders", 1000, "order number per account")

func main() {
	flag.Parse()
	addresses := strings.Split(*flagAddr, ",")
	region := *flagOrigin

	cfg := dax.DefaultConfig()
	cfg.HostPorts = addresses
	cfg.Region = region
	client, err := dax.New(cfg)
	if err != nil {
		logrus.WithError(err).Errorln("dax.New")
		panic(err)
	}

	total := int64(0)
	finished := int64(0)
	limited := int64(0)
	wg := sync.WaitGroup{}
	wg.Add(*flagAccounts)
	since := time.Now()

	var deltaMutex sync.Mutex
	delta := int64(0)
	deltaFrom := time.Now()

	// 测试多账号，并行写入订单的吞吐量
	for acc := 0; acc < *flagAccounts; acc++ {
		acc := acc
		// 每个账号并行写入
		go func() {
			accountSince := time.Now()
			defer func() {
				wg.Done()
				atomic.AddInt64(&finished, 1)

				// 本账号结束统计
				speed := 1000 * int64(*flagOrders) / time.Since(accountSince).Milliseconds()
				logrus.WithFields(logrus.Fields{
					"accountId":      acc,
					"takeTime":       time.Since(accountSince),
					"speed(items/s)": speed,
				}).Infoln("AccountFinished")
			}()

			// 账号ID
			accountId := (10032 << 40) | acc
			// 账号订单自增
			orderInc := 0
			for orderInc < *flagOrders {
				batchData := make([]*dynamodb.WriteRequest, 0, 25)
				// 每个批量25条
				for j := 0; j < 25; j++ {
					// 总数统计
					tt := atomic.AddInt64(&total, 1)

					func() {
						deltaMutex.Lock()
						defer deltaMutex.Unlock()
						delta += 1
					}()

					if tt%1000 == 0 {
						// 定时总量统计
						func() {
							deltaMutex.Lock()
							defer deltaMutex.Unlock()
							speed := 1000 * delta / time.Since(deltaFrom).Milliseconds()
							delta = 0
							deltaFrom = time.Now()
							logrus.WithField("orderInc", total).
								WithField("totalFinished", finished).
								WithField("speed", fmt.Sprintf("%d items/s", speed)).
								WithField("limited", limited).
								Infoln("PrintOrderInc")
						}()
					}

					orderId := (acc << 40) | orderInc
					orderInc++
					putItem := &dynamodb.WriteRequest{
						PutRequest: &dynamodb.PutRequest{
							Item: map[string]*dynamodb.AttributeValue{
								"accountId":        {N: StringPtr(fmt.Sprintf("%d", accountId))},
								"orderId":          {N: StringPtr(fmt.Sprintf("%d", orderId))},
								"originQty":        {N: StringPtr("0.0005")},
								"originPrice":      {N: StringPtr("4000.01")},
								"orderType":        {S: StringPtr("LIMIT")},
								"side":             {S: StringPtr("SELL")},
								"symbol":           {S: StringPtr("BTC_USDT")},
								"state":            {S: StringPtr("FILLED")},
								"executedQty":      {N: StringPtr("0.000500000000000000")},
								"filledAmount":     {N: StringPtr("2.000005000000000000")},
								"fee":              {N: StringPtr("0.001000002500000000")},
								"updatedTime":      {N: StringPtr("1591172701939000000")},
								"createdTime":      {N: StringPtr("1590214105385175747")},
								"sn":               {N: StringPtr("12063")},
								"clientOrderId":    {N: StringPtr("0")},
								"strategyId":       {N: StringPtr("0")},
								"strategyTag":      {N: StringPtr("0")},
								"submitCancelTime": {N: StringPtr("0")},
								"strategy_order":   {S: StringPtr(fmt.Sprintf("%014d_%018d", 0, orderId))},
								"symbol_order":     {S: StringPtr(fmt.Sprintf("%s_%018d", "BTC_USDT", orderId))},
								"ttl":              {N: StringPtr(fmt.Sprintf("%d", time.Now().Add(time.Minute*5).Unix()))},
							},
						},
					}
					batchData = append(batchData, putItem)
				}

				var input dynamodb.BatchWriteItemInput
				input.RequestItems = map[string][]*dynamodb.WriteRequest{
					"account_orders_test1": batchData,
				}

				// 重试直到成功
				for {
					_, err := client.BatchWriteItem(&input)
					if err != nil {
						cerr, ok := err.(awserr.Error)
						if ok {
							if cerr.Code() == "ProvisionedThroughputExceededException" {
								atomic.AddInt64(&limited, 1)
								continue
							}
						}
						logrus.WithError(err).Errorln("client.PutItem")
						return
					}
					break
				}
			}
		}()
	}

	wg.Wait()
	logrus.WithField("takeTime", time.Since(since)).Println("output")
}
