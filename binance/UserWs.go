package binance

import (
	"encoding/json"
	"os"
	"strings"
	"time"

	"github.com/crypto-quant/goexpro"
	"github.com/crypto-quant/goexpro/internal/logger"
)

type UserDataWs struct {
	c     *goex.WsConn
	reqId int

	accountInfoCallFn     func(accountInfo *AccountInfo)
	accountPositionCallFn func(accountPosition *AccountPosition)
	balanceUpdateCallFn   func(balanceUpdate *BalanceUpdate)
	orderUpdateCallFn     func(orderUpdate *OrderUpdate)
	listStatusCallFn      func(listStatus *ListStatus)
}

func (s *UserDataWs) AccountInfoCallback(f func(accountInfo *AccountInfo)) {
	s.accountInfoCallFn = f
}

func (s *UserDataWs) AccountPositionCallback(f func(accountPosition *AccountPosition)) {
	s.accountPositionCallFn = f
}

func (s *UserDataWs) BalanceUpdateCallback(f func(balanceUpdate *BalanceUpdate)) {
	s.balanceUpdateCallFn = f
}

func (s *UserDataWs) OrderUpdateCallback(f func(orderUpdateorder *OrderUpdate)) {
	s.orderUpdateCallFn = f
}

func (s *UserDataWs) ListStatusCallback(f func(listStatus *ListStatus)) {
	s.listStatusCallFn = f
}

type AccountInfo struct {
	Stream string `json:"stream"`
	Data   struct {
		CanDeposit       bool    `json:"D"`
		CanTrade         bool    `json:"T"`
		CanWithdraw      bool    `json:"W"`
		EventTime        int64   `json:"E"`
		LastUpdated      int64   `json:"u"`
		BuyerCommission  float64 `json:"b"`
		MakerCommission  float64 `json:"m"`
		SellerCommission float64 `json:"s"`
		TakerCommission  float64 `json:"t"`
		EventType        string  `json:"e"`
		Currencies       []struct {
			Asset     string  `json:"a"`
			Available float64 `json:"f,string"`
			Locked    float64 `json:"l,string"`
		} `json:"B"`
	} `json:"data"`
}

type AccountPosition struct {
	Stream string `json:"stream"`
	Data   struct {
		Currencies []struct {
			Asset     string  `json:"a"`
			Available float64 `json:"f,string"`
			Locked    float64 `json:"l,string"`
		} `json:"B"`
		EventTime   int64  `json:"E"`
		LastUpdated int64  `json:"u"`
		EventType   string `json:"e"`
	} `json:"data"`
}

type BalanceUpdate struct {
	Stream string `json:"stream"`
	Data   struct {
		EventTime    int64   `json:"E"`
		ClearTime    int64   `json:"T"`
		BalanceDelta float64 `json:"d,string"`
		Asset        string  `json:"a"`
		EventType    string  `json:"e"`
	} `json:"data"`
}

type OrderUpdate struct {
	Stream string `json:"stream"`
	Data   struct {
		ClientOrderID                     string  `json:"C"`
		EventTime                         int64   `json:"E"`
		IcebergQuantity                   float64 `json:"F,string"`
		LastExecutedPrice                 float64 `json:"L,string"`
		CommissionAsset                   string  `json:"N"`
		OrderCreationTime                 int64   `json:"O"`
		StopPrice                         float64 `json:"P,string"`
		QuoteOrderQuantity                float64 `json:"Q,string"`
		Side                              string  `json:"S"`
		TransactionTime                   int64   `json:"T"`
		OrderStatus                       string  `json:"X"`
		LastQuoteAssetTransactedQuantity  float64 `json:"Y,string"`
		CumulativeQuoteTransactedQuantity float64 `json:"Z,string"`
		CancelledClientOrderID            string  `json:"c"`
		EventType                         string  `json:"e"`
		TimeInForce                       string  `json:"f"`
		OrderListID                       int64   `json:"g"`
		OrderID                           int64   `json:"i"`
		LastExecutedQuantity              float64 `json:"l,string"`
		IsMaker                           bool    `json:"m"`
		Commission                        float64 `json:"n,string"`
		OrderType                         string  `json:"o"`
		Price                             float64 `json:"p,string"`
		Quantity                          float64 `json:"q,string"`
		RejectionReason                   string  `json:"r"`
		Symbol                            string  `json:"s"`
		TradeID                           int64   `json:"t"`
		IsOnOrderBook                     bool    `json:"w"`
		CurrentExecutionType              string  `json:"x"`
		CumulativeFilledQuantity          float64 `json:"z,string"`
	} `json:"data"`
}

type ListStatus struct {
	Stream string `json:"stream"`
	Data   struct {
		ListClientOrderID string `json:"C"`
		EventTime         int64  `json:"E"`
		ListOrderStatus   string `json:"L"`
		Orders            []struct {
			ClientOrderID string `json:"c"`
			OrderID       int64  `json:"i"`
			Symbol        string `json:"s"`
		} `json:"O"`
		TransactionTime int64  `json:"T"`
		ContingencyType string `json:"c"`
		EventType       string `json:"e"`
		OrderListID     int64  `json:"g"`
		ListStatusType  string `json:"l"`
		RejectionReason string `json:"r"`
		Symbol          string `json:"s"`
	} `json:"data"`
}

func NewUserDataWs(bn *Binance) *UserDataWs {
	listenKey, err := bn.GetListenKey()
	if err != nil {
		logger.Errorf("get listen key failed for %s\n", err)
		return nil
	}
	userDataWs := &UserDataWs{}
	wsBuilder := goex.NewWsBuilder().
		WsUrl("wss://stream.binance.com:9443/stream?streams=" + listenKey).
		ProxyUrl(os.Getenv("HTTPS_PROXY")).
		ProtoHandleFunc(userDataWs.handle).AutoReconnect()
	userDataWs.c = wsBuilder.Build()
	userDataWs.reqId = 1

	go updateListenKeyTimeWorker(bn, listenKey)
	return userDataWs
}

func updateListenKeyTimeWorker(bn *Binance, listenKey string) {
	var (
		timer *time.Timer
		err   error
	)

	timer = time.NewTimer(time.Minute * 5)

	for {
		select {
		case <-timer.C:
			err = bn.KeepAliveListenKey(listenKey)
			if err != nil {
				logger.Errorf("keep alive listen key failed: %s", err)
			}
			timer.Reset(time.Minute * 5)
		}
	}
}

func (s *UserDataWs) handle(respRaw []byte) error {
	var multiStreamData map[string]interface{}
	err := json.Unmarshal(respRaw, &multiStreamData)
	if err != nil {
		logger.Errorf("json unmarshal ws response error [%s] , response data = %s", err, string(respRaw))
		return err
	}

	if method, ok := multiStreamData["method"].(string); ok {
		// TODO handle subscription handling
		if strings.EqualFold(method, "subscribe") {
			return nil
		}
		if strings.EqualFold(method, "unsubscribe") {
			return nil
		}
	}

	if newdata, ok := multiStreamData["data"].(map[string]interface{}); ok {
		if e, ok := newdata["e"].(string); ok {
			switch e {
			case "outboundAccountInfo":
				var data AccountInfo
				err := json.Unmarshal(respRaw, &data)
				if err != nil {
					logger.Errorf("Could not convert to %s structure %s", e, err)
					return err
				}
				if s.accountInfoCallFn != nil {
					s.accountInfoCallFn(&data)
				}
			case "outboundAccountPosition":
				var data AccountPosition
				err := json.Unmarshal(respRaw, &data)
				if err != nil {
					logger.Errorf("Could not convert to %s structure %s", e, err)
					return err
				}
				if s.accountPositionCallFn != nil {
					s.accountPositionCallFn(&data)
				}
			case "balanceUpdate":
				var data BalanceUpdate
				err := json.Unmarshal(respRaw, &data)
				if err != nil {
					logger.Errorf("Could not convert to %s structure %s", e, err)
					return err
				}
				if s.balanceUpdateCallFn != nil {
					s.balanceUpdateCallFn(&data)
				}
			case "executionReport":
				var data OrderUpdate
				err := json.Unmarshal(respRaw, &data)
				if err != nil {
					logger.Errorf("Could not convert to %s structure %s, %s", e, err, string(respRaw))
					return err
				}
				if s.orderUpdateCallFn != nil {
					s.orderUpdateCallFn(&data)
				}
			case "listStatus":
				var data ListStatus
				err := json.Unmarshal(respRaw, &data)
				if err != nil {
					logger.Errorf("Could not convert to %s structure %s", e, err)
					return err
				}
				if s.listStatusCallFn != nil {
					s.listStatusCallFn(&data)
				}
			}
		} else {
			logger.Error("Unknown user data event type\n")
		}
	} else {
		logger.Errorf("Wrong user data\n")
	}
	return nil
}
