package worker

import (
	"context"
	"errors"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"

	"github.com/wong1998/chain-sync/common/clock"
	"github.com/wong1998/chain-sync/database"
	"github.com/wong1998/chain-sync/rpcclient"
)

type Transaction struct {
	BusinessId     string
	BlockNumber    *big.Int
	FromAddress    string
	ToAddress      string
	Hash           string
	TokenAddress   string
	ContractWallet string
	TxType         database.TransactionType
}

type Config struct {
	LoopIntervalMsec uint
	HeaderBufferSize uint
	StartHeight      *big.Int
	Confirmations    uint64
}
type TransactionsChannel struct {
	BlockHeight  uint64
	ChannelId    string
	Transactions []*Transaction
}
type BaseSynchronizer struct {
	loopInterval     time.Duration
	headerBufferSize uint64
	businessChannels chan map[string]*TransactionsChannel
	rpcClient        *rpcclient.WalletChainAccountClient
	blockBatch       *rpcclient.BatchBlock
	database         *database.DB

	headers []rpcclient.BlockHeader
	worker  *clock.LoopFn
}

func (syncer *BaseSynchronizer) Start() error {
	if syncer.worker != nil {
		return errors.New("already started")
	}
	syncer.worker = clock.NewLoopFn(clock.SystemClock, syncer.tick, func() error {
		log.Info("shutting down batch producer")
		close(syncer.businessChannels)
		return nil
	}, syncer.loopInterval)
	return nil
}
func (syncer *BaseSynchronizer) Close() error {
	if syncer.worker == nil {
		return nil
	}
	return syncer.worker.Close()
}

func (syncer *BaseSynchronizer) tick(_ context.Context) {
	//如何缓冲里面还有的话就继续处理
	if len(syncer.headers) > 0 {
		log.Info("retrying previous batch")
	} else {
		newHeaders, err := syncer.blockBatch.NextHeaders(syncer.headerBufferSize)
		if err != nil {
			log.Error("error querying for headers", "err", err)
		} else if len(newHeaders) == 0 {
			log.Warn("no new headers. syncer at head?")
		} else {
			syncer.headers = newHeaders
		}
	}
	err := syncer.processBatch(syncer.headers)
	if err == nil {
		syncer.headers = nil
	}
}

func (syncer *BaseSynchronizer) processBatch(headers []rpcclient.BlockHeader) error {
	if len(headers) == 0 {
		return nil
	}
	businessTxChannel := make(map[string]*TransactionsChannel)
	blockHeaders := make([]database.Blocks, len(headers))
	for i := range headers {
		log.Info("Sync block data", "height", headers[i].Number)
		blockHeaders[i] = database.Blocks{Hash: headers[i].Hash, ParentHash: headers[i].ParentHash, Number: headers[i].Number, Timestamp: headers[i].Timestamp}
		//	获取区块中的交易信息
		txList, err := syncer.rpcClient.GetBlockInfo(headers[i].Number)
		if err != nil {
			log.Error("get block info fail", "err", err)
			return err
		}
		//查出所有business表中的信息
		businessList, err := syncer.database.Business.QueryBusinessList()
		if err != nil {
			log.Error("query business list fail", "err", err)
			return err
		}

		for _, business := range businessList {
			var businessTransactions []*Transaction
			for _, tx := range txList {
				toAddress := common.HexToAddress(tx.To)
				fromAddress := common.HexToAddress(tx.From)
				//查询对应的business分表，看是否存在
				existToAddress, toAddressType := syncer.database.Addresses.AddressExist(business.BusinessUid, &toAddress)
				existFromAddress, FromAddressType := syncer.database.Addresses.AddressExist(business.BusinessUid, &fromAddress)
				//都不存在直接跳过
				if !existToAddress && !existFromAddress {
					continue
				}
				log.Info("Found transaction", "txHash", tx.Hash, "from", fromAddress, "to", toAddress)
				txItem := &Transaction{
					BusinessId:     business.BusinessUid,
					BlockNumber:    headers[i].Number,
					FromAddress:    tx.From,
					ToAddress:      tx.To,
					Hash:           tx.Hash,
					TokenAddress:   tx.TokenAddress,
					ContractWallet: tx.ContractWallet,
					TxType:         database.TxTypeUnKnow,
				}
				/*
				 * If the 'from' address is an external address and the 'to' address is an internal user address, it is a deposit; call the callback interface to notifier the business side.
				 * If the 'from' address is a user address and the 'to' address is a hot wallet address, it is consolidation; call the callback interface to notifier the business side.
				 * If the 'from' address is a hot wallet address and the 'to' address is an external user address, it is a withdrawal; call the callback interface to notifier the business side.
				 * If the 'from' address is a hot wallet address and the 'to' address is a cold wallet address, it is a hot-to-cold transfer; call the callback interface to notifier the business side.
				 * If the 'from' address is a cold wallet address and the 'to' address is a hot wallet address, it is a cold-to-hot transfer; call the callback interface to notifier the business side.
				 */
				if !existFromAddress && (existToAddress && toAddressType == database.AddressTypeEOA) { // 充值
					log.Info("Found deposit transaction", "txHash", tx.Hash, "from", fromAddress, "to", toAddress)
					txItem.TxType = database.TxTypeDeposit
				}

				if (existFromAddress && FromAddressType == database.AddressTypeHot) && !existToAddress { // 提现
					log.Info("Found withdraw transaction", "txHash", tx.Hash, "from", fromAddress, "to", toAddress)
					txItem.TxType = database.TxTypeWithdraw
				}

				if (existFromAddress && FromAddressType == database.AddressTypeEOA) && (existToAddress && toAddressType == database.AddressTypeHot) { // 归集
					log.Info("Found collection transaction", "txHash", tx.Hash, "from", fromAddress, "to", toAddress)
					txItem.TxType = database.TxTypeCollection
				}

				if (existFromAddress && FromAddressType == database.AddressTypeHot) && (existToAddress && toAddressType == database.AddressTypeCold) { // 热转冷
					log.Info("Found hot2cold transaction", "txHash", tx.Hash, "from", fromAddress, "to", toAddress)
					txItem.TxType = database.TxTypeHot2Cold
				}

				if (existFromAddress && FromAddressType == database.AddressTypeCold) && (existToAddress && toAddressType == database.AddressTypeHot) { // 冷转热
					log.Info("Found cold2hot transaction", "txHash", tx.Hash, "from", fromAddress, "to", toAddress)
					txItem.TxType = database.TxTypeCold2Hot
				}
				businessTransactions = append(businessTransactions, txItem)

			}
			if len(businessTransactions) > 0 {
				if businessTxChannel[business.BusinessUid] == nil {
					businessTxChannel[business.BusinessUid] = &TransactionsChannel{
						BlockHeight:  headers[i].Number.Uint64(),
						Transactions: businessTransactions,
					}
				} else {
					//设置最新的高度
					businessTxChannel[business.BusinessUid].BlockHeight = headers[i].Number.Uint64()
					businessTxChannel[business.BusinessUid].Transactions = append(businessTxChannel[business.BusinessUid].Transactions, businessTransactions...)
				}
			}
		}
	}
	//存在的交易的block就入库保存
	if len(blockHeaders) > 0 {
		log.Info("Store block headers success", "totalBlockHeader", len(blockHeaders))
		if err := syncer.database.Blocks.StoreBlockss(blockHeaders); err != nil {
			return err
		}
	}
	log.Info("business tx channel", "businessTxChannel", businessTxChannel, "map length", len(businessTxChannel))
	if len(businessTxChannel) > 0 {
		//通道通知
		syncer.businessChannels <- businessTxChannel
	}

	return nil
}
