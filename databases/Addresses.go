package databases

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type Addresses struct {
	GUID uuid.UUID `gorm:"primary_key" json:"guid"`
	//公钥
	PublicKey string `gorm:"type:varchar;not null" json:"public_key"`
	//以太坊地址为20 byte
	Address common.Address `gorm:"type:varchar;unique;not null;serializer:bytes" json:"address"`
	//eoa 代表是地址是账户地址，而不是合约地址
	AddressType AddressType `gorm:"type:varchar(10);not null;default:'eoa'" json:"address_type"`
	Timestamp   uint64      `gorm:"type:bigint;not null;check:timestamp > 0" json:"timestamp"`
}

type AddressesView interface {
	AddressExist(requestId string, address *common.Address) (bool, AddressType)
	QueryAddressesByToAddress(string, *common.Address) (*Addresses, error)
	QueryHotWalletInfo(string) (*Addresses, error)
	QueryColdWalletInfo(string) (*Addresses, error)
	GetAllAddresses(string) ([]*Addresses, error)
}
