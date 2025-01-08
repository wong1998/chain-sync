package databases

type AddressType string

const (
	AddressTypeEOA  AddressType = "eoa"
	AddressTypeHot  AddressType = "hot"
	AddressTypeCold AddressType = "cold"
)
