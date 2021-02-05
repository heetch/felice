module github.com/heetch/felice/v2

go 1.15

require (
	github.com/Shopify/sarama v1.27.2
	github.com/avast/retry-go v3.0.0+incompatible // indirect
	github.com/burdiyan/kafkautil v0.0.0-20190131162249-eaf83ed22d5b
	github.com/frankban/quicktest v1.11.3
	github.com/google/go-cmp v0.5.4
	github.com/heetch/kafkatest v1.1.0
	github.com/lovoo/goka v1.0.5 // indirect
	github.com/pkg/errors v0.9.1
	github.com/rogpeppe/fastuuid v1.2.0
	github.com/stretchr/testify v1.7.0
)

// replace github.com/Shopify/sarama => /home/rog/src/go/src/github.com/Shopify/sarama
