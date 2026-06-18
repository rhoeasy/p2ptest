package types

import "errors"

var (
	ErrNoValidNodeAddress = errors.New("no valid node address")
	ErrInvalidPeerInfo    = errors.New("invalid peer info")
	ErrPeerNotFound       = errors.New("peer not found")
)
