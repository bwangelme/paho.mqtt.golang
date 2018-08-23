package main

import (
	"testing"
	"github.com/eclipse/paho.mqtt.golang/packets"
)

func TestStorePut(t *testing.T) {
	mystore := NewFileStore("/tmp/mqtt-store")
	key := "unique_file"

	mystore.Open()
	cp := packets.NewControlPacket(packets.Publish)
	mystore.Put(key, cp)

	data := mystore.Get(key)

	if data.String() != cp.String() {
		t.Fatalf("assert error %s != %s", data.String(), cp.String())
	}
}
