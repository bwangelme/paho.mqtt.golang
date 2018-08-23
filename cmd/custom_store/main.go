/*
 * Copyright (c) 2013 IBM Corp.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * Contributors:
 *    Seth Hoenig
 *    Allan Stockdill-Mander
 *    Mike Robertson
 */

package main

import (
	"encoding/gob"
	"fmt"
	"log"
	"os"
	"path"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/eclipse/paho.mqtt.golang/packets"
)

type FileStore struct {
	// Contain nothing
	sync.RWMutex
	opened bool
	dir    string
}

func NewFileStore(dir string) mqtt.Store {
	return &FileStore{
		dir:    dir,
		opened: false,
	}
}

func (store *FileStore) Open() {
	store.Lock()
	defer store.Unlock()
	store.opened = true
}

func (store *FileStore) Put(key string, pkg packets.ControlPacket) {
	store.Lock()
	defer store.Unlock()

	if !store.opened {
		log.Println("Filestore not open")
		return
	}

	filename := path.Join(store.dir, key)
	fd, err := os.Create(filename)
	defer fd.Close()

	if err != nil {
		log.Println(err)
		return
	}

	encoder := gob.NewEncoder(fd)
	encoder.Encode(pkg)
}

func (store *FileStore) Get(key string) packets.ControlPacket {
	store.Lock()
	defer store.Unlock()

	if !store.opened {
		log.Println("Filestore not open")
		return nil
	}

	filename := path.Join(store.dir, key)
	fd, err := os.OpenFile(filename, os.O_RDONLY, 0666)
	defer fd.Close()

	if err != nil {
		log.Println("create error:", err)
		return nil
	}

	// TODO 用gob编码写入的话，解码的时候需要传入类型，但是文件的类型是根据头来指定的
	// 放弃gob编码，首先尝试使用普通字节流将内容写入到文件中去
	var cp = packets.NewControlPacket(packets.Publish)

	decoder := gob.NewDecoder(fd)
	err = decoder.Decode(cp)

	if err != nil {
		log.Println("decoder error:", err)
		return nil
	}

	return cp
}

func (store *FileStore) Del(string) {
	// Do nothing
}

func (store *FileStore) All() []string {
	return nil
}

func (store *FileStore) Close() {
	store.Lock()
	defer store.Unlock()
	if !store.opened {
		log.Println("Trying to close unopend store")
		return
	}
	store.opened = false
}

func (store *FileStore) Reset() {
	// Do Nothing
}

func main() {
	myStore := NewFileStore("/tmp/mqtt-store")

	opts := mqtt.NewClientOptions()
	opts.AddBroker("tcp://vm.bwangel.me:1883")
	opts.SetClientID("custom-store")
	opts.SetStore(myStore)

	var callback mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
		fmt.Printf("TOPIC: %s\n", msg.Topic())
		fmt.Printf("MSG: %s\n", msg.Payload())
	}

	c := mqtt.NewClient(opts)
	if token := c.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	if token := c.Subscribe("/go-mqtt/sample", 0, callback); token.Wait() && token.Error() != nil {
		log.Println(token.Error())
		return
	}

	for i := 0; i < 5; i++ {
		text := fmt.Sprintf("this is msg #%d!", i)
		token := c.Publish("/go-mqtt/sample", 0, false, text)
		token.Wait()
	}

	for i := 1; i < 5; i++ {
		time.Sleep(1 * time.Second)
	}

	c.Disconnect(250)
}
