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

/*----------------------------------------------------------------------
This sample is designed to demonstrate the ability to set individual
callbacks on a per-subscription basis. There are three handlers in use:
 brokerLoadHandler -        $SYS/broker/load/#
 brokerConnectionHandler -  $SYS/broker/connection/#
 brokerClientHandler -      $SYS/broker/clients/#
The client will receive 100 messages total from those subscriptions,
and then print the total number of messages received from each.
It may take a few moments for the sample to complete running, as it
must wait for messages to be published.
-----------------------------------------------------------------------*/

package main

import (
	"fmt"
	"log"

	MQTT "github.com/eclipse/paho.mqtt.golang"
)

var clientsCount = make(chan bool)
var sessionsCount = make(chan bool)
var topicsCount = make(chan bool)

func clientsCountHandler(client MQTT.Client, msg MQTT.Message) {
	clientsCount <- true
	fmt.Printf("ClientsCountHandler         ")
	fmt.Printf("[%s]  ", msg.Topic())
	fmt.Printf("%s\n", msg.Payload())
}

func sessionsCountHandler(client MQTT.Client, msg MQTT.Message) {
	sessionsCount <- true
	fmt.Printf("SessionsCountHandler   ")
	fmt.Printf("[%s]  ", msg.Topic())
	fmt.Printf("%s\n", msg.Payload())
}

func topicsCountHandler(client MQTT.Client, msg MQTT.Message) {
	topicsCount <- true
	fmt.Printf("TopicsCountHandler      ")
	fmt.Printf("[%s]  ", msg.Topic())
	fmt.Printf("%s\n", msg.Payload())
}

func main() {
	opts := MQTT.NewClientOptions()
	opts.AddBroker("tcp://vm.bwangel.me:1883")
	opts.SetClientID("router-sample")
	opts.SetUsername("paho")
	opts.SetPassword("passwd")
	opts.SetCleanSession(true)
	node := "emq@127.0.0.1"

	c := MQTT.NewClient(opts)
	if token := c.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalln(token.Error())
	}

	if token := c.Subscribe(fmt.Sprintf("$SYS/brokers/%s/stats/clients/count", node), 0, clientsCountHandler); token.Wait() && token.Error() != nil {
		log.Fatalln(token.Error())
	}

	if token := c.Subscribe(fmt.Sprintf("$SYS/brokers/%s/stats/sessions/count", node), 0, sessionsCountHandler); token.Wait() && token.Error() != nil {
		log.Fatalln(token.Error())
	}

	if token := c.Subscribe(fmt.Sprintf("$SYS/brokers/%s/stats/topics/count", node), 0, topicsCountHandler); token.Wait() && token.Error() != nil {
		log.Fatalln(token.Error())
	}

	clientsCountStat := 0
	sessionsCountStat := 0
	topicsCountStat := 0

	fmt.Println("Start to listen")
	for i := 0; i < 5; i++ {
		select {
		case <-clientsCount:
			clientsCountStat++
		case <-sessionsCount:
			sessionsCountStat++
		case <-topicsCount:
			topicsCountStat++
		}
	}

	fmt.Printf("Received %3d clientsCount messages\n", clientsCountStat)
	fmt.Printf("Received %3d sessionsCount messages\n", sessionsCountStat)
	fmt.Printf("Received %3d topicsCount messages\n", topicsCountStat)

	c.Disconnect(250)
}
