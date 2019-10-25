package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sync"
	"time"

	api "github.com/synerex/synerex_api"
	nodeapi "github.com/synerex/synerex_nodeapi"
	pbase "github.com/synerex/synerex_proto"
	sxutil "github.com/synerex/synerex_sxutil"
)

var (
	nodesrv         = flag.String("nodesrv", "127.0.0.1:9990", "Node ID Server")
	cluster_id      = flag.Int("cluster_id", 0, "ClusterId for The Synerex Server")
	channel         = flag.Int("channel", 1, "Channel")
	name            = flag.String("name", "SimpleProvider", "Provider Name")
	sxServerAddress string
)

func supplyCallback(clt *sxutil.SXServiceClient, sp *api.Supply) {
	log.Printf("callback [%v]\n", sp)
}

func subscribeSupply(client *sxutil.SXServiceClient) {
	for {
		ctx := context.Background() //
		err := client.SubscribeSupply(ctx, supplyCallback)
		log.Printf("Error:Supply %s\n", err.Error())
		// we need to restart

		time.Sleep(5 * time.Second) // wait 5 seconds to reconnect
		newClt := sxutil.GrpcConnectServer(sxServerAddress)
		if newClt != nil {
			log.Printf("Reconnect server [%s]\n", sxServerAddress)
			client.Client = newClt
		}
	}
}

func main() {
	flag.Parse()
	go sxutil.HandleSigInt()
	sxutil.RegisterDeferFunction(sxutil.UnRegisterNode)

	channelTypes := []uint32{uint32(*channel)}
	sxo := &sxutil.SxServerOpt{
		NodeType:  nodeapi.NodeType_PROVIDER,
		ClusterId: int32(*cluster_id),
		AreaId:    "Default",
	}
	// obtain synerex server address from nodeserv
	srv, err := sxutil.RegisterNode(*nodesrv, *name, channelTypes, sxo)
	if err != nil {
		log.Fatal("Can't register node...")
	}
	log.Printf("Connecting Server [%s]\n", srv)

	wg := sync.WaitGroup{} // for syncing other goroutines
	sxServerAddress = srv
	client := sxutil.GrpcConnectServer(srv)
	argJson := fmt.Sprintf("{Client:Simple}")
	sclient := sxutil.NewSXServiceClient(client, pbase.RIDE_SHARE, argJson)

	wg.Add(1)

	go subscribeSupply(sclient)

	wg.Wait()
	sxutil.CallDeferFunctions() // cleanup!

}

