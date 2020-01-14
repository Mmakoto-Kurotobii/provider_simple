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
	name            = flag.String("name", "Proposer", "Provider Name")
	sxServerAddress string
	idlist     []uint64
	spMap      map[uint64]*sxutil.SupplyOpts
	mu		sync.Mutex
)

func init(){
	idlist = make([]uint64, 0)
	spMap = make(map[uint64]*sxutil.SupplyOpts)
}

func demandCallback(clt *sxutil.SXServiceClient, dm *api.Demand) {
	log.Printf("callback [%v]\n", dm)

	if dm.TargetId != 0 { // this is Select!
		log.Println("getSelect!")

		clt.Confirm(sxutil.IDType(dm.GetId()))

	}else { // not select
		// select any ride share demand!
		// should check the type of ride..

		sp := &sxutil.SupplyOpts{
			Target: dm.GetId(),
			Name: "Test Supply",
			JSON: ``,
		}

		mu.Lock()
		pid := clt.ProposeSupply(sp)
		idlist = append(idlist,pid)
		spMap[pid] = sp
		mu.Unlock()
	}
}

func subscribeDemand(client *sxutil.SXServiceClient) {
	for {
		ctx := context.Background() //
		err := client.SubscribeDemand(ctx, demandCallback)
		log.Printf("Error:Demand %s\n", err.Error())
		// we need to restart

		time.Sleep(5 * time.Second) // wait 5 seconds to reconnect
		newClt := sxutil.GrpcConnectServer(sxServerAddress)
		if newClt != nil {
			log.Printf("Reconnect server [%s]\n", sxServerAddress)
			client.Client = newClt
		}
	}
}

func providerInit(command nodeapi.KeepAliveCommand, ret string) {
	channelTypes := []uint32{uint32(*channel)}
	sxo := &sxutil.SxServerOpt{
		NodeType:  nodeapi.NodeType_PROVIDER,
		ClusterId: int32(*cluster_id),
		AreaId:    "Default",
	}
	// obtain synerex server address from nodeserv
	srv, err := sxutil.RegisterNodeWithCmd(*nodesrv, *name, channelTypes, sxo, providerInit)
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

	go subscribeDemand(sclient)

	wg.Wait()
}

func main() {
	flag.Parse()
	go sxutil.HandleSigInt()
	sxutil.RegisterDeferFunction(sxutil.UnRegisterNode)

	providerInit(nodeapi.KeepAliveCommand_NONE, "")

	sxutil.CallDeferFunctions() // cleanup!

}

