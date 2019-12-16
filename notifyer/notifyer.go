package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sync"
	"time"
	"math/rand"

	api "github.com/synerex/synerex_api"
	nodeapi "github.com/synerex/synerex_nodeapi"
	pbase "github.com/synerex/synerex_proto"
	sxutil "github.com/synerex/synerex_sxutil"
)

var (
	nodesrv         = flag.String("nodesrv", "127.0.0.1:9990", "Node ID Server")
	cluster_id      = flag.Int("cluster_id", 0, "ClusterId for The Synerex Server")
	channel         = flag.Int("channel", 1, "Channel")
	name            = flag.String("name", "Notifier", "Provider Name")
	sxServerAddress string
	dmMap      map[uint64]*sxutil.DemandOpts
	spMap		map[uint64]*api.Supply
	selection 	bool
	idlist     []uint64
	mu		sync.Mutex
)

func init(){
	idlist = make([]uint64, 0)
	dmMap = make(map[uint64]*sxutil.DemandOpts)
	spMap = make(map[uint64]*api.Supply)
	selection = false
}

// this function waits
func startSelection(clt *sxutil.SXServiceClient,d time.Duration){
	var sid uint64

	for i := 0; i < 5; i++{
		time.Sleep(d / 5)
		log.Printf("waiting... %v",i)
	}
	mu.Lock()
	// とりあえず、最後のProposeを選択する。
	for k, _ := range spMap {
		sid = k
	}
	mu.Unlock()
	log.Printf("Select supply %v", spMap[sid])
	clt.SelectSupply(spMap[sid])
	// we have to cleanup all info.
}

func supplyCallback(clt *sxutil.SXServiceClient, sp *api.Supply) {
	log.Printf("callback [%v]\n", sp)
	// choice is supply for me? or not.

	mu.Lock()
	if clt.IsSupplyTarget(sp, idlist) { //
		// always select Supply
		// this is not good..
		//		clt.SelectSupply(sp)
		// just show the supply Information
		opts :=	dmMap[sp.TargetId]
		log.Printf("Got Supply for %v as '%v'",opts, sp )
		spMap[sp.TargetId] = sp
		// should wait several seconds to find the best proposal.
		// if there is no selection .. lets start
		if !selection {
			selection = true
			go startSelection(clt, time.Second*5)
		}
	}else{
		//		log.Printf("This is not my supply id %v, %v",sp,idlist)
		// do not need to say.
	}
	mu.Unlock()
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

func notifyDemand(sclient *sxutil.SXServiceClient, nm string, js string) {
	opts := &sxutil.DemandOpts{Name: nm, JSON: js}
	mu.Lock()
	id, _ := sclient.NotifyDemand(opts)
	idlist = append(idlist, id) // my supply list
	dmMap[id] = opts            // my supply options
	mu.Unlock()
	log.Printf("Register my demand as id %v, %v",id,idlist)
}

func providerInit() {
	channelTypes := []uint32{uint32(*channel)}
	sxo := &sxutil.SxServerOpt{
		NodeType:  nodeapi.NodeType_PROVIDER,
		ClusterId: int32(*cluster_id),
		AreaId:    "Default",
	}
	// obtain synerex server address from nodeserv
	srv, err := sxutil.RegisterNodeAndProc(*nodesrv, *name, channelTypes, sxo, providerInit)
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

	for {
		notifyDemand(sclient, "Test Supply", "")
		time.Sleep(time.Second * time.Duration(10 + rand.Int()%10))
	}

	wg.Wait()
}

func main() {
	flag.Parse()
	go sxutil.HandleSigInt()
	sxutil.RegisterDeferFunction(sxutil.UnRegisterNode)

	providerInit()

	sxutil.CallDeferFunctions() // cleanup!

}

