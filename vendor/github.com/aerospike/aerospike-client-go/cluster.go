// Copyright 2013-2019 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aerospike

import (
	"errors"
	"fmt"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/aerospike/aerospike-client-go/logger"
	"golang.org/x/sync/errgroup"

	. "github.com/aerospike/aerospike-client-go/internal/atomic"
	. "github.com/aerospike/aerospike-client-go/types"
)

// Cluster encapsulates the aerospike cluster nodes and manages
// them.
type Cluster struct {
	// Initial host nodes specified by user.
	seeds *SyncVal //[]*Host

	// All aliases for all nodes in cluster.
	// Only accessed within cluster tend thread.
	aliases *SyncVal //map[Host]*Node

	// Map of active nodes in cluster.
	// Only accessed within cluster tend thread.
	nodesMap *SyncVal //map[string]*Node

	// Active nodes in cluster.
	nodes     *SyncVal              //[]*Node
	stats     map[string]*nodeStats //host => stats
	statsLock sync.Mutex

	// Hints for best node for a partition
	partitionWriteMap    atomic.Value //partitionMap
	partitionUpdateMutex sync.Mutex

	clientPolicy        ClientPolicy
	infoPolicy          InfoPolicy
	connectionThreshold AtomicInt // number of parallel opening connections

	nodeIndex    uint64 // only used via atomic operations
	replicaIndex uint64 // only used via atomic operations

	wgTend      sync.WaitGroup
	tendChannel chan struct{}
	closed      AtomicBool

	// Aerospike v3.6.0+
	supportsFloat, supportsBatchIndex, supportsReplicasAll, supportsGeo *AtomicBool

	// User name in UTF-8 encoded bytes.
	user string

	// Password in hashed format in bytes.
	password *SyncVal // []byte
}

// NewCluster generates a Cluster instance.
func NewCluster(policy *ClientPolicy, hosts []*Host) (*Cluster, error) {
	// Default TLS names when TLS enabled.
	newHosts := make([]*Host, 0, len(hosts))
	if policy.TlsConfig != nil && !policy.TlsConfig.InsecureSkipVerify {
		useClusterName := len(policy.ClusterName) > 0

		for _, host := range hosts {
			nh := *host
			if nh.TLSName == "" {
				if useClusterName {
					nh.TLSName = policy.ClusterName
				} else {
					nh.TLSName = host.Name
				}
			}
			newHosts = append(newHosts, &nh)
		}
		hosts = newHosts
	}

	newCluster := &Cluster{
		clientPolicy: *policy,
		infoPolicy:   InfoPolicy{Timeout: policy.Timeout},
		tendChannel:  make(chan struct{}),

		seeds:    NewSyncVal(hosts),
		aliases:  NewSyncVal(make(map[Host]*Node)),
		nodesMap: NewSyncVal(make(map[string]*Node)),
		nodes:    NewSyncVal([]*Node{}),
		stats:    map[string]*nodeStats{},

		password: NewSyncVal(nil),

		supportsFloat:       NewAtomicBool(false),
		supportsBatchIndex:  NewAtomicBool(false),
		supportsReplicasAll: NewAtomicBool(false),
		supportsGeo:         NewAtomicBool(false),
	}

	newCluster.partitionWriteMap.Store(make(partitionMap))

	// setup auth info for cluster
	if policy.RequiresAuthentication() {
		if policy.AuthMode == AuthModeExternal && policy.TlsConfig == nil {
			return nil, errors.New("External Authentication requires TLS configuration to be set, because it sends clear password on the wire.")
		}

		newCluster.user = policy.User
		hashedPass, err := hashPassword(policy.Password)
		if err != nil {
			return nil, err
		}
		newCluster.password = NewSyncVal(hashedPass)
	}

	// try to seed connections for first use
	err := newCluster.waitTillStabilized()

	// apply policy rules
	if policy.FailIfNotConnected && !newCluster.IsConnected() {
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("Failed to connect to host(s): %v. The network connection(s) to cluster nodes may have timed out, or the cluster may be in a state of flux.", hosts)
	}

	// start up cluster maintenance go routine
	newCluster.wgTend.Add(1)
	go newCluster.clusterBoss(&newCluster.clientPolicy)

	if err == nil {
		Logger.Debug("New cluster initialized and ready to be used...")
	} else {
		Logger.Error("New cluster was not initialized successfully, but the client will keep trying to connect to the database. Error: %s", err.Error())
	}

	return newCluster, err
}

// String implements the stringer interface
func (clstr *Cluster) String() string {
	return fmt.Sprintf("%v", clstr.nodes)
}

// Maintains the cluster on intervals.
// All clean up code for cluster is here as well.
func (clstr *Cluster) clusterBoss(policy *ClientPolicy) {
	Logger.Info("Starting the cluster tend goroutine...")

	defer func() {
		if r := recover(); r != nil {
			Logger.Error("Cluster tend goroutine crashed: %s", debug.Stack())
			go clstr.clusterBoss(&clstr.clientPolicy)
		}
	}()

	defer clstr.wgTend.Done()

	tendInterval := policy.TendInterval
	if tendInterval <= 10*time.Millisecond {
		tendInterval = 10 * time.Millisecond
	}

Loop:
	for {
		select {
		case <-clstr.tendChannel:
			// tend channel closed
			Logger.Debug("Tend channel closed. Shutting down the cluster...")
			break Loop
		case <-time.After(tendInterval):
			tm := time.Now()
			if err := clstr.tend(); err != nil {
				Logger.Warn(err.Error())
			}

			// Tending took longer than requested tend interval.
			// Tending is too slow for the cluster, and may be falling behind scheule.
			if tendDuration := time.Since(tm); tendDuration > clstr.clientPolicy.TendInterval {
				Logger.Warn("Tending took %s, while your requested ClientPolicy.TendInterval is %s. Tends are slower than the interval, and may be falling behind the changes in the cluster.", tendDuration, clstr.clientPolicy.TendInterval)
			}
		}
	}

	// cleanup code goes here
	// close the nodes
	nodeArray := clstr.GetNodes()
	for _, node := range nodeArray {
		node.Close()
	}
}

// AddSeeds adds new hosts to the cluster.
// They will be added to the cluster on next tend call.
func (clstr *Cluster) AddSeeds(hosts []*Host) {
	clstr.seeds.Update(func(val interface{}) (interface{}, error) {
		seeds := val.([]*Host)
		seeds = append(seeds, hosts...)
		return seeds, nil
	})
}

// Updates cluster state
func (clstr *Cluster) tend() error {

	nodes := clstr.GetNodes()
	nodeCountBeforeTend := len(nodes)

	// All node additions/deletions are performed in tend goroutine.
	// If active nodes don't exist, seed cluster.
	if len(nodes) == 0 {
		Logger.Info("No connections available; seeding...")
		if newNodesFound, err := clstr.seedNodes(); !newNodesFound {
			return err
		}

		// refresh nodes list after seeding
		nodes = clstr.GetNodes()
	}

	peers := newPeers(len(nodes)+16, 16)

	floatSupport := true
	batchIndexSupport := true
	geoSupport := true

	for _, node := range nodes {
		// Clear node reference counts.
		node.referenceCount.Set(0)
		node.partitionChanged.Set(false)
		if !node.supportsPeers.Get() {
			peers.usePeers.Set(false)
		}
	}

	wg := sync.WaitGroup{}
	wg.Add(len(nodes))
	for _, node := range nodes {
		go func(node *Node) {
			defer wg.Done()
			if err := node.Refresh(peers); err != nil {
				Logger.Debug("Error occurred while refreshing node: %s", node.String())
			}
		}(node)
	}
	wg.Wait()

	// Refresh peers when necessary.
	if peers.usePeers.Get() && (peers.genChanged.Get() || len(peers.peers()) != nodeCountBeforeTend) {
		// Refresh peers for all nodes that responded the first time even if only one node's peers changed.
		peers.refreshCount.Set(0)

		wg.Add(len(nodes))
		for _, node := range nodes {
			go func(node *Node) {
				defer wg.Done()
				node.refreshPeers(peers)
			}(node)
		}
		wg.Wait()
	}

	var partitionMap partitionMap

	// Use the following function to allocate memory for the partitionMap on demand.
	// This will prevent the allocation when the cluster is stable, and make tend a bit faster.
	pmlock := new(sync.Mutex)
	setPartitionMap := func(l *sync.Mutex) {
		l.Lock()
		defer l.Unlock()
		if partitionMap == nil {
			partitionMap = clstr.getPartitions().clone()
		}
	}

	// find the first host that connects
	for _, _peer := range peers.peers() {
		if clstr.peerExists(peers, _peer.nodeName) {
			// Node already exists. Do not even try to connect to hosts.
			continue
		}

		wg.Add(1)
		go func(__peer *peer) {
			defer wg.Done()
			for _, host := range __peer.hosts {
				// attempt connection to the host
				nv := nodeValidator{}
				if err := nv.validateNode(clstr, host); err != nil {
					Logger.Warn("Add node `%s` failed: `%s`", host, err)
					continue
				}

				// Must look for new node name in the unlikely event that node names do not agree.
				if __peer.nodeName != nv.name {
					Logger.Warn("Peer node `%s` is different than actual node `%s` for host `%s`", __peer.nodeName, nv.name, host)
				}

				if clstr.peerExists(peers, nv.name) {
					// Node already exists. Do not even try to connect to hosts.
					break
				}

				// Create new node.
				node := clstr.createNode(&nv)
				peers.addNode(nv.name, node)
				setPartitionMap(pmlock)
				node.refreshPartitions(peers, partitionMap)
				break
			}
		}(_peer)
	}

	// Refresh partition map when necessary.
	wg.Add(len(nodes))
	for _, node := range nodes {
		go func(node *Node) {
			defer wg.Done()
			if node.partitionChanged.Get() {
				setPartitionMap(pmlock)
				node.refreshPartitions(peers, partitionMap)
			}
		}(node)
	}

	// This waits for the both steps above
	wg.Wait()

	if peers.genChanged.Get() || !peers.usePeers.Get() {
		// Handle nodes changes determined from refreshes.
		removeList := clstr.findNodesToRemove(peers.refreshCount.Get())

		// Remove nodes in a batch.
		if len(removeList) > 0 {
			for _, n := range removeList {
				Logger.Debug("The following nodes will be removed: %s", n)
			}
			clstr.removeNodes(removeList)
		}

		clstr.aggregateNodestats(removeList)
	}

	// Add nodes in a batch.
	if len(peers.nodes()) > 0 {
		clstr.addNodes(peers.nodes())
	}

	if !floatSupport {
		Logger.Warn("Some cluster nodes do not support float type. Disabling native float support in the client library...")
	}

	// set the cluster supported features
	clstr.supportsFloat.Set(floatSupport)
	clstr.supportsBatchIndex.Set(batchIndexSupport)
	clstr.supportsGeo.Set(geoSupport)

	// update all partitions in one go
	updatePartitionMap := false
	for _, node := range clstr.GetNodes() {
		if node.partitionChanged.Get() {
			updatePartitionMap = true
			break
		}
	}

	if updatePartitionMap {
		clstr.setPartitions(partitionMap)
	}

	if err := clstr.getPartitions().validate(); err != nil {
		Logger.Debug("Error validating the cluster partition map after tend: %s", err.Error())
	}

	// only log if node count is changed
	if nodeCountBeforeTend != len(clstr.GetNodes()) {
		Logger.Info("Tend finished. Live node count changes from %d to %d", nodeCountBeforeTend, len(clstr.GetNodes()))
	}

	clstr.aggregateNodestats(clstr.GetNodes())

	return nil
}

func (clstr *Cluster) aggregateNodestats(nodeList []*Node) {
	// update stats
	clstr.statsLock.Lock()
	defer clstr.statsLock.Unlock()

	for _, node := range nodeList {
		h := node.host.String()
		if stats, exists := clstr.stats[h]; exists {
			stats.aggregate(node.stats.getAndReset())
		} else {
			clstr.stats[h] = node.stats.getAndReset()
		}
	}
}

func (clstr *Cluster) statsCopy() map[string]nodeStats {
	clstr.statsLock.Lock()
	defer clstr.statsLock.Unlock()

	res := make(map[string]nodeStats, len(clstr.stats))
	for _, node := range clstr.GetNodes() {
		h := node.host.String()
		if stats, exists := clstr.stats[h]; exists {
			statsCopy := stats.clone()
			statsCopy.ConnectionsOpen = int64(node.connectionCount.Get())
			res[h] = statsCopy
		}
	}

	// stats for nodes which do not exist anymore
	for h, stats := range clstr.stats {
		if _, exists := res[h]; !exists {
			stats.ConnectionsOpen = 0
			res[h] = stats.clone()
		}
	}

	return res
}

func (clstr *Cluster) peerExists(peers *peers, nodeName string) bool {
	node := clstr.findNodeByName(nodeName)
	if node != nil {
		node.referenceCount.IncrementAndGet()
		return true
	}

	node = peers.nodeByName(nodeName)
	if node != nil {
		node.referenceCount.IncrementAndGet()
		return true
	}

	return false
}

// Tend the cluster until it has stabilized and return control.
// This helps avoid initial database request timeout issues when
// a large number of threads are initiated at client startup.
//
// If the cluster has not stabilized by the timeout, return
// control as well.  Do not return an error since future
// database requests may still succeed.
func (clstr *Cluster) waitTillStabilized() error {
	count := -1

	doneCh := make(chan error, 10)

	// will run until the cluster is stabilized
	go func() {
		var err error
		for {
			if err = clstr.tend(); err != nil {
				if aerr, ok := err.(AerospikeError); ok {
					switch aerr.ResultCode() {
					case NOT_AUTHENTICATED, CLUSTER_NAME_MISMATCH_ERROR:
						doneCh <- err
						return
					}
				}
				Logger.Warn(err.Error())
			}

			// // if there are no errors in connecting to the cluster, then validate the partition table
			// if err == nil {
			// 	err = clstr.getPartitions().validate()
			// }

			// Check to see if cluster has changed since the last Tend().
			// If not, assume cluster has stabilized and return.
			if count == len(clstr.GetNodes()) {
				break
			}

			time.Sleep(time.Millisecond)

			count = len(clstr.GetNodes())
		}
		doneCh <- err
	}()

	select {
	case <-time.After(clstr.clientPolicy.Timeout):
		if clstr.clientPolicy.FailIfNotConnected {
			clstr.Close()
		}
		return errors.New("Connecting to the cluster timed out.")
	case err := <-doneCh:
		if err != nil && clstr.clientPolicy.FailIfNotConnected {
			clstr.Close()
		}
		return err
	}
}

func (clstr *Cluster) findAlias(alias *Host) *Node {
	res, _ := clstr.aliases.GetSyncedVia(func(val interface{}) (interface{}, error) {
		aliases := val.(map[Host]*Node)
		return aliases[*alias], nil
	})

	return res.(*Node)
}

func (clstr *Cluster) setPartitions(partMap partitionMap) {
	if err := partMap.validate(); err != nil {
		Logger.Error("Partition map error: %s.", err.Error())
	}

	clstr.partitionWriteMap.Store(partMap)
}

func (clstr *Cluster) getPartitions() partitionMap {
	return clstr.partitionWriteMap.Load().(partitionMap)
}

// Adds seeds to the cluster
func (clstr *Cluster) seedNodes() (bool, error) {
	// Must copy array reference for copy on write semantics to work.
	seedArrayIfc, _ := clstr.seeds.GetSyncedVia(func(val interface{}) (interface{}, error) {
		seeds := val.([]*Host)
		seeds_copy := make([]*Host, len(seeds))
		copy(seeds_copy, seeds)

		return seeds_copy, nil
	})
	seedArray := seedArrayIfc.([]*Host)

	successChan := make(chan struct{}, len(seedArray))
	errChan := make(chan error, len(seedArray))

	Logger.Info("Seeding the cluster. Seeds count: %d", len(seedArray))

	// Add all nodes at once to avoid copying entire array multiple times.
	for i, seed := range seedArray {
		go func(index int, seed *Host) {
			nodesToAdd := make(nodesToAddT, 128)
			nv := nodeValidator{}
			err := nv.seedNodes(clstr, seed, nodesToAdd)
			if err != nil {
				Logger.Warn("Seed %s failed: %s", seed.String(), err.Error())
				errChan <- err
				return
			}
			clstr.addNodes(nodesToAdd)
			successChan <- struct{}{}
		}(i, seed)
	}

	errorList := make([]error, 0, len(seedArray))
	seedCount := len(seedArray)
L:
	for {
		select {
		case err := <-errChan:
			errorList = append(errorList, err)
			seedCount--
			if seedCount <= 0 {
				break L
			}
		case <-successChan:
			// even one seed is enough
			return true, nil
		case <-time.After(clstr.clientPolicy.Timeout):
			// time is up, no seeds found
			break L
		}
	}

	var errStrs []string
	for _, err := range errorList {
		if err != nil {
			if aerr, ok := err.(AerospikeError); ok {
				switch aerr.ResultCode() {
				case NOT_AUTHENTICATED:
					return false, NewAerospikeError(NOT_AUTHENTICATED)
				case CLUSTER_NAME_MISMATCH_ERROR:
					return false, aerr
				}
			}
			errStrs = append(errStrs, err.Error())
		}
	}

	return false, NewAerospikeError(INVALID_NODE_ERROR, "Failed to connect to hosts:"+strings.Join(errStrs, "\n"))
}

func (clstr *Cluster) createNode(nv *nodeValidator) *Node {
	return newNode(clstr, nv)
}

// Finds a node by name in a list of nodes
func (clstr *Cluster) findNodeName(list []*Node, name string) bool {
	for _, node := range list {
		if node.GetName() == name {
			return true
		}
	}
	return false
}

func (clstr *Cluster) addAlias(host *Host, node *Node) {
	if host != nil && node != nil {
		clstr.aliases.Update(func(val interface{}) (interface{}, error) {
			aliases := val.(map[Host]*Node)
			aliases[*host] = node
			return aliases, nil
		})
	}
}

func (clstr *Cluster) removeAlias(alias *Host) {
	if alias != nil {
		clstr.aliases.Update(func(val interface{}) (interface{}, error) {
			aliases := val.(map[Host]*Node)
			delete(aliases, *alias)
			return aliases, nil
		})
	}
}

func (clstr *Cluster) findNodesToRemove(refreshCount int) []*Node {
	nodes := clstr.GetNodes()

	removeList := []*Node{}

	for _, node := range nodes {
		if !node.IsActive() {
			// Inactive nodes must be removed.
			removeList = append(removeList, node)
			continue
		}

		// Single node clusters rely on whether it responded to info requests.
		if refreshCount == 0 && node.failures.Get() >= 5 {
			// All node info requests failed and this node had 5 consecutive failures.
			// Remove node.  If no nodes are left, seeds will be tried in next cluster
			// tend iteration.
			removeList = append(removeList, node)
			continue
		}

		// Two node clusters require at least one successful refresh before removing.
		if len(nodes) > 1 && refreshCount >= 1 && node.referenceCount.Get() == 0 {
			// Node is not referenced by other nodes.
			// Check if node responded to info request.
			if node.failures.Get() == 0 {
				// Node is alive, but not referenced by other nodes.  Check if mapped.
				if !clstr.findNodeInPartitionMap(node) {
					// Node doesn't have any partitions mapped to it.
					// There is no point in keeping it in the cluster.
					removeList = append(removeList, node)
				}
			} else {
				// Node not responding. Remove it.
				removeList = append(removeList, node)
			}
		}
	}

	return removeList
}

func (clstr *Cluster) findNodeInPartitionMap(filter *Node) bool {
	partitionMap := clstr.getPartitions()

	for _, partitions := range partitionMap {
		for _, nodeArray := range partitions.Replicas {
			for _, node := range nodeArray {
				// Use reference equality for performance.
				if node == filter {
					return true
				}
			}
		}
	}
	return false
}

func (clstr *Cluster) addNodes(nodesToAdd map[string]*Node) {
	clstr.nodes.Update(func(val interface{}) (interface{}, error) {
		nodes := val.([]*Node)
		for _, node := range nodesToAdd {
			if node != nil && !clstr.findNodeName(nodes, node.name) {
				Logger.Debug("Adding node %s (%s) to the cluster.", node.name, node.host.String())
				nodes = append(nodes, node)
			}
		}

		nodesMap := make(map[string]*Node, len(nodes))
		nodesAliases := make(map[Host]*Node, len(nodes))
		for i := range nodes {
			nodesMap[nodes[i].name] = nodes[i]

			for _, alias := range nodes[i].GetAliases() {
				nodesAliases[*alias] = nodes[i]
			}
		}

		clstr.nodesMap.Set(nodesMap)
		clstr.aliases.Set(nodesAliases)

		return nodes, nil
	})
}

func (clstr *Cluster) removeNodes(nodesToRemove []*Node) {

	// There is no need to delete nodes from partitionWriteMap because the nodes
	// have already been set to inactive.

	// Cleanup node resources.
	for _, node := range nodesToRemove {
		// Remove node's aliases from cluster alias set.
		// Aliases are only used in tend goroutine, so synchronization is not necessary.
		clstr.aliases.Update(func(val interface{}) (interface{}, error) {
			aliases := val.(map[Host]*Node)
			for _, alias := range node.GetAliases() {
				delete(aliases, *alias)
			}
			return aliases, nil
		})

		clstr.nodesMap.Update(func(val interface{}) (interface{}, error) {
			nodesMap := val.(map[string]*Node)
			delete(nodesMap, node.name)
			return nodesMap, nil
		})

		node.Close()
	}

	// Remove all nodes at once to avoid copying entire array multiple times.
	clstr.nodes.Update(func(val interface{}) (interface{}, error) {
		nodes := val.([]*Node)
		nlist := make([]*Node, 0, len(nodes))
		nlist = append(nlist, nodes...)
		for i, n := range nlist {
			for _, ntr := range nodesToRemove {
				if ntr.Equals(n) {
					nlist[i] = nil
				}
			}
		}

		newNodes := make([]*Node, 0, len(nlist))
		for i := range nlist {
			if nlist[i] != nil {
				newNodes = append(newNodes, nlist[i])
			}
		}

		return newNodes, nil
	})

}

// IsConnected returns true if cluster has nodes and is not already closed.
func (clstr *Cluster) IsConnected() bool {
	// Must copy array reference for copy on write semantics to work.
	nodeArray := clstr.GetNodes()
	return (len(nodeArray) > 0) && !clstr.closed.Get()
}

func (clstr *Cluster) getReadNode(partition *Partition, replica ReplicaPolicy, seq *int) (*Node, error) {
	switch replica {
	case SEQUENCE:
		return clstr.getSequenceNode(partition, seq)
	case MASTER:
		return clstr.getMasterNode(partition)
	case MASTER_PROLES:
		return clstr.getMasterProleNode(partition)
	case PREFER_RACK:
		return clstr.getSameRackNode(partition, seq)
	default:
		// includes case RANDOM:
		return clstr.GetRandomNode()
	}
}

// getSameRackNode returns either a node on the same rack, or in Replica Sequence
func (clstr *Cluster) getSameRackNode(partition *Partition, seq *int) (*Node, error) {
	// RackAware has not been enabled in client policy.
	if !clstr.clientPolicy.RackAware {
		return nil, NewAerospikeError(UNSUPPORTED_FEATURE, "ReplicaPolicy is set to PREFER_RACK but ClientPolicy.RackAware is not set.")
	}

	pmap := clstr.getPartitions()
	partitions := pmap[partition.Namespace]
	if partitions == nil {
		return nil, NewAerospikeError(PARTITION_UNAVAILABLE, "Invalid namespace in partition table:", partition.Namespace)
	}

	// CP mode (Strong Consistency) does not support the RackAware feature.
	if partitions.CPMode {
		return nil, NewAerospikeError(UNSUPPORTED_FEATURE, "ReplicaPolicy is set to PREFER_RACK but the cluster is in Strong Consistency Mode.")
	}

	replicaArray := partitions.Replicas

	var seqNode *Node
	for range replicaArray {
		index := *seq % len(replicaArray)
		node := replicaArray[index][partition.PartitionId]
		*seq++

		if node != nil {
			// assign a node to seqNode in case no node was found on the same rack was found
			if seqNode == nil {
				seqNode = node
			}

			// if the node didn't belong to rack for that namespace, continue
			nodeRack, err := node.Rack(partition.Namespace)
			if err != nil {
				continue
			}

			if node.IsActive() && nodeRack == clstr.clientPolicy.RackId {
				return node, nil
			}
		}
	}

	// if no nodes were found belonging to the same rack, and no other node was also found
	// then the partition table replicas are empty for that namespace
	if seqNode == nil {
		return nil, newInvalidNodeError(len(clstr.GetNodes()), partition)
	}

	return seqNode, nil
}

func (clstr *Cluster) getSequenceNode(partition *Partition, seq *int) (*Node, error) {
	pmap := clstr.getPartitions()
	partitions := pmap[partition.Namespace]
	if partitions == nil {
		return nil, NewAerospikeError(PARTITION_UNAVAILABLE, "Invalid namespace in partition table:", partition.Namespace)
	}

	replicaArray := partitions.Replicas

	if replicaArray != nil {
		index := *seq % len(replicaArray)
		node := replicaArray[index][partition.PartitionId]

		if node != nil && node.IsActive() {
			return node, nil
		}
		*seq++
	}

	return nil, newInvalidNodeError(len(clstr.GetNodes()), partition)
}

func (clstr *Cluster) getMasterNode(partition *Partition) (*Node, error) {
	pmap := clstr.getPartitions()
	partitions := pmap[partition.Namespace]
	if partitions == nil {
		return nil, NewAerospikeError(PARTITION_UNAVAILABLE, "Invalid namespace in partition table:", partition.Namespace)
	}

	node := partitions.Replicas[0][partition.PartitionId]
	if node != nil && node.IsActive() {
		return node, nil
	}

	return nil, newInvalidNodeError(len(clstr.GetNodes()), partition)
}

func (clstr *Cluster) getMasterProleNode(partition *Partition) (*Node, error) {
	pmap := clstr.getPartitions()
	partitions := pmap[partition.Namespace]
	if partitions == nil {
		return nil, NewAerospikeError(PARTITION_UNAVAILABLE, "Invalid namespace in partition table:", partition.Namespace)
	}

	replicaArray := partitions.Replicas

	if replicaArray != nil {
		for range replicaArray {
			index := int(atomic.AddUint64(&clstr.replicaIndex, 1) % uint64(len(replicaArray)))
			node := replicaArray[index][partition.PartitionId]
			if node != nil && node.IsActive() {
				return node, nil
			}
		}
	}

	return nil, newInvalidNodeError(len(clstr.GetNodes()), partition)
}

// GetRandomNode returns a random node on the cluster
func (clstr *Cluster) GetRandomNode() (*Node, error) {
	// Must copy array reference for copy on write semantics to work.
	nodeArray := clstr.GetNodes()
	length := len(nodeArray)
	for i := 0; i < length; i++ {
		// Must handle concurrency with other non-tending goroutines, so nodeIndex is consistent.
		index := int(atomic.AddUint64(&clstr.nodeIndex, 1) % uint64(length))
		node := nodeArray[index]

		if node != nil && node.IsActive() {
			// Logger.Debug("Node `%s` is active. index=%d", node, index)
			return node, nil
		}
	}

	return nil, NewAerospikeError(INVALID_NODE_ERROR, "Cluster is empty.")
}

// GetNodes returns a list of all nodes in the cluster
func (clstr *Cluster) GetNodes() []*Node {
	// Must copy array reference for copy on write semantics to work.
	return clstr.nodes.Get().([]*Node)
}

// GetSeeds returns a list of all seed nodes in the cluster
func (clstr *Cluster) GetSeeds() []Host {
	res, _ := clstr.seeds.GetSyncedVia(func(val interface{}) (interface{}, error) {
		seeds := val.([]*Host)
		res := make([]Host, 0, len(seeds))
		for _, seed := range seeds {
			res = append(res, *seed)
		}

		return res, nil
	})

	return res.([]Host)
}

// GetAliases returns a list of all node aliases in the cluster
func (clstr *Cluster) GetAliases() map[Host]*Node {
	res, _ := clstr.aliases.GetSyncedVia(func(val interface{}) (interface{}, error) {
		aliases := val.(map[Host]*Node)
		res := make(map[Host]*Node, len(aliases))
		for h, n := range aliases {
			res[h] = n
		}

		return res, nil
	})

	return res.(map[Host]*Node)
}

// GetNodeByName finds a node by name and returns an
// error if the node is not found.
func (clstr *Cluster) GetNodeByName(nodeName string) (*Node, error) {
	node := clstr.findNodeByName(nodeName)

	if node == nil {
		return nil, NewAerospikeError(INVALID_NODE_ERROR, "Invalid node name"+nodeName)
	}
	return node, nil
}

func (clstr *Cluster) findNodeByName(nodeName string) *Node {
	// Must copy array reference for copy on write semantics to work.
	for _, node := range clstr.GetNodes() {
		if node.GetName() == nodeName {
			return node
		}
	}
	return nil
}

// Close closes all cached connections to the cluster nodes
// and stops the tend goroutine.
func (clstr *Cluster) Close() {
	if clstr.closed.CompareAndToggle(false) {
		// send close signal to maintenance channel
		close(clstr.tendChannel)

		// wait until tend is over
		clstr.wgTend.Wait()
	}
}

// MigrationInProgress determines if any node in the cluster
// is participating in a data migration
func (clstr *Cluster) MigrationInProgress(timeout time.Duration) (res bool, err error) {
	if timeout <= 0 {
		timeout = _DEFAULT_TIMEOUT
	}

	done := make(chan bool, 1)

	go func() {
		// this function is guaranteed to return after _DEFAULT_TIMEOUT
		nodes := clstr.GetNodes()
		for _, node := range nodes {
			if node.IsActive() {
				if res, err = node.MigrationInProgress(); res || err != nil {
					done <- true
					return
				}
			}
		}

		res, err = false, nil
		done <- false
	}()

	dealine := time.After(timeout)
	for {
		select {
		case <-dealine:
			return false, NewAerospikeError(TIMEOUT)
		case <-done:
			return res, err
		}
	}
}

// WaitUntillMigrationIsFinished will block until all
// migration operations in the cluster all finished.
func (clstr *Cluster) WaitUntillMigrationIsFinished(timeout time.Duration) (err error) {
	if timeout <= 0 {
		timeout = _NO_TIMEOUT
	}
	done := make(chan error, 1)

	go func() {
		// this function is guaranteed to return after timeout
		// no go routines will be leaked
		for {
			if res, err := clstr.MigrationInProgress(timeout); err != nil || !res {
				done <- err
				return
			}
		}
	}()

	dealine := time.After(timeout)
	select {
	case <-dealine:
		return NewAerospikeError(TIMEOUT)
	case err = <-done:
		return err
	}
}

// Password returns the password that is currently used with the cluster.
func (clstr *Cluster) Password() (res []byte) {
	pass := clstr.password.Get()
	if pass != nil {
		return pass.([]byte)
	}
	return nil
}

func (clstr *Cluster) changePassword(user string, password string, hash []byte) {
	// change password ONLY if the user is the same
	if clstr.user == user {
		clstr.clientPolicy.Password = password
		clstr.password.Set(hash)
	}
}

// ClientPolicy returns the client policy that is currently used with the cluster.
func (clstr *Cluster) ClientPolicy() (res ClientPolicy) {
	return clstr.clientPolicy
}

// WarmUp fills the connection pool with connections for all nodes.
// This is necessary on startup for high traffic programs.
// If the count is <= 0, the connection queue will be filled.
// If the count is more than the size of the pool, the pool will be filled.
// Note: One connection per node is reserved for tend operations and is not used for transactions.
func (clstr *Cluster) WarmUp(count int) (int, error) {
	var g errgroup.Group
	cnt := NewAtomicInt(0)
	nodes := clstr.GetNodes()
	for i := range nodes {
		node := nodes[i]
		g.Go(func() error {
			n, err := node.WarmUp(count)
			cnt.AddAndGet(n)

			return err
		})
	}

	err := g.Wait()
	return cnt.Get(), err
}
