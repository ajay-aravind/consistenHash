using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsistentHash
{
    public class Ring
    {
	    int ringLength=1000;
        internal List<Server> servers = new List<Server>();
        Dictionary<String, List<String>> PhysicalServerToVirtualServersMap = new Dictionary<string, List<string>>();
        LinkedList<KeyValuePair<String,String>> virtualServers = new LinkedList<KeyValuePair<String, String>>();
        const int VirtualServerPerServer = 5;
        public Dictionary<String, int> requestCountPerServer = new Dictionary<string, int>();

        // todo
        // Handler cases where there is a server already at generated index
        internal Boolean AddServer(string id)
        {
            try
            {
                //print server id
                Console.WriteLine("Server: " + id);
                Server server = new Server(id, VirtualServerPerServer);
                servers.Add(server);
                server.GenerateVirtualServers();
                PhysicalServerToVirtualServersMap.Add(server.Id, server.PhysicalServerToVirtualServersMap);

                // Add dummy 1000 virtual servers
                for (int j = 0; j < ringLength; j++)
                {
                    virtualServers.AddLast(KeyValuePair.Create("", ""));
                }

                int i = 0;

                foreach (string vServer in server.PhysicalServerToVirtualServersMap)
                {
                    uint hash = sdbmHash(vServer);
                    //print hash
                    Console.WriteLine("Hash: " + hash);
                    int index = (int)(hash % ringLength);
                    Console.WriteLine("Hash index: " + index);

                    if (index < virtualServers.Count)
                    {
                        LinkedListNode<KeyValuePair<String, String>> linkedListNode = GetNodeAtIndex(index, virtualServers);
                        //print linkedListNode
                        Console.WriteLine("LinkedListNode: " + linkedListNode.Value);
                        var newServerNode = KeyValuePair.Create(server.PhysicalServerToVirtualServersMap[i], server.Id);
                        virtualServers.AddBefore(linkedListNode, newServerNode);
                        virtualServers.Remove(linkedListNode);
                    }
                    else
                    {
                        throw new Exception("Index out of bounds");
                    }

                    int k = 0;
                    // print all virtual servers if key is not empty
                    foreach (KeyValuePair<String, String> virtualServer in virtualServers)
                    {
                        if (virtualServer.Key != "")
                        {
                            Console.WriteLine("index:"+ k + "   Virtual Server: " + virtualServer.Key + " Server: " + virtualServer.Value);
                        }
                        k++;
                    }

                    i++;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Error while adding server: " + e.Message);
                return false;
            }

            return true;
        }

        internal Boolean RemoveServer(string id)
        {
            try
            {
                List<String> virtualServers = PhysicalServerToVirtualServersMap[id];
                foreach (string virtualServer in virtualServers)
                {
                    uint hash = sdbmHash(virtualServer);
                    //print hash
                    Console.WriteLine("Hash: " + hash);
                    int index = (int)(hash % ringLength);
                    Console.WriteLine("Hash index: " + index);
                    LinkedListNode<KeyValuePair<String, String>> linkedListNode = GetNodeAtIndex(index, this.virtualServers);
                    //print linkedListNode
                    Console.WriteLine("LinkedListNode: " + linkedListNode.Value);
                    if (linkedListNode.Value.Key == virtualServer)
                    {
                        //print linkedListNode
                        Console.WriteLine("LinkedListNode: " + linkedListNode.Value);
                        this.virtualServers.AddBefore(linkedListNode, KeyValuePair.Create("", ""));
                        this.virtualServers.Remove(linkedListNode);
                    }
                }
                PhysicalServerToVirtualServersMap.Remove(id);
                this.servers.RemoveAll(server => server.Id == id);

                // print physical server to virtual servers map
                foreach (KeyValuePair<String, List<String>> entry in PhysicalServerToVirtualServersMap)
                {
                    Console.WriteLine("Server: " + entry.Key);
                    foreach (string virtualServer in entry.Value)
                    {
                        Console.WriteLine("Virtual Server: " + virtualServer);
                    }
                }

                // print all virtual servers if key is not empty
                int k = 0;
                foreach (KeyValuePair<String, String> virtualServer in this.virtualServers)
                {
                    if (virtualServer.Key != "")
                    {
                        Console.WriteLine("index:" + k + "   Virtual Server: " + virtualServer.Key + " Server: " + virtualServer.Value);
                    }
                    k++;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Error while removing server: " + e.Message);
                return false;
            }
            return false;
        }
        internal String GetServerToHandleRequest(string requestId)
        {
            
            // print request id
            //Console.WriteLine("Request: " + requestId);
            uint hash = sdbmHash(requestId);
            
            int index = (int)(hash % ringLength);
            //Console.WriteLine("Hash index: " + index);
            var node = GetNodeAtOrAfterIndex(index, virtualServers);
            //print node
            //Console.WriteLine("Node: " + node.Value.Key);
            //Console.WriteLine("Node: " + node.Value.Value);
            // add or increment request count per server
            if (requestCountPerServer.ContainsKey(node.Value.Value))
            {
                requestCountPerServer[node.Value.Value] = requestCountPerServer[node.Value.Value] + 1;
            }
            else
            {
                requestCountPerServer.Add(node.Value.Value, 1);
            }

            return node.Value.Key;
        }

        private LinkedListNode<KeyValuePair<String, String>> GetNodeAtOrAfterIndex(int index, LinkedList<KeyValuePair<string, string>> virtualServers)
        {
            LinkedListNode<KeyValuePair<String, String>> node = GetNodeAtIndex(index, virtualServers);
            //print node
            //Console.WriteLine("Node: " + node.Value.Key);

            while (node != null)
            {
                if (node.Value.Key != "" && index <= virtualServers.Count)
                {
                    return node;
                }
                node = node.Next;
            }

            node = virtualServers.First;
            //print node
            //Console.WriteLine("Node: " + node.Value.Key);
            int currentIndex = 0;

            while (node != null && currentIndex <= index)
            {
                if (node.Value.Key != "")
                {
                    return node;
                }
                node = node.Next;
                currentIndex++;
            }

            return null;
        }

        internal LinkedListNode<KeyValuePair<String, String>> GetNodeAtIndex(int index, LinkedList<KeyValuePair<String, String>> virtualServers)
        {
            LinkedListNode<KeyValuePair<String,String>> node = virtualServers.First;
            for (int i = 0; i < index; i++)
            {
                node = node.Next;
            }
            return node;
        }

        private IEnumerable<uint> GenerateIndexesForVirtualServers(List<string> virtualServers, int ringLength)
        {
            HashSet<uint> indexes = new HashSet<uint>();

            foreach (string virtualServer in virtualServers)
            {
                uint hash = JenkinsOneAtATimeHash(virtualServer);
                //print hash
                Console.WriteLine("Hash: " + hash);

                if (!indexes.Add((uint)(hash % ringLength)))
                {
                    uint hash2 = sdbmHash(virtualServer);
                    //print hash2
                    Console.WriteLine("Hash2: " + hash2);
                    if (!indexes.Add((uint)(hash2 % ringLength)))
                    {
                        throw new Exception("Unable to generate unique hash for virtual server"+ virtualServer);
                    }
                }
            }

            //sort indexes inplace ascending order
            indexes = new HashSet<uint>(indexes.OrderBy(i => i));
            //printall indexes
            foreach (int index in indexes)
            {
                Console.WriteLine("Index: " + index);
            }

            return indexes;
        }

        uint JenkinsOneAtATimeHash(string input)
        {
            uint hash = 0;

            foreach (char ch in input)
            {
                hash += ch;
                hash += hash << 10;
                hash ^= hash >> 6;
            }

            hash += hash << 3;
            hash ^= hash >> 11;
            hash += hash << 15;

            return hash;
        }

        // SDBM Hash
        private uint sdbmHash(string str)
        {
            uint hash = 0;
            foreach (char ch in str)
            {
                hash = ch + hash * 65599;
            }
            return hash;
        }
    }
}
