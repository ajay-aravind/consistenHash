using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

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


        public Ring()
        {
            this.AddNdummyServers(ringLength);
        }

        public Ring(int ringLength)
        {
            this.ringLength = ringLength;
            this.AddNdummyServers(ringLength);
        }

        // todo
        // Handler cases where there is a server already at generated index
        internal Boolean AddServer(string serverId)
        {
            try
            {
                //print server id
                Console.WriteLine("Server: " + serverId);
                Server server = new Server(serverId, VirtualServerPerServer);
                servers.Add(server);
                server.GenerateVirtualServers();
                this.PhysicalServerToVirtualServersMap.Add(server.Id, server.PhysicalServerToVirtualServersMap);
                this.AddAllVirtualServersOfServerToRing(server);
            }
            catch (Exception e)
            {
                Console.WriteLine("Error while adding server: " + e.Message);
                return false;
            }

            return true;
        }

        internal Boolean RemoveServer(string serverId)
        {
            try
            {
                List<String> virtualServers = PhysicalServerToVirtualServersMap[serverId];
                foreach (string virtualServer in virtualServers)
                {
                    this.removeVirtualServer(virtualServer);
                }

                PhysicalServerToVirtualServersMap.Remove(serverId);
                this.servers.RemoveAll(server => server.Id == serverId);

                this.PrintPhysicalServerToVirtualServersMap();
                this.PrintNonEmptyVirtualServers();
            }
            catch (Exception e)
            {
                Console.WriteLine("Error while removing server: " + e.Message);
                return false;
            }
            return false;
        }

        private void removeVirtualServer(string virtualServer)
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

        private void PrintPhysicalServerToVirtualServersMap()
        {
            // print physical server to virtual servers map
            foreach (KeyValuePair<String, List<String>> entry in this.PhysicalServerToVirtualServersMap)
            {
                Console.WriteLine("Server: " + entry.Key);
                foreach (string virtualServer in entry.Value)
                {
                    Console.WriteLine("Virtual Server: " + virtualServer);
                }
            }
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
            this.UpdateRequestCountPerServer(node.Value.Value);
            return node.Value.Key;
        }

        private void AddVirtualServerToRingAtIndex(int index, Server server, String virtualServerId)
        {
            LinkedListNode<KeyValuePair<String, String>> linkedListNode = GetNodeAtIndex(index, virtualServers);
            //print linkedListNode
            Console.WriteLine("LinkedListNode: " + linkedListNode.Value);
            var newServerNode = KeyValuePair.Create(virtualServerId, server.Id);
            virtualServers.AddBefore(linkedListNode, newServerNode);
            virtualServers.Remove(linkedListNode);
        }

        private void PrintNonEmptyVirtualServers()
        {
            int k = 0;
            // print all virtual servers if key is not empty
            foreach (KeyValuePair<String, String> virtualServer in this.virtualServers)
            {
                if (virtualServer.Key != "")
                {
                    Console.WriteLine("index:" + k + "   Virtual Server: " + virtualServer.Key + " Server: " + virtualServer.Value);
                }
                k++;
            }
        }

        private void AddAllVirtualServersOfServerToRing(Server server)
        {
            foreach (string vServer in server.PhysicalServerToVirtualServersMap)
            {
                uint hash = sdbmHash(vServer);
                //print hash
                Console.WriteLine("Hash: " + hash);
                int index = (int)(hash % ringLength);
                Console.WriteLine("Hash index: " + index);

                if (index < this.virtualServers.Count)
                {
                    this.AddVirtualServerToRingAtIndex(index, server, vServer);
                }
                else
                {
                    throw new Exception("Index out of bounds");
                }
                this.PrintNonEmptyVirtualServers();
            }
        }

        private void UpdateRequestCountPerServer(String serverId)
        {
            if (requestCountPerServer.ContainsKey(serverId))
            {
                requestCountPerServer[serverId] = requestCountPerServer[serverId] + 1;
            }
            else
            {
                requestCountPerServer.Add(serverId, 1);
            }
        }

        private LinkedListNode<KeyValuePair<String, String>> GetNodeAtOrAfterIndex(int index, LinkedList<KeyValuePair<string, string>> virtualServers)
        {
            LinkedListNode<KeyValuePair<String, String>> node = GetNodeAtIndex(index, virtualServers);
            //print node
            //Console.WriteLine("Node: " + node.Value.Key);

             node = GetNextValidNode(node, index, virtualServers);
            if(node != null)
            {
                return node;
            }
            else
            {
                node = this.GetValidNodeFromStartToIndex(index, virtualServers);
                if (node != null)
                {
                    return node;
                }
            }

            return null;
        }

        private LinkedListNode<KeyValuePair<String, String>>? GetValidNodeFromStartToIndex(int index, LinkedList<KeyValuePair<String, String>> virtualServers)
        {
            var node = virtualServers.First;
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

        private LinkedListNode<KeyValuePair<String, String>>? GetNextValidNode(LinkedListNode<KeyValuePair<String, String>> node, int index, LinkedList<KeyValuePair<String, String>> virtualServers)
        {
            while (node != null)
            {
                if (node.Value.Key != "" && index <= virtualServers.Count)
                {
                    return node;
                }
                node = node.Next;
            }

            return null;
        }

        private LinkedListNode<KeyValuePair<String, String>> GetNodeAtIndex(int index, LinkedList<KeyValuePair<String, String>> virtualServers)
        { 
            if(virtualServers.Count == 0)
            {
                throw new Exception("Virtual servers list is empty");
            }

            LinkedListNode<KeyValuePair<String,String>> node = virtualServers.First;
            for (int i = 0; i < index; i++)
            {
                node = node.Next;
            }
            return node;
        }

        private void AddNdummyServers(int ringLength)
        {
            // Add dummy 1000 virtual servers
            for (int j = 0; j < ringLength; j++)
            {
                this.virtualServers.AddLast(KeyValuePair.Create("", ""));
            }
        }

        /*
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
        */

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
