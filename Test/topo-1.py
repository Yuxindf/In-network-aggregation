# Test Complex situation: three clients
# (1) Target: packets of two go through proxy, one to server directly
# (2) Contrast: packets of three clients go through proxy 

from mininet.topo import Topo
from mininet.link import TCLink

class customTopo(Topo):
    def __init__(self):
        super(customTopo,self).__init__()

        # Add hosts
        server = self.addHost('server', ip='10.0.0.1',mac='00:00:00:00:00:01')        
        proxy = self.addHost('proxy', ip='10.0.0.2',mac='00:00:00:00:00:02')
        client1 = self.addHost('client1', ip='10.0.0.3',mac='00:00:00:00:00:03')
        client2 = self.addHost('client2', ip='10.0.0.4',mac='00:00:00:00:00:04')
        client3 = self.addHost('client3', ip='10.0.0.5',mac='00:00:00:00:00:05')
        
        # Add switches
        s1 = self.addSwitch('s1')
        
        # Add links
        servers1 = {'bw':1}
        self.addLink(s1,server,**servers1)
        client1s1 = {'bw':1,'loss':3}
        self.addLink(s1,client1,**client1s1)
        client2s1 = {'bw':1,'loss':3}
        self.addLink(s1,client2,**client2s1)
        client3s1 = {'bw':1,'loss':3}
        self.addLink(s1,client3,**client3s1)
        proxys1 = {'bw':1}
        self.addLink(s1,proxy, cls=TCLink , **proxys1)

        

topos = {'mytopo':(lambda:customTopo())}
