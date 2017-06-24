/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	//int id = *(int*)(&memberNode->addr.addr);
	//int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {

	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        //char *message = "JOINREQ";
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);
        log->LOG(&memberNode->addr, "JOINREQ msg sent.");

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
    memberNode->inited = false;
	memberNode->inGroup = false;
    return 0;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
	/*
	 * Your code goes here
	 */
    MessageHdr *msg = (MessageHdr *)data;
    Address *sender = (Address *)(data + sizeof(MessageHdr));
    long *htbt = (long *)(data + sizeof(MessageHdr) + sizeof(Address) + 1);
    
    // JOINREQ message received at introducer
    if (memberNode->addr.getAddress() == "1:0" && msg->msgType == JOINREQ) {
        MemberListEntry *le = new MemberListEntry(sender->addr[0], sender->addr[4], *htbt, par->getcurrtime());
        memberNode->memberList.push_back(*le);
        log->logNodeAdd(&memberNode->addr, sender);
        sendJoinRep(memberNode, sender);
    }

    // JOINREP message received at node
    if (msg->msgType == JOINREP && memberNode->addr.getAddress() != "1:0") {
        if (!memberNode->inGroup) {
            memberNode->inGroup = true;
        }

        vector<MemberListEntry> *ml = (vector<MemberListEntry> *)(data + sizeof(MessageHdr) + sizeof(Address) + 1 + sizeof(long) + sizeof(int));
        //int *mleSize = (int *)(data + sizeof(MessageHdr) + sizeof(Address) + 1 + sizeof(long));
        // cout << endl << "JOINREP - At " + memberNode->addr.getAddress() + ", member list received from " + sender->getAddress() + " has " << *mleSize << " members:" << endl;
        // printMemberList(ml);
        updateMemberListTable(ml);
    }

    // GOSSIP message received at node
    if (msg->msgType == GOSSIP) {
        if (memberNode->inGroup) {
            vector<MemberListEntry> *ml = (vector<MemberListEntry> *)(data + sizeof(MessageHdr) + sizeof(Address) + 1 + sizeof(long) + sizeof(int));
            //int *mleSize = (int *)(data + sizeof(MessageHdr) + sizeof(Address) + 1 + sizeof(long));
            // cout << endl << "GOSSIP - At " + memberNode->addr.getAddress() + ", member list received from " + sender->getAddress() + " has " << *mleSize << " members:" << endl;
            // printMemberList(ml);
            updateMemberListTable(ml);
        }
    }

    return true;
}

/**
 * FUNCTION NAME: sendJoinRep
 *
 * DESCRIPTION: Used by node 1 to send JOIN responses
 */
void MP1Node::sendJoinRep(Member *introducer, Address *joiner) {
    // Construct message holder
    size_t msgsize = sizeof(MessageHdr) + sizeof(joiner->addr) + sizeof(long) + 1 + sizeof(int) + (memberNode->memberList.size() * sizeof(MemberListEntry));
    MessageHdr *msg = (MessageHdr *)malloc(msgsize * sizeof(MessageHdr));

    // create JOINREP message
    msg->msgType = JOINREP;
    memcpy((char *)(msg+1), &introducer->addr.addr, sizeof(introducer->addr.addr));
    memcpy((char *)(msg+1) + 1 + sizeof(introducer->addr.addr), &introducer->heartbeat, sizeof(long));
    
    // Add member list
    int mlSize = memberNode->memberList.size();
    memcpy((char *)(msg+1) + 1 + sizeof(introducer->addr.addr) + sizeof(long), &mlSize, sizeof(int));
    memcpy((char *)(msg+1) + 1 + sizeof(introducer->addr.addr) + sizeof(long) + sizeof(int), &memberNode->memberList, memberNode->memberList.size() * sizeof(MemberListEntry));

    // cout << endl << "At " + memberNode->addr.getAddress() + ", member list being sent to " + joiner->getAddress() + " in JOINREP has " << memberNode->memberList.size() << " members:" << endl;
    // printMemberList(&memberNode->memberList);

    emulNet->ENsend(&memberNode->addr, joiner, (char *)msg, msgsize);
    log->LOG(&memberNode->addr, "JOINREP msg sent.");

    free(msg);
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

	/*
	 * Your code goes here
	 */
    
    // If not failed, send member list to other nodes
    if (!memberNode->bFailed) {
        // Update own heartbeat and timestamp
        // Check for failed nodes
        memberNode->heartbeat++;
        vector<string> removeIds;
        for (MemberListEntry &le : memberNode->memberList) {
            string leAddr = to_string(le.id) + ":" + to_string(le.port);
            if (leAddr == memberNode->addr.getAddress()) {
                le.heartbeat++;
                le.timestamp = par->getcurrtime();
            }
            else {
                // If current time is TREMOVE greater than timestamp, remove
                if (par->getcurrtime() - le.timestamp >= TREMOVE) {
                    cout << "[" << par->getcurrtime() << "]" << " H: " << le.heartbeat << " T: " << le.timestamp << " . Removing " << to_string(le.id) + ":" + to_string(le.port) << " from member list at " << memberNode->addr.getAddress() << endl;
                    removeIds.push_back(to_string(le.id) + ":" + to_string(le.port));
                }
            }
        }

        for (string id : removeIds) {
            removeMemberListEntry(id);
        }

        // Send updated member list table to others
        // Construct message holder
        size_t msgsize = sizeof(MessageHdr) + sizeof(memberNode->addr) + sizeof(long) + 1 + sizeof(int) + (memberNode->memberList.size() * sizeof(MemberListEntry));
        MessageHdr *msg = (MessageHdr *)malloc(msgsize * sizeof(MessageHdr));

        // create JOINREP message
        msg->msgType = GOSSIP;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));
        
        // Add member list
        int mlSize = memberNode->memberList.size();
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr) + sizeof(long), &mlSize, sizeof(int));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr) + sizeof(long) + sizeof(int), &memberNode->memberList, memberNode->memberList.size() * sizeof(MemberListEntry));

        // Send to each node in current member list
        for (MemberListEntry le : memberNode->memberList) {
            string leAddr = to_string(le.id) + ":" + to_string(le.port);
            // Don't send to oneself
            if (leAddr != memberNode->addr.getAddress()) {
                emulNet->ENsend(&memberNode->addr, new Address(leAddr), (char *)msg, msgsize);
            }
        }

        free(msg);

        // cout << endl << "Node loop: [" + to_string(par->getcurrtime()) + "] " + memberNode->addr.getAddress() + ": " << memberNode->memberList.size() << " members:" << endl;
        // printMemberList(&memberNode->memberList);
    }
    return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
    // Add the node to its own member list
    MemberListEntry *nle = new MemberListEntry(memberNode->addr.addr[0], memberNode->addr.addr[4], memberNode->heartbeat, par->getcurrtime());
    memberNode->memberList.push_back(*nle);
    log->logNodeAdd(&memberNode->addr, &memberNode->addr);
}

/**
 * FUNCTION NAME: updateMemberListTable
 *
 * DESCRIPTION: Updates the current node's member list table based on incoming list from another node
 */
void MP1Node::updateMemberListTable(vector<MemberListEntry> *ml) {
    for(MemberListEntry mle : *ml) {
        if (mle.heartbeat > 10000 || mle.timestamp > 10000 || mle.id > par->EN_GPSZ) return;
        bool matchFound = false;
        for(MemberListEntry &le : memberNode->memberList) {
            if (mle.id == le.id) {
                // Entry is in the list - update it
                matchFound = true;
                if (mle.timestamp >= le.timestamp) 
                {
                    // If incoming timestamp is later, update heartbeat
                    // But only if local token is not bigger than incoming
                    if (mle.heartbeat > le.heartbeat) {
                        le.heartbeat = mle.heartbeat;
                        le.timestamp = par->getcurrtime();
                    }
                }
                break;
            }
        }
        if (!matchFound) {
            // Entry doesn't exist - add
            if (par->getcurrtime() - mle.timestamp <= TFAIL) {
                if (mle.id > 0 && mle.id <= par->EN_GPSZ) {
                    MemberListEntry *nle = new MemberListEntry(mle.id, mle.port, mle.heartbeat, par->getcurrtime());
                    memberNode->memberList.push_back(*nle);
                    log->logNodeAdd(&memberNode->addr, new Address(to_string(mle.id) + ":" + to_string(mle.port)));
                }
            }
        }
    }
}

/**
 * FUNCTION NAME: removeMemberListEntry
 *
 * DESCRIPTION: Removes entry with given ID from current node's member list
 */
void MP1Node::removeMemberListEntry(string id) {
    Address *nodeAddr = new Address(id);
    memberNode->myPos = find_if(begin(memberNode->memberList), end(memberNode->memberList), [id](MemberListEntry mle) { return id == (to_string(mle.id) + ":" + to_string(mle.port)); });
    memberNode->memberList.erase(memberNode->myPos);
    log->logNodeRemove(&memberNode->addr, nodeAddr);
}

/**
 * FUNCTION NAME: printMemberList
 *
 * DESCRIPTION: Print the member list at the node
 */
void MP1Node::printMemberList(vector<MemberListEntry> *ml) {
    cout << endl;
    for (MemberListEntry le : *ml) {
        cout << le.id << ":" << le.port << "," << le.heartbeat << "," << le.timestamp << endl;
    }
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}