from segment import Segment

"""                                                                                                   
The reliable data transfer (RDT) layer is used as a communication layer to resolve issues over an unreliable        
channel. Implements Go-Back-N protocol for server retransmission of packets.
"""


class RDTLayer(object):
    DATA_LENGTH = 4 # in characters                     # The length of the string data that will be sent per packet...
    FLOW_CONTROL_WIN_SIZE = 15 # in characters          # Receive window size for flow-control
    sendChannel = None
    receiveChannel = None
    dataToSend = ''
    currentIteration = 0                                # Use this for segment 'timeouts'

    # client state vars
    base = 0
    unackedSegments = 0
    nextSeqNum = 0
    countSegmentTimeouts = 0
    dupAck = 0

    # server state vars
    expectedSeqNum = 0
    receivedDataBuffer = None

    def __init__(self):
        self.sendChannel = None
        self.receiveChannel = None
        self.dataToSend = ''
        self.currentIteration = 0
        self.receivedDataBuffer = dict()
        self.sentPacketCheck = dict()

    def setSendChannel(self, channel):
        self.sendChannel = channel

    def setReceiveChannel(self, channel):
        self.receiveChannel = channel

    # application layer "sends" data down to socket (rdt layer)
    def setDataToSend(self,data):
        self.dataToSend = data

    # Called by main to get the currently received and buffered string data, in order
    def getDataReceived(self):
        data = ""
        for i in range(len(self.receivedDataBuffer)):
            try:
                data += self.receivedDataBuffer[i]
            except KeyError:
                break
        return data

    # rdt client/server makes move
    def processData(self):
        self.currentIteration += 1
        self.processReceiveAndSendRespond()
        self.processSend()

    # rdt layer "sends" data to network layer (create segment, send to sendChannel)
    def processSend(self):
            # GBN timeout retransmission
            for seg in self.sentPacketCheck.values():
                if isinstance(seg, Segment):
                    if (self.currentIteration - seg.getStartIteration()) > 2:
                        self.countSegmentTimeouts += 1
                        # resend all segments in window
                        self.nextSeqNum = self.base
                        self.unackedSegments = 0
                        break

            while (self.nextSeqNum < len(self.dataToSend)) and \
                    ((self.nextSeqNum - self.base + self.DATA_LENGTH) <= self.FLOW_CONTROL_WIN_SIZE):
                segmentSend = Segment()
                data = self.dataToSend[self.nextSeqNum : self.nextSeqNum + self.DATA_LENGTH]
                segmentSend.setData(self.nextSeqNum, data)
                print("Sending segment: ", segmentSend.to_string())

                # send segment to unreliable channel sendQueue
                self.sendChannel.send(segmentSend)
                segmentSend.setStartIteration(self.currentIteration)
                self.sentPacketCheck[segmentSend.seqnum] = segmentSend

                # move pointers to next segment
                self.nextSeqNum += self.DATA_LENGTH
                self.unackedSegments = (self.nextSeqNum - self.base) // self.DATA_LENGTH
                print(f"unacked segments in transit: {self.unackedSegments}")


    # manage segment receive tasks
    def processReceiveAndSendRespond(self):
        if len(self.receiveChannel.receiveQueue) == 0:
            return

        incomingSegments = self.receiveChannel.receive()
        needAck = False

        for segment in incomingSegments:
            # CLIENT: ack is response from server receiving segment
            if segment.acknum != -1:
                # fast retransmit
                if segment.acknum == self.base and self.nextSeqNum > self.base:
                    self.dupAck += 1
                    print(f"dup ACK received, count: {self.dupAck}")
                    if self.dupAck == 2:
                        print("Fast retransmit retriggered!")
                        self.nextSeqNum = self.base
                        self.dupAck = 0

                # cumulative ack received:
                elif segment.acknum > self.base:
                    for seq in range(self.base, segment.acknum, self.DATA_LENGTH):
                        self.sentPacketCheck.pop(seq, None)
                    self.base = segment.acknum
                    self.dupAck = 0
                    self.unackedSegments = (self.nextSeqNum - self.base) // self.DATA_LENGTH

            # SERVER: because ack is -1
            else:
                if segment.checkChecksum():
                    index = segment.seqnum // self.DATA_LENGTH
                    if segment.seqnum == self.expectedSeqNum:
                        self.receivedDataBuffer[index] = segment.payload

                    while (self.expectedSeqNum // self.DATA_LENGTH) in self.receivedDataBuffer:
                        self.expectedSeqNum += self.DATA_LENGTH

                    needAck = True

        # SERVER: reply with cumulative ACK acking the last continuous byte
        if needAck:
            ackSegment = Segment()
            ackSegment.setAck(self.expectedSeqNum)
            print("Sending cumulative ack: ", ackSegment.to_string())
            self.sendChannel.send(ackSegment)
