from segment import Segment

"""                                                                                                   
The reliable data transfer (RDT) layer is used as a communication layer to resolve issues over an unreliable        
channel.
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
    sendPacketCheck = None
    countSegmentTimeouts = 0
    dupAck = 0

    # server state vars
    expectedSeqNum = 0
    outOfOrderPacket = 0
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
        # if there is stuff to send from the app layer:
        if len(self.dataToSend) != 0:

            # GBN timeout retransmission
            for seg in self.sentPacketCheck.values():
                if isinstance(seg, Segment):
                    if (self.currentIteration - seg.getStartIteration()) > 1:
                        self.countSegmentTimeouts += 1
                        # resend all segments in window
                        self.nextSeqNum = self.base
                        self.unackedSegments = 0
                        break

            while (self.nextSeqNum < len(self.dataToSend)) and \
            ((self.unackedSegments + 1) * self.DATA_LENGTH <= self.FLOW_CONTROL_WIN_SIZE):
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
        if len(self.receiveChannel.receiveQueue) != 0:
            incomingSegments = self.receiveChannel.receive()

            for segment in incomingSegments:
                # CLIENT: ack is response from server receiving segment
                if segment.acknum != -1:

                    # fast retransmit
                    if segment.acknum == self.base:
                        self.dupAck += 1
                        print(f"dup ACK received, count: {self.dupAck}")
                        if self.dupAck == 2:
                            print("Fast retransmit retriggered!")
                            # resend all packets in window
                            for seq in range(self.base, self.nextSeqNum, self.DATA_LENGTH):
                                packet = self.sentPacketCheck.get(seq)
                                if isinstance(packet, Segment):
                                    self.sendChannel.send(packet)
                                    print("Retransmission- Sending segment: ", packet.to_string())
                                    packet.setStartIteration(self.currentIteration)
                            self.dupAck = 0

                    # cumulative ack received:
                    elif segment.acknum > self.base:
                        for seq in range(self.base, segment.acknum, self.DATA_LENGTH):
                            if seq in self.sentPacketCheck:
                                self.sentPacketCheck[seq] = 0
                                print(f"Segment {seq} ACKed")

                        self.base = segment.acknum
                        self.unackedSegments = (self.nextSeqNum - self.base) // self.DATA_LENGTH
                        self.dupAck = 0

                # SERVER: because ack is -1
                else:
                    seqNum = segment.seqnum
                    if not segment.checkChecksum():
                        break

                    # only store the packet's data if in order
                    if seqNum == self.expectedSeqNum:
                        index = seqNum // self.DATA_LENGTH
                        self.receivedDataBuffer[index] = segment.payload
                        self.expectedSeqNum += self.DATA_LENGTH
                    else:
                        self.outOfOrderPacket += 1

            # SERVER: reply with cumulative ACK acking the last continuous byte
            if segment.acknum == -1:
                while self.outOfOrderPacket >= 0:
                    ackSegment = Segment()
                    ackSegment.setAck(self.expectedSeqNum)
                    print(f"Sending cumulative ack for packet {segment.seqnum}: ", ackSegment.to_string())
                    self.sendChannel.send(ackSegment)
                    self.outOfOrderPacket -= 1

