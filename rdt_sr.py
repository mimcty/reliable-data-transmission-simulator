from segment import Segment

"""                                                                                                   
The reliable data transfer (RDT) layer is used as a communication layer to resolve issues over an unreliable        
channel. Implements Selective Repeat protocol for server retransmission of packets.
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
            # SR timeout retransmission
            print(f"base: {self.base}, nextSeqNum: {self.nextSeqNum}, unackedSegments: {self.unackedSegments}")
            for seg in self.sentPacketCheck.values():
                if isinstance(seg, Segment):
                    print(f"seg {seg.seqnum}: age = {self.currentIteration - seg.getStartIteration()}")
                    if (self.currentIteration - seg.getStartIteration()) > 5:
                        self.countSegmentTimeouts += 1
                        # resend that segment
                        self.sendChannel.send(seg)
                        seg.setStartIteration(self.currentIteration)

            while (self.nextSeqNum < len(self.dataToSend)) and \
                    (self.nextSeqNum < self.base + self.FLOW_CONTROL_WIN_SIZE) and \
                    (self.unackedSegments * self.DATA_LENGTH < self.FLOW_CONTROL_WIN_SIZE):
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
                self.unackedSegments += 1
                print(f"unacked segments in transit: {self.unackedSegments}")


    # manage segment receive tasks
    def processReceiveAndSendRespond(self):
        if len(self.receiveChannel.receiveQueue) != 0:
            incomingSegments = self.receiveChannel.receive()

            for segment in incomingSegments:
                # CLIENT: ack is response from server receiving segment
                if segment.acknum != -1:
                    packetSeqNum = segment.acknum - self.DATA_LENGTH
                    if packetSeqNum in self.sentPacketCheck and isinstance(self.sentPacketCheck[packetSeqNum], Segment):
                        self.sentPacketCheck[packetSeqNum] = 0
                        self.unackedSegments -= 1
                        while self.base in self.sentPacketCheck and self.sentPacketCheck[self.base] == 0:
                            del self.sentPacketCheck[self.base]
                            self.base += self.DATA_LENGTH

                # SERVER: because ack is -1
                else:
                    if not segment.checkChecksum():
                        continue

                    index = segment.seqnum // self.DATA_LENGTH

                    if index not in self.receivedDataBuffer:
                        self.receivedDataBuffer[index] = segment.payload

                    ackSegment = Segment()
                    ackSegment.setAck(segment.seqnum + self.DATA_LENGTH)
                    print("Sending ack: ", ackSegment.to_string())
                    self.sendChannel.send(ackSegment)

