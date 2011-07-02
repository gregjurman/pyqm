import qmclient

client = qmclient.QMClient()

print "Login test:"
print client.connect("TESTACC", "qm", "fossbox", host="192.168.1.104")
print

print "Arbitrary Execute:"
print client.execute('LIST TESTACC NAME ADDRESS')
print

print "Open TESTACC File"
print client.open("TESTACC")
print

#print "Read TEST record with no lock"
#print client.read("TESTACC", "TEST")
#print

#print "Read TEST record with shared lock"
#print client.read_shared("TESTACC", "TEST", True)
#print

#print "Read TEST record with exclusive lock"
#print client.read_excl("TESTACC", "TEST", True)
#print

#print "Lock TEST2 record"
#print client.record_lock("TESTACC", "TEST2")
#print

#print "Write TEST2 record"
#print client.write("TESTACC", "TEST2", "This is a test from Python")
#print

new_rec = qmclient.QMRecord()
new_rec.data = ["Greg Jurman", "123 Fake St"]

client.record_lock("TESTACC", "GREGJ")
client.write('TESTACC', 'GREGJ', new_rec)

print "Close the TESTACC File"
print client.close("TESTACC")
print

print "Disconnect from OpenQM"
print client.disconnect()
print

