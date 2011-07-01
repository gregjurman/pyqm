import qmclient

client = qmclient.QMClient()

print "Login test:"
print client.connect("TESTACC", "qm", "fossbox", host="192.168.1.104")
print

print "Arbitrary Execute:"
print client.execute('LIST TESTACC TITLE')
print

print "Open TESTACC File"
print client.open("TESTACC")
print

print "Read TEST record with no locks"
print client.read("TESTACC", "TEST")
print

print "Read TEST record with shared lock"
print client.read_shared("TESTACC", "TEST", True)
print

print "Read TEST record with exclusive lock"
print client.read_excl("TESTACC", "TEST", True)
print

print "Close the TESTACC File"
print client.close("TESTACC")
print

print "Disconnect from OpenQM"
print client.disconnect()
print

