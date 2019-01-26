
f=open('log','r')
iids = []
for r in f.readlines():
	zone, iid = r.rstrip('\r\n').split(":")
	iid.rstrip('\n')
	if int(zone) == :
		iids.append(iid)
print iids