Steps to run the membership service -

1) Copy the boltdb-0.0.1-SNAPSHOT.jar, gson-1.7.1.jar, log4j-1.2.17.jar, dgroupmembership and boltdb.prop
   files into a folder.
2) cd into the folder to which you copied the above files.
3) Open the boltdb.prop file in a text editor and set the properties
	*	"groupmembership.contact" - This is the hostname/ipaddress of the contact machine.
	*	"groupmembership.tfail" - The time out for marking a process as failed
	*	"groupmembership.heartbeat.freq" - The frequency at which the local heartbeat should be
		updated.
	*	"groupmembership.refreshMembershipList.freq" - The frequency at which the local
		membership list should be refreshed (mark failures and remove marked entries).
	*	"groupmembership.gossip.freq" - The gossip frequency.
	*	"groupmembership.lossrate" - The message loss rate to be simulated.

4) Run the command - "./dgroupmembership -contact true -id machine.1
	*	"-contact" option is for specifying whether or not this daemon is the contact machine 
		daemon.(In this case, it is "true". For the other daemons, specify "-contact" as "false")
	*	"-id" is a unique id for the daemon and this unique id will be used to name the log file 
		on this machine.
5) While running the service, if you wish to voluntarily leave the system, type "leave" and hit enter. If you
   want to crash the process, press Ctrl+C.

Steps to run the Log Querier service -

Start the LogQuerier server -
1) Copy the boltdb-0.0.1-SNAPSHOT.jar, dgrep and boltdb.prop files into a folder. (If you have already 
   copied these files in the previous step - while running the GroupMembership daemon, you should be fine)
2) cd into the folder to which you copied the above files.
3) Run the command - "./dgrep -startServer 1 6789"
	*	Here, 1 is the serverId and 6789 is the port number. Make sure you specify the same serverId
		that you used while starting the GroupMembership service. Eg.- If you have specified "-id" as
		machine.1 while starting the GroupMembership daemon, use the serverId as 1. 


Query logs using the LogQuerier client -
1) Copy the boltdb-0.0.1-SNAPSHOT.jar, dgrep and boltdb.prop files into a folder (If you have already 
   copied these files in the previous step - while running the GroupMembership daemon, you should be fine)
2) cd into the folder to which you copied the above files.
5) Edit the boltdb.prop file to add the ipaddress and port details of the servers in the system.
3) Run the command - "./dgrep -key <keyRegExp> -value <valRegExp>
	*	Here <keyRegExp> and <valRegExp> are the key and value regular expressions that you wish
		to search.
	*	To look for joins, type the command "./dgrep -key JOINED"
	*	To look for crashes, type the command "./dgrep -key CRASHED"
	*	To look for leavs, type the command "./dgrep -key LEFT"
