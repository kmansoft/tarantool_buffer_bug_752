The test case for regression in 1.6.8-752-g8fc147c:

Tarantool is unstable under load.

https://groups.google.com/forum/#!topic/tarantool/j7f3l7xPqvA

Repro instructions:

- Test app

	A simple database with some simple scripts.
	
	A Go test app making calls to those scripts from 20 "threads".
	
	A performance / stability test, essentially.

- In one terminal window, run

	./push_db_server.lua

- In another, run

	/run_test_db_server.sh subs ping change

- Expected output

	The three tests (subs, ping, change) are executed completely, sequentially, each
	reporting progress and performance
	
- Output with 1.6.8-752-g8fc147c

	2016/07/29 12:21:24 Subs test, c = 20, n = 100000
	2016/07/29 12:21:24 Key length: 40
	2016/07/29 12:21:25 Completed  10000 requests,  38147.77 rps
	2016/07/29 12:21:25 Completed  20000 requests,  39481.04 rps
	2016/07/29 12:21:30 Error calling function: Error calling push_CreateDev: client timeout for request 57582
	exit status 1
	
- Downgrading to 1.6.8.746-1.fc23 makes the issue go away

- Setting box.readahead = 323200 makes the issue go away in 1.6.8-752-g8fc147c
