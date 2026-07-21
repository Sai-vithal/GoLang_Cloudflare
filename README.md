## `log_consumer`

### Problem definition
You are provided with a stream of request logs. 

The goal of the project is for you to write a program which:
1. Continuously reads from the stream of request logs and store the most recent 1000 for each zone ID. (Zone IDs are randomly chosen between 7,500-12,500.)
2. Exposes an HTTP interface which supports: a) getting a list of zone IDs; and b) getting all stored logs for a given zone ID.

### During the interview
1. Please share your entire screen and work on your local system in your own environment.
2. Run `go run main.go` from the `log_consumer` folder to compile and run the application. It is expected that the code given induces an "imported and not used" compile error: this means your environment is working.
3. You are free to use any resource (Google, StackOverflow, Go docs). You can also use any standard Go libraries. Furthermore, you can ask the interviewer(s) for questions about Go language/libs as long as you aren't asking for help in solving a problem (allowed: "Does Go have a set data type?"; not allowed: "How do you implement a set in Go?").
