# NATS Interactions

- To interact successfully with NATS:
  * Use the official NATS Go client library
  * Establish a connection with `nats.Connect()` 
  * Use `nc.Publish()` for sending messages
  * Use `nc.Subscribe()` for receiving messages
  * Always close the connection with `nc.Close()` when done
  * Handle connection and message errors explicitly
  * Consider using NATS JetStream for more reliable messaging

# Coding approach

We are still alpha. DONT CARE about backward-compatibility.
