```rust


Archiver -> Script

[DataIn (0u8)] [Url of input (terminated by newline)] [Length of input (u64, le)] [Input] // send the script a new response body to process
[Rpc (3u8)] [Length of input (u64, le)] [JSON-RPC 2.0 response] // sends the script an RPC response


Script -> Archiver

[Url (1u8)] [Url (terminated by newline)] // sends the archiver a new URL to archive
[FileOver (2u8)] // signals the archiver that we're done with this file
[Rpc (3u8)] [Length of input (u64, le)] [JSON-RPC 2.0 request] // sends an rpc call to the archiver
```