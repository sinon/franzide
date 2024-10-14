#![allow(unused_imports)]
use std::{io::Write, net::TcpListener};

fn main() -> Result<(), std::io::Error> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                println!("accepted new connection");
                let buf: [u8; 8] = [0, 0, 0, 0, 0, 0, 0, 7];
                let wr_resp = _stream.write(&buf)?;
                assert_eq!(wr_resp, 8);
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
    Ok(())
}
