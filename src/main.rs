use std::{
    io::{Cursor, Read, Write},
    net::{TcpListener, TcpStream},
};

use tokio::io::AsyncReadExt;

#[derive(Debug)]
struct Message {
    request_api_key: u16,
    request_api_version: u16,
    correlation_id: u32,
}

impl Message {
    async fn read_message(stream: &mut TcpStream, len: u32) -> Self {
        // Read the message using the previous len
        let mut msg_buf = vec![0u8; len as usize];
        stream.read_exact(msg_buf.as_mut_slice()).unwrap();
        let mut rdr = Cursor::new(msg_buf);
        let request_api_key = rdr.read_u16().await.unwrap();
        let request_api_version = rdr.read_u16().await.unwrap();
        let correlation_id = rdr.read_u32().await.unwrap();

        Message {
            request_api_key,
            request_api_version,
            correlation_id,
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                // Read the message len from stream
                let mut buf = [0_u8; 4];
                _stream
                    .read_exact(buf.as_mut_slice())
                    .expect("Expected a 4 bytes for msg len");
                let mut rdr = Cursor::new(buf);
                let msg_len = rdr
                    .read_u32()
                    .await
                    .expect("Expected that bytearray converts to u32");

                let msg = Message::read_message(&mut _stream, msg_len).await;

                _stream.write_all([0, 0, 0, 4].as_slice()).unwrap();
                println!("MSG: {:?}", msg);
                _stream
                    .write_all(&msg.correlation_id.to_be_bytes())
                    .unwrap();
                if msg.request_api_version != 4 {
                    println!("Unsupported api version: {}", &msg.request_api_version);
                    _stream.write_all([0, 35].as_slice()).unwrap();
                } else {
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
    Ok(())
}
