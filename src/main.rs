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
    error_code: u16,
}

impl Message {
    async fn read_message(stream: &mut TcpStream, len: u32) -> Self {
        /*Request Header v0 => request_api_key request_api_version correlation_id
        request_api_key => INT16
        request_api_version => INT16
        correlation_id => INT32 */

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
            error_code: 0,
        }
    }

    fn create_api_versions_response(
        correlation_id: u32,
        request_api_key: u16,
        request_api_version: u16,
    ) -> Self {
        let error_code = match request_api_version {
            4..=18 => 0,
            _ => 35,
        };
        Message {
            correlation_id,
            error_code,
            request_api_key,
            request_api_version,
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
                println!("MSG: {:?}", msg);

                let resp_msg = Message::create_api_versions_response(
                    msg.correlation_id,
                    msg.request_api_key,
                    msg.request_api_version,
                );
                println!("RESP: {:?}", resp_msg);

                let mut resp_len = msg.correlation_id.to_be_bytes().len() as u32;
                // error_code u16
                resp_len += msg.error_code.to_be_bytes().len() as u32;
                // Represents a sequence of objects of a given type T.
                // Type T can be either a primitive type (e.g. STRING) or a structure.
                // First, the length N is given as an INT32. Then N instances of type T follow.
                // A null array is represented with a length of -1. In protocol documentation an array of T instances is referred to as [T].

                // Length of [api_key]
                resp_len += 3_u32.to_be_bytes().len() as u32;
                // api_key - 18
                resp_len += 18_u16.to_be_bytes().len() as u32;
                // min_version 4+
                resp_len += 4_u16.to_be_bytes().len() as u32;
                resp_len += 4_u16.to_be_bytes().len() as u32;
                // throttle_time_ms => INT32
                resp_len += 0_u32.to_be_bytes().len() as u32;
                println!("Response with msg len: {resp_len}");

                /*
                ApiVersions Response (Version: 4) => error_code [api_keys] throttle_time_ms TAG_BUFFER
                error_code => INT16
                api_keys => api_key min_version max_version TAG_BUFFER
                  api_key => INT16
                  min_version => INT16
                  max_version => INT16
                throttle_time_ms => INT32
                */

                _stream.write_all(&resp_len.to_be_bytes()).unwrap();
                _stream
                    .write_all(&msg.correlation_id.to_be_bytes())
                    .unwrap();
                println!("Error code: {}", &msg.error_code);
                _stream
                    .write_all(&resp_msg.error_code.to_be_bytes())
                    .unwrap();
                //
                _stream.write_all(&3_u32.to_be_bytes()).unwrap();
                _stream.write_all(&18_u16.to_be_bytes()).unwrap();
                _stream.write_all(&4_u16.to_be_bytes()).unwrap();
                _stream.write_all(&4_u16.to_be_bytes()).unwrap();
                _stream.write_all(&0_u32.to_be_bytes()).unwrap();
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
    Ok(())
}
