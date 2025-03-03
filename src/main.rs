use std::io::Cursor;

use anyhow::Error;
use bytes::BufMut;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

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
        stream.read_exact(msg_buf.as_mut_slice()).await.unwrap();
        let mut rdr = Cursor::new(msg_buf);
        let request_api_key = rdr.read_u16().await.unwrap();
        let request_api_version = rdr.read_u16().await.unwrap();
        let correlation_id = rdr.read_u32().await.unwrap();

        Self {
            request_api_key,
            request_api_version,
            correlation_id,
            error_code: 0,
        }
    }

    const fn create_api_versions_response(
        correlation_id: u32,
        request_api_key: u16,
        request_api_version: u16,
    ) -> Self {
        let error_code = match request_api_version {
            4..=18 => 0,
            _ => 35,
        };
        Self {
            request_api_key,
            request_api_version,
            correlation_id,
            error_code,
        }
    }
}

async fn parse_request(stream: &mut TcpStream) -> Result<Message, Error> {
    // Read the message len from stream
    let mut buf = [0_u8; 4];
    stream
        .read_exact(buf.as_mut_slice())
        .await
        .expect("Expected a 4 bytes for msg len");
    let mut reader = Cursor::new(buf);
    let msg_len = reader
        .read_u32()
        .await
        .expect("Expected that bytearray converts to u32");

    let msg = Message::read_message(stream, msg_len).await;
    println!("MSG: {msg:?}");
    Ok(msg)
}

fn create_response(msg: &Message) -> Vec<u8> {
    let resp_msg = Message::create_api_versions_response(
        msg.correlation_id,
        msg.request_api_key,
        msg.request_api_version,
    );
    println!("RESP: {resp_msg:?}");

    let mut resp_data: Vec<u8> = Vec::new();
    // Represents a sequence of objects of a given type T.
    // Type T can be either a primitive type (e.g. STRING) or a structure.
    // First, the length N is given as an INT32. Then N instances of type T follow.
    // A null array is represented with a length of -1. In protocol documentation an array of T instances is referred to as [T].
    //
    /*
    ApiVersions Response (Version: 4) => error_code [api_keys] throttle_time_ms TAG_BUFFER
    error_code => INT16
    api_keys => api_key min_version max_version TAG_BUFFER
      api_key => INT16
      min_version => INT16
      max_version => INT16
    throttle_time_ms => INT32
    */
    if msg.request_api_key == 18 {
        resp_data.put_u32(resp_msg.correlation_id);
        resp_data.put_u16(resp_msg.error_code);
        resp_data.put_i8(2);
        resp_data.put_i16(18); // api key
        resp_data.put_i16(0); // min version: 0
        resp_data.put_i16(4); // max version: 4
        resp_data.put_i8(0); // size of tag buffer
        resp_data.put_i32(200); // throttle time ms
        resp_data.put_i8(0);
        println!("Request for ApiVersions");
    } else {
        println!("Request for unknown API key");
    }

    let mut response = Vec::new();
    response.put_i32(
        resp_data
            .len()
            .try_into()
            .expect("len should convert to i32"),
    );
    response.put(&resp_data[..]);
    response
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let listener = TcpListener::bind("127.0.0.1:9092")
        .await
        .expect("Cannot connect to local port 9092");

    loop {
        let (mut socket, _) = listener.accept().await?;

        tokio::spawn(async move {
            while let Ok(req_msg) = parse_request(&mut socket).await {
                let resp = create_response(&req_msg);
                match socket.write_all(&resp).await {
                    Ok(()) => println!("Successfully wrote response"),
                    Err(e) => eprintln!("error: {e}"),
                }
            }
        });
    }
}
