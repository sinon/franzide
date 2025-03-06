use std::io::Cursor;

use anyhow::Error;
use bytes::BufMut;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

#[derive(Debug)]
struct MessageRequestHeader {
    /// Request Header v0 => request_api_key request_api_version correlation_id
    /// request_api_key => INT16
    /// request_api_version => INT16
    /// correlation_id => INT32
    request_api_key: u16,
    request_api_version: u16,
    correlation_id: u32,
}

#[derive(Debug)]
struct MessageResponseHeader {
    /// Response Header v1 => correlation_id TAG_BUFFER
    ///  correlation_id => INT32
    correlation_id: u32,
}

#[derive(Debug)]
struct ApiKey {
    key: u16,
    min_version: u16,
    max_version: u16,
}

#[derive(Debug)]
struct ApiVersionsResponse {
    correlation_id: u32,
    error_code: u16,
    api_keys: Vec<ApiKey>,
    throttle_time_ms: i32,
}

#[derive(Debug)]
struct TopicPartition {
    error_code: u16,
    partition_index: u32,
    leader_id: u32,
    leader_epoch: u32,
    replica_nodes: u32,
    isr_nodes: u32,
    eligible_leader_replicas: u32,
    last_known_elr: u32,
}

#[derive(Debug)]
struct Topic {
    error_code: u16,
    name: String,
    // topic_id: uuid,
    is_internal: bool,
    partitions: Vec<TopicPartition>,
    topic_authorized_operations: u32,
}

#[derive(Debug)]
struct DescribeTopicPartitionsResponse {
    correlation_id: u32,
    topics: Vec<Topic>,
    throttle_time_ms: i32,
    // next_cursor: u32,
}

struct MessageType {
    api_key: u16,
    min_version: u16,
    max_version: u16,
}

mod supported_messages {
    use super::MessageType;
    pub const API_VERSIONS: MessageType = MessageType {
        api_key: 18,
        min_version: 0,
        max_version: 4,
    };
    pub const DESCRIBE_TOPIC_PARTITIONS: MessageType = MessageType {
        api_key: 75,
        min_version: 0,
        max_version: 0,
    };
}

struct KafkaCursor {}

struct DescribeTopicPartitionsRequestBody {
    topics: Vec<Topic>,
    response_partition_limit: u32,
    cursor: Vec<KafkaCursor>,
}

impl MessageRequestHeader {
    async fn read_message_header(stream: &mut TcpStream, len: u32) -> Result<Self, Error> {
        /*Request Header v0 => request_api_key request_api_version correlation_id
        request_api_key => INT16
        request_api_version => INT16
        correlation_id => INT32 */

        // Read the message using the previous len
        let mut msg_buf = vec![0u8; len as usize];
        stream.read_exact(msg_buf.as_mut_slice()).await?;
        let mut rdr = Cursor::new(msg_buf);
        let request_api_key = rdr.read_u16().await?;
        let request_api_version = rdr.read_u16().await?;
        let correlation_id = rdr.read_u32().await?;

        Ok(Self {
            request_api_key,
            request_api_version,
            correlation_id,
        })
    }
}

impl ApiKey {
    fn serialize(&self, buf: &mut Vec<u8>) {
        buf.put_i16(self.key as i16);
        buf.put_i16(self.min_version as i16);
        buf.put_i16(self.max_version as i16);
        buf.put_i8(0); // TAG_BUFFER
    }
}

impl ApiVersionsResponse {
    fn new(correlation_id: u32, error_code: u16) -> Self {
        let api_keys = vec![
            ApiKey {
                key: supported_messages::API_VERSIONS.api_key,
                min_version: supported_messages::API_VERSIONS.min_version,
                max_version: supported_messages::API_VERSIONS.max_version,
            },
            ApiKey {
                key: supported_messages::DESCRIBE_TOPIC_PARTITIONS.api_key,
                min_version: supported_messages::DESCRIBE_TOPIC_PARTITIONS.min_version,
                max_version: supported_messages::DESCRIBE_TOPIC_PARTITIONS.max_version,
            },
        ];

        Self {
            correlation_id,
            error_code,
            api_keys,
            throttle_time_ms: 200,
        }
    }

    fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        buf.put_u32(self.correlation_id);
        buf.put_u16(self.error_code);
        buf.put_i8(self.api_keys.len() as i8);

        for api_key in &self.api_keys {
            api_key.serialize(&mut buf);
        }

        buf.put_i32(self.throttle_time_ms);
        buf.put_i8(0); // TAG_BUFFER
        buf
    }
}

impl DescribeTopicPartitionsResponse {
    fn new(correlation_id: u32) -> Self {
        Self {
            correlation_id,
            throttle_time_ms: 200,
            topics: vec![],
        }
    }

    fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        buf.put_u32(self.correlation_id);
        buf.put_i8(self.topics.len() as i8);

        buf.put_i32(self.throttle_time_ms);
        buf.put_i8(0); // TAG_BUFFER

        buf
    }
}

async fn parse_request(stream: &mut TcpStream) -> Result<MessageRequestHeader, Error> {
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

    let msg = MessageRequestHeader::read_message_header(stream, msg_len).await?;
    println!("MSG: {msg:?}");
    Ok(msg)
}

fn create_response(msg: &MessageRequestHeader) -> Vec<u8> {
    let error_code = match msg.request_api_version {
        4..=18 => 0,
        _ => 35,
    };

    let resp_data = if msg.request_api_key == supported_messages::API_VERSIONS.api_key {
        let api_versions_response = ApiVersionsResponse::new(msg.correlation_id, error_code);
        println!("Request for ApiVersions: {:?}", api_versions_response);
        api_versions_response.serialize()
    } else if msg.request_api_key == supported_messages::DESCRIBE_TOPIC_PARTITIONS.api_key {
        let describe_topic_partitions_response =
            DescribeTopicPartitionsResponse::new(msg.correlation_id);
        println!(
            "Request for DescribeTopicPartitions: {:?}",
            describe_topic_partitions_response
        );
        describe_topic_partitions_response.serialize()
    } else {
        println!("Request for unknown API key");
        Vec::new()
    };

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
