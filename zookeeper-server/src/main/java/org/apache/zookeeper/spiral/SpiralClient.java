package org.apache.zookeeper.spiral;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import proto.com.linkedin.spiral.GetRequestOrBuilder;
import proto.com.linkedin.spiral.Put;
import proto.com.linkedin.spiral.PutRequest;
import proto.com.linkedin.spiral.PutResponse;
import proto.com.linkedin.spiral.SpiralApiGrpc;
import proto.com.linkedin.spiral.SpiralContext;
import proto.com.linkedin.spiral.Key;
import proto.com.linkedin.spiral.GetRequest;
import proto.com.linkedin.spiral.GetResponse;
import proto.com.linkedin.spiral.Value;

public class SpiralClient {
  private static final Logger logger = LoggerFactory.getLogger(SpiralClient.class);

  private final SpiralApiGrpc.SpiralApiBlockingStub _blockingStub;


  public SpiralClient(String spiralEndpoint) {
    try {
      ManagedChannel channel = ManagedChannelBuilder.forTarget(spiralEndpoint).usePlaintext().build();
      _blockingStub = SpiralApiGrpc.newBlockingStub(channel);
      logger.info("Connected to spiral-service : {}", spiralEndpoint);
    } catch (Exception e) {
      logger.error("Failed to connect to spiral service at endpoint : {} {}", spiralEndpoint, e.getMessage());
      throw e;
    }
  }

  public byte[] get(String key) {
    SpiralContext cs = SpiralContext.newBuilder()
                          .setNamespace("test")
                          .setBucket("zk")
                          .build();

    ByteString keyBytes = ByteString.copyFromUtf8(key);
    Key apiKey = Key.newBuilder().setMessage(keyBytes).build();
    GetRequest request = GetRequest.newBuilder().setSpiralContext(cs).setKey(apiKey).build();
    try {
      GetResponse response = _blockingStub.get(request);
      return response.getValue().getMessage().toByteArray();
    } catch (Exception e) {
      logger.error("Get: RPC failed: {}", e.getMessage());
      throw e;
    }
  }

  public void put(String key, byte[] value) {
    SpiralContext cs = SpiralContext.newBuilder()
        .setNamespace("test")
        .setBucket("zk")
        .build();

    ByteString keyBytes = ByteString.copyFromUtf8(key);
    Key apiKey = Key.newBuilder().setMessage(keyBytes).build();
    Value apiValue = Value.newBuilder().setMessage(ByteString.copyFrom(value)).build();
    Put putValue = Put.newBuilder().setKey(apiKey).setValue(apiValue).build();
    PutRequest request = PutRequest.newBuilder()
        .setSpiralContext(cs)
        .setPut(putValue)
        .build();
    try {
      // TODO - convert response to ZK response.
      PutResponse response = _blockingStub.put(request);
      get(key);
    } catch (Exception e) {
      logger.error("put: RPC failed: {}, {}", e.getMessage(), e);
      throw e;
    }
  }
}
