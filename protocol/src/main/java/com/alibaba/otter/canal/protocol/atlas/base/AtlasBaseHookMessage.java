package com.alibaba.otter.canal.protocol.atlas.base;

public class AtlasBaseHookMessage {
  private AtlasBaseMessage message;
  private String msgCompressionKind;
  private String msgCreatedBy;
  private long msgCreationTime;
  private String msgSourceIP;
  private int msgSplitCount;
  private int msgSplitIdx;
  private Version version;

  public AtlasBaseMessage getMessage() {
    return message;
  }

  public void setMessage(AtlasBaseMessage message) {
    this.message = message;
  }

  public String getMsgCompressionKind() {
    return msgCompressionKind;
  }

  public void setMsgCompressionKind(String msgCompressionKind) {
    this.msgCompressionKind = msgCompressionKind;
  }

  public String getMsgCreatedBy() {
    return msgCreatedBy;
  }

  public void setMsgCreatedBy(String msgCreatedBy) {
    this.msgCreatedBy = msgCreatedBy;
  }

  public long getMsgCreationTime() {
    return msgCreationTime;
  }

  public void setMsgCreationTime(long msgCreationTime) {
    this.msgCreationTime = msgCreationTime;
  }

  public String getMsgSourceIP() {
    return msgSourceIP;
  }

  public void setMsgSourceIP(String msgSourceIP) {
    this.msgSourceIP = msgSourceIP;
  }

  public int getMsgSplitCount() {
    return msgSplitCount;
  }

  public void setMsgSplitCount(int msgSplitCount) {
    this.msgSplitCount = msgSplitCount;
  }

  public int getMsgSplitIdx() {
    return msgSplitIdx;
  }

  public void setMsgSplitIdx(int msgSplitIdx) {
    this.msgSplitIdx = msgSplitIdx;
  }

  public Version getVersion() {
    return version;
  }

  public void setVersion(Version version) {
    this.version = version;
  }
}
